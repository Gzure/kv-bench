"""Standalone manager for deploying and scheduling kv-bench.

Workers are data-plane processes only.  All manager-side operations use SSH/SCP;
there is deliberately no manager client or control socket in kv-bench.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Protocol

try:
    import httpx
except ImportError:  # domain layer stays usable without third-party deps
    httpx = None

#: worker 端 nohup 日志路径（deploy 时写入，日志拉取从这里 tail）
WORKER_LOG_PATH = "/var/log/kv-bench-worker.log"


def atomic_write_json(path: str | os.PathLike[str], data: Any) -> None:
    """原子写 JSON：临时文件 + os.replace，避免写一半损坏。"""
    path = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(path)) or "."
    temporary = os.path.join(directory, f".{os.path.basename(path)}.tmp")
    with open(temporary, "w", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, ensure_ascii=False)
        stream.write("\n")
    os.replace(temporary, path)


@dataclass(frozen=True)
class Node:
    name: str
    ip: str
    user: str = "root"
    ssh_port: int = 22
    workdir: str = "/opt/kv-bench"
    binary: str = "/opt/kv-bench/build/kv-bench"
    api_port: int = 18082
    password: str = field(default="", repr=False)
    tags: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # 标签归一化：去空白、去重、保持顺序；兼容旧 nodes.json（无 tags 字段）。
        seen: list[str] = []
        for tag in self.tags:
            tag = tag.strip()
            if tag and tag not in seen:
                seen.append(tag)
        object.__setattr__(self, "tags", tuple(seen))


class NodeStore:
    def __init__(self, path: str | os.PathLike[str] = "nodes.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._nodes: dict[str, Node] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as stream:
                self._nodes = {entry["name"]: Node(**entry) for entry in json.load(stream)}

    def _save(self) -> None:
        atomic_write_json(self.path, [asdict(node) for node in self.list()])

    def upsert(self, node: Node) -> None:
        with self._lock:
            self._nodes[node.name] = node
            self._save()

    def remove(self, name: str) -> None:
        with self._lock:
            if self._nodes.pop(name, None) is None:
                raise KeyError(name)
            self._save()

    def get(self, name: str) -> Node:
        with self._lock:
            return self._nodes[name]

    def list(self) -> list[Node]:
        return sorted(self._nodes.values(), key=lambda node: node.name)


class TaskStore:
    """任务状态持久化（tasks.json）：manager 重启后任务与状态不丢。"""

    def __init__(self, path: str | os.PathLike[str] = "tasks.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._tasks: dict[str, TaskSpec] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(self.path):
            return
        with open(self.path, encoding="utf-8") as stream:
            for entry in json.load(stream):
                task = TaskSpec(
                    task_id=entry["task_id"],
                    workers={name: Node(**worker) for name, worker in entry["workers"].items()},
                    bench_items=[BenchItem(**item) for item in entry["bench_items"]],
                    options=entry.get("options", {}),
                    state=entry.get("state", "queued"),
                    result=entry.get("result", {}),
                )
                self._tasks[task.task_id] = task

    def _save(self) -> None:
        # 调用方已持有 self._lock（upsert 内），直接遍历，避免重入死锁
        atomic_write_json(self.path, [asdict(task) for task in self._tasks.values()])

    def upsert(self, task: TaskSpec) -> None:
        with self._lock:
            self._tasks[task.task_id] = task
            self._save()

    def values(self) -> list[TaskSpec]:
        with self._lock:
            return list(self._tasks.values())


class DeployStatusStore:
    """节点部署状态持久化（deploy_status.json）。"""

    def __init__(self, path: str | os.PathLike[str] = "deploy_status.json"):
        self.path = os.fspath(path)
        self._lock = threading.Lock()
        self._status: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if os.path.exists(self.path):
            with open(self.path, encoding="utf-8") as stream:
                self._status = json.load(stream)

    def _save(self) -> None:
        atomic_write_json(self.path, self._status)

    def update(self, name: str, **fields: Any) -> None:
        with self._lock:
            self._status[name] = {**self._status.get(name, {}), **fields}
            self._save()

    def get(self, name: str) -> dict[str, Any] | None:
        with self._lock:
            return self._status.get(name)

    def all(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return dict(self._status)


class RunArtifacts:
    """任务运行产物目录 runs/{task_id}/：结果、任务快照、worker 日志、事件日志。"""

    def __init__(self, base_dir: str | os.PathLike[str] = "runs"):
        self.base = os.fspath(base_dir)

    def directory(self, task_id: str) -> str:
        return os.path.join(self.base, task_id)

    def _ensure(self, task_id: str) -> str:
        directory = self.directory(task_id)
        os.makedirs(directory, exist_ok=True)
        return directory

    def save_task(self, task: TaskSpec) -> None:
        directory = self._ensure(task.task_id)
        atomic_write_json(os.path.join(directory, "task.json"), asdict(task))

    def save_result(self, task: TaskSpec, result: dict[str, Any]) -> None:
        directory = self._ensure(task.task_id)
        atomic_write_json(os.path.join(directory, "result.json"), result)
        atomic_write_json(os.path.join(directory, "task.json"), asdict(task))

    def append_event(self, task_id: str, event: str) -> None:
        directory = self._ensure(task_id)
        with open(os.path.join(directory, "manager.log"), "a", encoding="utf-8") as stream:
            stream.write(f"{datetime.now().isoformat(timespec='seconds')}  {event}\n")

    def fetch_logs(self, executor: Executor, task: TaskSpec) -> dict[str, str]:
        """SSH tail 各 worker 的 kv-bench 日志到 logs/{worker}.log（失败写 .error）。"""
        directory = self._ensure(task.task_id)
        logs_dir = os.path.join(directory, "logs")
        os.makedirs(logs_dir, exist_ok=True)
        collected: dict[str, str] = {}
        for name, node in task.workers.items():
            try:
                content = executor.run(node, ["tail", "-n", "500", WORKER_LOG_PATH])
                suffix = "log"
            except Exception as error:
                content = f"[log fetch failed: {error}]"
                suffix = "error"
            path = os.path.join(logs_dir, f"{name}.{suffix}")
            with open(path, "w", encoding="utf-8", errors="replace") as stream:
                stream.write(content)
            collected[name] = content
        return collected

    def read_logs(self, task_id: str) -> dict[str, Any]:
        """读取已保存的日志：{workers: {name: {content, error}}, manager_log}。"""
        directory = self.directory(task_id)
        workers: dict[str, Any] = {}
        logs_dir = os.path.join(directory, "logs")
        if os.path.isdir(logs_dir):
            for entry in sorted(os.listdir(logs_dir)):
                name, _, suffix = entry.rpartition(".")
                if suffix not in {"log", "error"}:
                    continue
                with open(os.path.join(logs_dir, entry), encoding="utf-8", errors="replace") as stream:
                    workers[name] = {"content": stream.read(), "error": suffix == "error"}
        manager_log = ""
        log_path = os.path.join(directory, "manager.log")
        if os.path.isfile(log_path):
            with open(log_path, encoding="utf-8", errors="replace") as stream:
                manager_log = stream.read()
        return {"workers": workers, "manager_log": manager_log}


@dataclass(frozen=True)
class BenchItem:
    src: str
    dst: str
    type: str = "forward"

    def __post_init__(self) -> None:
        if self.type not in {"forward", "reverse", "bidirectional"}:
            raise ValueError("type must be forward, reverse, or bidirectional")
        if self.src == self.dst:
            raise ValueError("src and dst must be different workers")


@dataclass
class TaskSpec:
    task_id: str
    workers: dict[str, Node]
    bench_items: list[BenchItem]
    options: dict[str, Any] = field(default_factory=dict)
    state: str = "queued"
    result: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class VersionCheck:
    versions: dict[str, tuple[str, ...]]
    consistent: bool
    reference: tuple[str, ...]


class Executor(Protocol):
    def run(self, node: Node, command: list[str]) -> str: ...

    def copy(self, node: Node, source: str, destination: str) -> None: ...


class WorkerClient(Protocol):
    def start(self, node: Node, task_id: str, command: list[str]) -> None: ...

    def stop(self, node: Node, task_id: str) -> None: ...

    def result(self, node: Node, task_id: str) -> dict[str, Any]: ...


class SshExecutor:
    """Remote executor using the system ssh/scp clients."""

    def _target(self, node: Node) -> str:
        return f"{node.user}@{node.ip}"

    def run(self, node: Node, command: list[str]) -> str:
        prefix = ["sshpass", "-e"] if node.password else []
        result = subprocess.run(
            prefix + ["ssh", "-p", str(node.ssh_port), self._target(node), "--", *command],
            check=True,
            text=True,
            capture_output=True,
            env={**__import__("os").environ, "SSHPASS": node.password},
        )
        return result.stdout

    def copy(self, node: Node, source: str, destination: str) -> None:
        prefix = ["sshpass", "-e"] if node.password else []
        subprocess.run(
            prefix + ["scp", "-P", str(node.ssh_port), source, f"{self._target(node)}:{destination}"],
            check=True,
            env={**__import__("os").environ, "SSHPASS": node.password},
        )


class HttpWorkerClient:
    """Worker API client over httpx (mature HTTP stack, explicit timeouts)."""

    def _require_httpx(self) -> None:
        if httpx is None:
            raise RuntimeError(
                "httpx is required: pip install -r manager/requirements.txt")

    def _post(self, node: Node, path: str, payload: dict[str, Any]) -> None:
        self._require_httpx()
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(
                    f"http://{node.ip}:{node.api_port}{path}", json=payload)
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error
        if response.status_code >= 300:
            raise RuntimeError(f"worker {node.name} returned HTTP {response.status_code}")

    def start(self, node: Node, task_id: str, command: list[str]) -> None:
        self._post(node, "/v1/tasks/start", {"task_id": task_id, "command": command})

    def stop(self, node: Node, task_id: str) -> None:
        self._post(node, f"/v1/tasks/{task_id}/stop", {})

    def result(self, node: Node, task_id: str) -> dict[str, Any]:
        self._require_httpx()
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(
                    f"http://{node.ip}:{node.api_port}/v1/tasks/{task_id}/result")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as error:
            raise RuntimeError(f"worker {node.name} unreachable: {error}") from error


class DeploymentManager:
    def __init__(
        self,
        executor: Executor,
        worker_client: WorkerClient | None = None,
        node_store: NodeStore | None = None,
        task_store: TaskStore | None = None,
        deploy_status: DeployStatusStore | None = None,
        artifacts: RunArtifacts | None = None,
    ):
        self.executor = executor
        self.worker_client = worker_client
        self.node_store = node_store
        self.task_store = task_store
        self.deploy_status = deploy_status
        self.artifacts = artifacts
        self.tasks: dict[str, TaskSpec] = {}
        self._lock = threading.Lock()
        if task_store is not None:  # 恢复已持久化的任务
            for task in task_store.values():
                self.tasks[task.task_id] = task

    def _event(self, task_id: str, message: str) -> None:
        if self.artifacts is not None:
            self.artifacts.append_event(task_id, message)

    def urma_packages(self, node: Node) -> tuple[str, ...]:
        # Equivalent to: rpm -qa | grep urma. Filtering locally avoids shell
        # interpolation of node-controlled values.
        output = self.executor.run(node, ["rpm", "-qa"])
        return tuple(sorted(line.strip() for line in output.splitlines() if "urma" in line.lower()))

    def ensure_urma_consistency(
        self, nodes: list[Node], source_dir: str, umdk_root: str
    ) -> VersionCheck:
        versions = {node.name: self.urma_packages(node) for node in nodes}
        reference = next(iter(versions.values()), ())
        result = VersionCheck(versions, all(value == reference for value in versions.values()), reference)
        if not result.consistent:
            for node in nodes:
                if versions[node.name] != reference:
                    self.compile_node(node, source_dir, umdk_root)
        return result

    def compile_node(self, node: Node, source_dir: str, umdk_root: str) -> None:
        self.executor.run(node, ["cmake", "-S", source_dir, "-B", f"{node.workdir}/build", f"-DUMDK_ROOT={umdk_root}"])
        self.executor.run(node, ["cmake", "--build", f"{node.workdir}/build", "-j"])

    def deploy(self, nodes: list[Node], artifact: str, destination: str, source_dir: str, umdk_root: str) -> VersionCheck:
        if self.node_store is not None:
            for node in nodes:
                self.node_store.upsert(node)
        check = self.ensure_urma_consistency(nodes, source_dir, umdk_root)
        for node in nodes:
            self.executor.copy(node, artifact, destination)
            self.executor.run(node, ["sh", "-lc", f"nohup {shlex.quote(destination)} --worker --worker-port={node.api_port} >{WORKER_LOG_PATH} 2>&1 &"])
            if self.deploy_status is not None:
                self.deploy_status.update(
                    node.name,
                    deployed_at=datetime.now().isoformat(timespec="seconds"),
                    artifact=artifact,
                    destination=destination,
                    versions=list(check.versions.get(node.name, ())),
                    consistent=check.consistent or check.versions.get(node.name) == check.reference,
                    worker_state="started",
                )
        return check

    def build_commands(self, task: TaskSpec) -> list[str]:
        return [command for commands in self.build_assignments(task).values() for command in commands]

    def build_assignments(self, task: TaskSpec) -> dict[str, list[str]]:
        assignments: dict[str, list[str]] = {name: [] for name in task.workers}
        common = {"op": "write", "threads": 1, "duration": 10, **task.options}
        for item in task.bench_items:
            source = self._resolve_worker(task, item.src)
            destination = self._resolve_worker(task, item.dst)
            if source is None or destination is None:
                raise ValueError(f"unknown topology endpoint: {item.src}->{item.dst}")
            for index, (node, peer) in enumerate(((source, destination), (destination, source))):
                active = item.type == "bidirectional" or (item.type == "forward" and index == 0) or (item.type == "reverse" and index == 1)
                arguments = [node.binary]
                if active:
                    arguments += [f"--peer-ip={peer.ip}", f"--direction={item.type}"]
                else:
                    arguments += [f"--direction={item.type}", "--no-interactive"]
                arguments += [f"--{key.replace('_', '-')}={value}" for key, value in common.items()]
                assignments[node.name].append(" ".join(shlex.quote(str(value)) for value in arguments))
        return assignments

    @staticmethod
    def _resolve_worker(task: TaskSpec, endpoint: str) -> Node | None:
        if endpoint in task.workers:
            return task.workers[endpoint]
        return next((node for node in task.workers.values() if node.ip == endpoint), None)

    def create_task(self, payload: dict[str, Any]) -> TaskSpec:
        workers = {entry["name"]: Node(**entry) for entry in payload["workers"]}
        items = [BenchItem(**entry) for entry in payload["bench_items"]]
        task = TaskSpec(payload.get("task_id", f"task-{uuid.uuid4().hex[:8]}"), workers, items, payload.get("options", {}))
        with self._lock:
            if task.task_id in self.tasks:
                raise ValueError("task already exists")
            self.tasks[task.task_id] = task
        self._persist_task(task, "created")
        return task

    def start_task(self, task_id: str) -> list[str]:
        with self._lock:
            task = self.tasks[task_id]
            if task.state != "queued":
                raise ValueError("task is not queued")
            assignments = self.build_assignments(task)
            if self.worker_client is not None:
                for node_name, commands in assignments.items():
                    for command in commands:
                        self.worker_client.start(task.workers[node_name], task.task_id, shlex.split(command))
            commands = [command for values in assignments.values() for command in values]
            task.state = "running"
        self._persist_task(task, "started")
        return commands

    def stop_task(self, task_id: str) -> None:
        with self._lock:
            task = self.tasks[task_id]
            if self.worker_client is not None:
                for node in task.workers.values():
                    self.worker_client.stop(node, task_id)
            task.state = "stopped"
        self._persist_task(task, "stopped")

    def collect_result(self, task_id: str) -> dict[str, Any]:
        with self._lock:
            task = self.tasks[task_id]
            workers: dict[str, Any] = {}
            for name, node in task.workers.items():
                if self.worker_client is not None:
                    try:
                        workers[name] = self.worker_client.result(node, task_id)
                    except Exception as error:  # worker may still be starting
                        workers[name] = {"state": "unreachable", "error": str(error)}
            total = {"ops": 0, "bytes": 0, "errors": 0}
            for result in workers.values():
                for key in total:
                    total[key] += int(result.get(key, 0))
            duration = float(task.options.get("duration", 10) or 10)
            total["iops"] = total["ops"] / duration
            total["bandwidth_mb_s"] = total["bytes"] / duration / 1_000_000
            task.result = {"workers": workers, "aggregate": total}
        self._persist_task(task, "result collected")
        if self.artifacts is not None:
            self.artifacts.save_result(task, task.result)
        return task.result

    def collect_logs(self, task_id: str) -> dict[str, Any]:
        """SSH 拉取各 worker 日志并落盘，返回已保存的日志内容。"""
        with self._lock:
            task = self.tasks[task_id]
        if self.artifacts is None:
            return {"task_id": task_id, "directory": "", "workers": {}, "manager_log": ""}
        collected = self.artifacts.fetch_logs(self.executor, task)
        self._event(task_id, f"logs collected: {', '.join(collected)}")
        data = self.artifacts.read_logs(task_id)
        data.update(task_id=task_id, directory=self.artifacts.directory(task_id))
        return data

    def _persist_task(self, task: TaskSpec, event: str) -> None:
        if self.task_store is not None:
            self.task_store.upsert(task)
        if self.artifacts is not None:
            self._event(task.task_id, event)
            self.artifacts.save_task(task)


def serve(host: str, port: int) -> None:
    """Start the FastAPI manager (domain wiring + uvicorn)."""
    from .api import create_app
    import uvicorn

    manager = DeploymentManager(
        SshExecutor(), HttpWorkerClient(), NodeStore(),
        TaskStore(), DeployStatusStore(), RunArtifacts(),
    )
    uvicorn.run(create_app(manager), host=host, port=port)


def main() -> None:
    parser = argparse.ArgumentParser(description="kv-bench deployment and topology manager")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
