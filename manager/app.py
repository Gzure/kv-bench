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
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Protocol

from .console import CONSOLE_HTML


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
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        temporary = os.path.join(directory, f".{os.path.basename(self.path)}.tmp")
        with open(temporary, "w", encoding="utf-8") as stream:
            json.dump([asdict(node) for node in self.list()], stream, indent=2)
            stream.write("\n")
        os.replace(temporary, self.path)

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
    def _post(self, node: Node, path: str, payload: dict[str, Any]) -> None:
        request = urllib.request.Request(
            f"http://{node.ip}:{node.api_port}{path}",
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status >= 300:
                raise RuntimeError(f"worker {node.name} returned HTTP {response.status}")

    def start(self, node: Node, task_id: str, command: list[str]) -> None:
        self._post(node, "/v1/tasks/start", {"task_id": task_id, "command": command})

    def stop(self, node: Node, task_id: str) -> None:
        self._post(node, f"/v1/tasks/{task_id}/stop", {})

    def result(self, node: Node, task_id: str) -> dict[str, Any]:
        with urllib.request.urlopen(f"http://{node.ip}:{node.api_port}/v1/tasks/{task_id}/result", timeout=10) as response:
            return json.loads(response.read())


class DeploymentManager:
    def __init__(self, executor: Executor, worker_client: WorkerClient | None = None, node_store: NodeStore | None = None):
        self.executor = executor
        self.worker_client = worker_client
        self.node_store = node_store
        self.tasks: dict[str, TaskSpec] = {}
        self._lock = threading.Lock()

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
            self.executor.run(node, ["sh", "-lc", f"nohup {shlex.quote(destination)} --worker --worker-port={node.api_port} >/var/log/kv-bench-worker.log 2>&1 &"])
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
            return commands

    def stop_task(self, task_id: str) -> None:
        with self._lock:
            task = self.tasks[task_id]
            if self.worker_client is not None:
                for node in task.workers.values():
                    self.worker_client.stop(node, task_id)
            task.state = "stopped"

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
            return task.result


class ApiHandler(BaseHTTPRequestHandler):
    manager: DeploymentManager

    def _json(self, status: int, payload: Any) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        return json.loads(self.rfile.read(length) or b"{}")

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(CONSOLE_HTML.encode())))
            self.end_headers()
            self.wfile.write(CONSOLE_HTML.encode())
        elif self.path in {"/v1/nodes", "/v1/workers"}:
            nodes = self.manager.node_store.list() if self.manager.node_store else []
            self._json(200, [{key: value for key, value in asdict(node).items() if key != "password"} for node in nodes])
        elif self.path == "/v1/tasks":
            self._json(200, [asdict(task) for task in self.manager.tasks.values()])
        elif self.path.startswith("/v1/tasks/") and self.path.endswith("/result"):
            task_id = self.path[len("/v1/tasks/") : -len("/result")].rstrip("/")
            self._json(200, self.manager.collect_result(task_id))
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            if self.path == "/v1/nodes":
                node = Node(**self._body())
                if self.manager.node_store is None:
                    raise ValueError("node store is disabled")
                self.manager.node_store.upsert(node)
                self._json(201, {"name": node.name, "state": "saved"})
            elif self.path == "/v1/deploy":
                payload = self._body()
                nodes = [Node(**entry) for entry in payload["nodes"]]
                result = self.manager.deploy(nodes, payload["artifact"], payload["destination"], payload["source_dir"], payload["umdk_root"])
                self._json(200, asdict(result))
            elif self.path == "/v1/tasks":
                task = self.manager.create_task(self._body())
                self._json(201, {"task_id": task.task_id, "state": task.state})
            elif self.path.startswith("/v1/tasks/") and self.path.endswith("/start"):
                task_id = self.path[len("/v1/tasks/") : -len("/start")].rstrip("/")
                commands = self.manager.start_task(task_id)
                self._json(200, {"task_id": task_id, "state": "running", "commands": commands})
            elif self.path.startswith("/v1/tasks/") and self.path.endswith("/stop"):
                task_id = self.path[len("/v1/tasks/") : -len("/stop")].rstrip("/")
                self.manager.stop_task(task_id)
                self._json(200, {"task_id": task_id, "state": "stopped"})
            else:
                self._json(404, {"error": "not found"})
        except KeyError as error:
            self._json(404, {"error": str(error)})
        except (KeyError, ValueError, json.JSONDecodeError) as error:
            self._json(400, {"error": str(error)})

    def do_DELETE(self) -> None:  # noqa: N802
        if self.path.startswith("/v1/nodes/") and self.manager.node_store is not None:
            try:
                self.manager.node_store.remove(self.path[len("/v1/nodes/") :])
                self._json(200, {"state": "deleted"})
            except KeyError as error:
                self._json(404, {"error": str(error)})
            return
        self._json(404, {"error": "not found"})

    def log_message(self, *_args: Any) -> None:
        return


def serve(host: str, port: int) -> None:
    manager = DeploymentManager(SshExecutor(), HttpWorkerClient(), NodeStore())
    handler = type("ManagerApiHandler", (ApiHandler,), {"manager": manager})
    ThreadingHTTPServer((host, port), handler).serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="kv-bench deployment and topology manager")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18080)
    args = parser.parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
