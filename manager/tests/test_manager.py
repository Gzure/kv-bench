import json
import tempfile
import unittest
from pathlib import Path

from manager import BenchItem, DeploymentManager, Node, NodeStore, TaskSpec
from manager.app import DeployStatusStore, RunArtifacts, TaskStore


class FakeExecutor:
    def __init__(self, versions):
        self.versions = versions
        self.calls = []

    def run(self, node, command):
        self.calls.append((node.name, tuple(command)))
        if command[:3] == ["rpm", "-qa", "|"]:
            return ""
        if command[:2] == ["rpm", "-qa"]:
            return self.versions[node.name]
        return "ok"

    def copy(self, node, source, destination):
        self.calls.append((node.name, "copy", source, destination))


class FakeWorkerClient:
    def __init__(self):
        self.started = []
        self.stopped = []

    def start(self, node, task_id, command):
        self.started.append((node.name, task_id, command))

    def stop(self, node, task_id):
        self.stopped.append((node.name, task_id))

    def result(self, node, task_id):
        return {"state": "ready", "ops": 10 if node.name == "a" else 20, "bytes": 100, "errors": 1}


class ManagerTests(unittest.TestCase):
    def test_topology_generates_forward_and_reverse_commands(self):
        manager = DeploymentManager(FakeExecutor({"a": "liburma-1.0", "b": "liburma-1.0"}))
        task = TaskSpec(
            task_id="t1",
            workers={"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
            bench_items=[BenchItem("10.0.0.1", "10.0.0.2", "bidirectional")],
        )
        commands = manager.build_commands(task)
        self.assertEqual(len(commands), 2)
        self.assertTrue(all("--direction=bidirectional" in c for c in commands))
        self.assertIn("--peer-ip=10.0.0.2", commands[0])
        self.assertIn("--peer-ip=10.0.0.1", commands[1])

    def test_build_assignments_handles_flag_options(self):
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}))
        task = TaskSpec(
            task_id="t2",
            workers={"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
            bench_items=[BenchItem("a", "b", "forward")],
            options={
                "event_mode": True,   # 开关 True -> --event-mode
                "cacheable": False,   # 默认关开关 False -> 不传
                "mbind": False,       # 默认开开关 False -> --no-mbind
                "drv_ext": True,      # 默认开开关 True -> --drv-ext
                "seed": 42,           # 标量 -> --seed=42
            },
        )
        commands = manager.build_commands(task)
        first = commands[0]
        self.assertIn("--event-mode", first)
        self.assertNotIn("--cacheable", first)
        self.assertIn("--no-mbind", first)
        self.assertIn("--drv-ext", first)
        self.assertIn("--seed=42", first)
        self.assertIn("--op=write", first)
        self.assertIn("--threads=1", first)

    def test_version_mismatch_compiles_only_mismatching_node(self):
        executor = FakeExecutor({"a": "liburma-1.0", "b": "liburma-2.0"})
        manager = DeploymentManager(executor)
        nodes = [Node("a", "10.0.0.1"), Node("b", "10.0.0.2")]
        result = manager.ensure_urma_consistency(nodes, "/opt/kv-bench", "/opt/umdk")
        self.assertFalse(result.consistent)
        compile_calls = [call for call in executor.calls if "cmake" in call[-1]]
        self.assertEqual([call[0] for call in compile_calls], ["b", "b"])

    def test_compile_node_without_umdk_root_omits_flag(self):
        executor = FakeExecutor({"a": "u"})
        manager = DeploymentManager(executor)
        manager.compile_node(Node("a", "10.0.0.1"), "/opt/kv-bench", "")
        configure = executor.calls[0][1]
        self.assertEqual(configure[0], "cmake")
        self.assertNotIn("-DUMDK_ROOT", configure)
        manager.compile_node(Node("a", "10.0.0.1"), "/opt/kv-bench", "/opt/umdk")
        configure_with = executor.calls[2][1]
        self.assertIn("-DUMDK_ROOT=/opt/umdk", configure_with)

    def test_manager_controls_workers_through_worker_api(self):
        client = FakeWorkerClient()
        manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), client)
        task = manager.create_task({
            "task_id": "t2",
            "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
            "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
        })
        manager.start_task(task.task_id)
        self.assertEqual([call[0] for call in client.started], ["a", "b"])
        self.assertIn("--peer-ip=10.0.0.2", client.started[0][2])
        self.assertNotIn("--peer-ip=10.0.0.1", client.started[1][2])
        manager.stop_task(task.task_id)
        self.assertEqual([call[0] for call in client.stopped], ["a", "b"])
        result = manager.collect_result(task.task_id)
        self.assertEqual(result["aggregate"]["ops"], 30)
        self.assertEqual(result["aggregate"]["errors"], 2)

    def test_node_store_persists_registered_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nodes.json"
            store = NodeStore(path)
            node = Node("a", "10.0.0.1", password="secret")
            store.upsert(node)
            self.assertEqual(store.get("a"), node)
            restored = NodeStore(path)
            self.assertEqual(restored.list(), [node])
            restored.remove("a")
            self.assertEqual(restored.list(), [])

    def test_node_tags_persist_and_normalize(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "nodes.json"
            store = NodeStore(path)
            node = Node("a", "10.0.0.1", tags=["gpu", "gpu", "  fast ", "fast"])
            store.upsert(node)
            # 去空白、去重、保序，元组归一化
            self.assertEqual(store.get("a").tags, ("gpu", "fast"))
            restored = NodeStore(path)
            self.assertEqual(restored.list()[0].tags, ("gpu", "fast"))
            # 无 tags 字段的旧文件兼容
            old = Node("b", "10.0.0.2")
            self.assertEqual(old.tags, ())

    def test_task_store_persists_state_across_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tasks.json"
            manager = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), task_store=TaskStore(path))
            manager.create_task({
                "task_id": "t1",
                "workers": [{"name": "a", "ip": "10.0.0.1"}, {"name": "b", "ip": "10.0.0.2"}],
                "bench_items": [{"src": "a", "dst": "b", "type": "forward"}],
            })
            manager.start_task("t1")
            manager.stop_task("t1")
            # 用持久化的 store 重建 manager -> 状态不丢
            manager2 = DeploymentManager(FakeExecutor({"a": "u", "b": "u"}), task_store=TaskStore(path))
            task = manager2.tasks["t1"]
            self.assertEqual(task.state, "stopped")
            self.assertEqual(task.workers["a"].ip, "10.0.0.1")
            self.assertEqual(task.bench_items[0].type, "forward")

    def test_deploy_status_store_persists(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "deploy_status.json"
            store = DeployStatusStore(path)
            store.update("a", deployed_at="2026-01-01T00:00:00", consistent=True, versions=["liburma-1.0"])
            restored = DeployStatusStore(path)
            self.assertEqual(restored.get("a")["consistent"], True)
            self.assertEqual(restored.get("a")["versions"], ["liburma-1.0"])
            self.assertIsNone(restored.get("nope"))

    def test_run_artifacts_save_result_and_fetch_logs(self):
        with tempfile.TemporaryDirectory() as directory:
            artifacts = RunArtifacts(directory)
            executor = FakeExecutor({"a": "u", "b": "u"})
            task = TaskSpec(
                "t1",
                {"a": Node("a", "10.0.0.1"), "b": Node("b", "10.0.0.2")},
                [BenchItem("a", "b", "forward")],
            )
            artifacts.save_result(task, {"aggregate": {"ops": 42}})
            artifacts.append_event("t1", "created")
            logs = artifacts.fetch_logs(executor, task)
            self.assertEqual(set(logs), {"a", "b"})
            base = Path(directory) / "t1"
            self.assertEqual(json.loads((base / "result.json").read_text())["aggregate"]["ops"], 42)
            for name in ("a", "b"):
                self.assertEqual((base / "logs" / f"{name}.log").read_text(), "ok")
            data = artifacts.read_logs("t1")
            self.assertEqual(data["workers"]["a"]["content"], "ok")
            self.assertIn("created", data["manager_log"])


if __name__ == "__main__":
    unittest.main()
