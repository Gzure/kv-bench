import tempfile
import unittest
from pathlib import Path

from manager import BenchItem, DeploymentManager, Node, NodeStore, TaskSpec


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

    def test_version_mismatch_compiles_only_mismatching_node(self):
        executor = FakeExecutor({"a": "liburma-1.0", "b": "liburma-2.0"})
        manager = DeploymentManager(executor)
        nodes = [Node("a", "10.0.0.1"), Node("b", "10.0.0.2")]
        result = manager.ensure_urma_consistency(nodes, "/opt/kv-bench", "/opt/umdk")
        self.assertFalse(result.consistent)
        compile_calls = [call for call in executor.calls if "cmake" in call[-1]]
        self.assertEqual([call[0] for call in compile_calls], ["b", "b"])

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


if __name__ == "__main__":
    unittest.main()
