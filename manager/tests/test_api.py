"""FastAPI route tests for the manager.

Skipped automatically when fastapi/httpx are not installed (e.g. offline
environments that still run the domain-level test_manager.py).
"""

import tempfile
import unittest
from pathlib import Path

try:
    from fastapi.testclient import TestClient

    from manager.api import create_app
    from manager.app import DeploymentManager, NodeStore

    HAVE_FASTAPI = True
except Exception:  # pragma: no cover - exercised only without third-party deps
    HAVE_FASTAPI = False


class FakeExecutor:
    def __init__(self, versions):
        self.versions = versions
        self.calls = []

    def run(self, node, command):
        self.calls.append((node.name, tuple(command)))
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


@unittest.skipUnless(HAVE_FASTAPI, "fastapi/httpx not installed")
class ApiTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        store = NodeStore(Path(self.tmpdir.name) / "nodes.json")
        manager = DeploymentManager(
            FakeExecutor({"a": "liburma-1.0", "b": "liburma-1.0"}),
            FakeWorkerClient(), store,
        )
        self.client = TestClient(create_app(manager, dist_dir=Path(self.tmpdir.name) / "dist"))

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_nodes_crud_roundtrip_without_password_leak(self):
        response = self.client.post("/v1/nodes", json={
            "name": "a", "ip": "10.0.0.1", "password": "secret",
        })
        self.assertEqual(response.status_code, 201)
        nodes = self.client.get("/v1/nodes").json()
        self.assertEqual([node["name"] for node in nodes], ["a"])
        self.assertNotIn("password", nodes[0])
        response = self.client.delete("/v1/nodes/a")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.client.get("/v1/nodes").json(), [])

    def test_deploy_reports_version_consistency(self):
        response = self.client.post("/v1/deploy", json={
            "nodes": [
                {"name": "a", "ip": "10.0.0.1"},
                {"name": "b", "ip": "10.0.0.2"},
            ],
            "artifact": "build/kv-bench",
            "destination": "/opt/kv-bench/build/kv-bench",
            "source_dir": "/opt/kv-bench",
            "umdk_root": "/opt/umdk",
        })
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["consistent"])
        self.assertEqual(body["versions"]["a"], ["liburma-1.0"])

    def test_task_lifecycle_and_result(self):
        create = self.client.post("/v1/tasks", json={
            "task_id": "t1",
            "workers": [
                {"name": "a", "ip": "10.0.0.1", "password": "hunter2"},
                {"name": "b", "ip": "10.0.0.2", "password": "hunter2"},
            ],
            "bench_items": [{"src": "a", "dst": "b", "type": "bidirectional"}],
            "options": {"op": "write", "threads": 4, "duration": 30},
        })
        self.assertEqual(create.status_code, 201)
        self.assertEqual(create.json()["state"], "queued")

        tasks = self.client.get("/v1/tasks").json()
        self.assertEqual(len(tasks), 1)
        self.assertNotIn("password", tasks[0]["workers"]["a"])
        self.assertNotIn("password", str(tasks))

        start = self.client.post("/v1/tasks/t1/start")
        self.assertEqual(start.status_code, 200)
        self.assertEqual(start.json()["state"], "running")
        self.assertEqual(len(start.json()["commands"]), 2)

        result = self.client.get("/v1/tasks/t1/result")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["aggregate"]["ops"], 30)

        stop = self.client.post("/v1/tasks/t1/stop")
        self.assertEqual(stop.status_code, 200)
        self.assertEqual(stop.json()["state"], "stopped")

    def test_errors_map_to_http_status(self):
        missing = self.client.post("/v1/tasks/nope/start")
        self.assertEqual(missing.status_code, 404)
        duplicate = self.client.post("/v1/tasks", json={
            "task_id": "dup",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b"}],
        })
        self.assertEqual(duplicate.status_code, 201)
        duplicate_again = self.client.post("/v1/tasks", json={
            "task_id": "dup",
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "b"}],
        })
        self.assertEqual(duplicate_again.status_code, 400)
        bad_item = self.client.post("/v1/tasks", json={
            "workers": [{"name": "a", "ip": "10.0.0.1"}],
            "bench_items": [{"src": "a", "dst": "a"}],
        })
        self.assertEqual(bad_item.status_code, 400)

    def test_index_serves_hint_without_built_frontend(self):
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("kv-bench Manager", response.text)
        self.assertIn("npm run build", response.text)


if __name__ == "__main__":
    unittest.main()
