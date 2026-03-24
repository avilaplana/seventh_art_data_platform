import os

import httpx
from mcp.server.fastmcp import FastMCP

AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8088/api/v1")
AUTH = (
    os.environ.get("AIRFLOW_USER", "admin"),
    os.environ.get("AIRFLOW_PASSWORD", "admin"),
)

mcp = FastMCP("airflow")


def _client() -> httpx.Client:
    return httpx.Client(auth=AUTH, timeout=30)


# ---------------------------------------------------------------------------
# DAG tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_dags() -> list[dict]:
    """List all DAGs with their id, description, schedule, and paused status."""
    with _client() as client:
        r = client.get(f"{AIRFLOW_BASE_URL}/dags")
        r.raise_for_status()
        return [
            {
                "dag_id": d["dag_id"],
                "description": d.get("description"),
                "schedule_interval": d.get("schedule_interval"),
                "is_paused": d.get("is_paused"),
                "is_active": d.get("is_active"),
                "last_parsed_time": d.get("last_parsed_time"),
            }
            for d in r.json()["dags"]
        ]


@mcp.tool()
def trigger_dag(dag_id: str, conf: dict | None = None) -> dict:
    """Trigger a DAG run. Optionally pass a conf dict to the run."""
    with _client() as client:
        payload = {"conf": conf or {}}
        r = client.post(f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns", json=payload)
        r.raise_for_status()
        data = r.json()
        return {
            "dag_id": data["dag_id"],
            "dag_run_id": data["dag_run_id"],
            "state": data["state"],
            "logical_date": data.get("logical_date"),
            "start_date": data.get("start_date"),
        }


@mcp.tool()
def list_dag_runs(dag_id: str, limit: int = 10) -> list[dict]:
    """List the most recent runs for a DAG."""
    with _client() as client:
        r = client.get(
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns",
            params={"limit": limit, "order_by": "-start_date"},
        )
        r.raise_for_status()
        return [
            {
                "dag_run_id": run["dag_run_id"],
                "state": run["state"],
                "start_date": run.get("start_date"),
                "end_date": run.get("end_date"),
                "logical_date": run.get("logical_date"),
            }
            for run in r.json()["dag_runs"]
        ]


@mcp.tool()
def get_dag_run_status(dag_id: str, dag_run_id: str) -> dict:
    """Get the status and timing of a specific DAG run."""
    with _client() as client:
        r = client.get(f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}")
        r.raise_for_status()
        data = r.json()
        return {
            "dag_id": data["dag_id"],
            "dag_run_id": data["dag_run_id"],
            "state": data["state"],
            "start_date": data.get("start_date"),
            "end_date": data.get("end_date"),
            "note": data.get("note"),
        }


@mcp.tool()
def get_task_instances(dag_id: str, dag_run_id: str) -> list[dict]:
    """List all task instances for a DAG run with their state and duration."""
    with _client() as client:
        r = client.get(
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        )
        r.raise_for_status()
        return [
            {
                "task_id": t["task_id"],
                "state": t.get("state"),
                "start_date": t.get("start_date"),
                "end_date": t.get("end_date"),
                "duration": t.get("duration"),
                "try_number": t.get("try_number"),
            }
            for t in r.json()["task_instances"]
        ]


@mcp.tool()
def get_task_logs(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1) -> str:
    """Get the logs for a specific task instance."""
    with _client() as client:
        r = client.get(
            f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
            headers={"Accept": "text/plain"},
        )
        r.raise_for_status()
        return r.text


@mcp.tool()
def pause_dag(dag_id: str) -> dict:
    """Pause a DAG so it stops being scheduled."""
    with _client() as client:
        r = client.patch(f"{AIRFLOW_BASE_URL}/dags/{dag_id}", json={"is_paused": True})
        r.raise_for_status()
        return {"dag_id": dag_id, "is_paused": r.json()["is_paused"]}


@mcp.tool()
def unpause_dag(dag_id: str) -> dict:
    """Unpause a DAG so it resumes being scheduled."""
    with _client() as client:
        r = client.patch(f"{AIRFLOW_BASE_URL}/dags/{dag_id}", json={"is_paused": False})
        r.raise_for_status()
        return {"dag_id": dag_id, "is_paused": r.json()["is_paused"]}


if __name__ == "__main__":
    mcp.run()
