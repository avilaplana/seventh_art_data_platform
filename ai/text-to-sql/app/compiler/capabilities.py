from pathlib import Path
import glob
import os
import threading
from ollama import Client
import jaydebeapi
from typing import List, Dict, Any, Mapping
import yaml
from jinja2 import Template

# -----------------------------
# Resources
# -----------------------------
SPARK_THRIFT_HOST = "spark-thrift-server"
SPARK_THRIFT_PORT = 10000
DATABASE = "demo.stage_analytics"
SPARK_JARS = ":".join(glob.glob("/opt/bitnami/spark/jars/*.jar"))
QUERY_TIMEOUT_SEC = int(os.getenv("QUERY_TIMEOUT_SEC", "30"))

# load schemas once
DATA_FOLDER = Path(__file__).parent.parent.parent / "data"

SPARK_CLIENT = jaydebeapi.connect(
    "org.apache.hive.jdbc.HiveDriver",
    f"jdbc:hive2://{SPARK_THRIFT_HOST}:{SPARK_THRIFT_PORT}/{DATABASE}",
    ["dbt", ""],
    SPARK_JARS,
)

LLM_CLIENT = Client(host="http://host.docker.internal:11434")

# -----------------------------
# Functions
# -----------------------------

def load_prompt_configuration(model: str, version: int) -> Any:
    with open(f"/usr/app/ai/eval/version_control/{model}/eval_config_v{version}.yml", "r") as f:
        return yaml.safe_load(f)    

def _build_messages(user_query: str, prompt_config: Any, history: List[Dict[str, str]]) -> List[Dict[str, str]]:
    templates = prompt_config["eval_config"]["prompt_templates"]

    system_prompt = templates["system"]

    user_prompt = Template(templates["user_template"]).render(
        schema_block=templates["schema_block"],
        semantic_block=templates["semantic_block"],
        question=user_query,
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    for attempt in history:
        messages.append({"role": "assistant", "content": attempt["sql"]})
        messages.append({"role": "user", "content": f"That query failed with error: {attempt['error']}\nPlease fix the SQL and output only valid SQL."})

    return messages


def generate_sql(user_query: str, prompt_config: Any, history: List[Dict[str, str]] = []) -> Mapping[str, Any]:
    messages = _build_messages(user_query, prompt_config, history)
    return LLM_CLIENT.chat(
        model=prompt_config["eval_config"]["model"],
        messages=messages,
        options={
            "num_ctx": prompt_config["eval_config"]["window_size"],
            "temperature": prompt_config["eval_config"]["sampling"]["temperature"],
            "top_p": prompt_config["eval_config"]["sampling"]["top_p"],
        }
    )


def execute_sql_query(sql: str) -> Dict[str, Any]:
    result_container: Dict[str, Any] = {}
    exception_container: Dict[str, Exception] = {}

    def _run() -> None:
        try:
            cursor = SPARK_CLIENT.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()
            result_container["result"] = {"columns": columns, "rows": rows}
        except Exception as exc:  # noqa: BLE001
            exception_container["exc"] = exc

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    thread.join(timeout=QUERY_TIMEOUT_SEC)

    if thread.is_alive():
        raise TimeoutError(
            f"Query execution exceeded the {QUERY_TIMEOUT_SEC}s timeout limit"
        )

    if "exc" in exception_container:
        raise exception_container["exc"]

    return result_container["result"]


