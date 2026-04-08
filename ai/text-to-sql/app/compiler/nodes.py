from .state import RunnerState
from . import capabilities
import re

def generate_llm_response_node(state: RunnerState) -> dict:
    llm_response = capabilities.generate_sql(state["user_query"], state["prompt_config"])
    return {"llm_response": llm_response}

def load_prompt_configuration_node(state: RunnerState) -> dict:
    prompt_config = capabilities.load_prompt_configuration(state["model"], state["version"])
    return {"prompt_config": prompt_config}

def execute_sql_node(state: RunnerState) -> dict:
    # If a previous node already produced an error, skip execution
    if state.get("error"):
        return {}

    try:
        db_result = capabilities.execute_sql_query(state["sql_sanitised"])
        return {"db_result": db_result}
    except Exception as e:
        return {"error": str(e)}

def repair_sql_node(state: RunnerState) -> dict:
    history = state["history"] + [{"sql": state["sql_sanitised"], "error": state["error"]}]
    llm_response = capabilities.generate_sql(state["user_query"], state["prompt_config"], history)
    return {
        "llm_response": llm_response,
        "history": history,
        "retry_count": state["retry_count"] + 1,
        "error": None,
    }

_AGGREGATION_KEYWORDS = re.compile(
    r"\b(COUNT|SUM|AVG|MAX|MIN)\s*\(", re.IGNORECASE
)

def _needs_limit(sql: str) -> bool:
    """Return True only when it is safe to inject LIMIT 10.

    LIMIT is NOT injected when the query already has a LIMIT, uses GROUP BY,
    uses HAVING, or contains aggregation functions (COUNT/SUM/AVG/MAX/MIN).
    """
    upper = sql.upper()
    if "LIMIT" in upper:
        return False
    if "GROUP BY" in upper:
        return False
    if "HAVING" in upper:
        return False
    if _AGGREGATION_KEYWORDS.search(sql):
        return False
    return True


def sanitize_sql_node(state: RunnerState) -> dict:
    """
    Strip ```sql``` blocks, leading/trailing whitespace, etc.
    Add 'LIMIT 10' only if the query has no LIMIT, no GROUP BY, no HAVING,
    and no aggregation functions (COUNT, SUM, AVG, MAX, MIN).
    """
    sql = state["llm_response"]["message"]["content"].strip()

    # Remove ```sql ... ``` fences
    if sql.startswith("```sql"):
        sql = "\n".join(sql.splitlines()[1:-1])

    sql = sql.strip()

    if _needs_limit(sql):
        # Preserve existing semicolon if present
        ends_with_semicolon = sql.strip().endswith(";")
        sql = sql.rstrip("; \t\n") + " LIMIT 10"
        if ends_with_semicolon:
            sql += ";"

    return {"sql_sanitised": sql}

FORBIDDEN_KEYWORDS = [
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "create",
    "truncate",
    "merge",
    "grant",
    "revoke"
]

def validate_sql_node(state):
    sql = state.get("sql_sanitised")

    if not sql:
        state["error"] = "No SQL query generated"
        return state

    query = sql.strip().lower()

    # -------------------------
    # Rule 1: SELECT only (including CTE: WITH ... SELECT)
    # -------------------------

    if not (query.startswith("select") or query.startswith("with")):
        state["error"] = "Only SELECT queries are allowed"
        return state

    # -------------------------
    # Rule 2: forbid DDL/DML
    # -------------------------

    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", query):
            state["error"] = f"Forbidden SQL keyword detected: {keyword}"
            return state

    # -------------------------
    # Rule 3: prevent multi statements
    # -------------------------

    if ";" in query[:-1]:
        state["error"] = "Multiple SQL statements are not allowed"
        return state

    # -------------------------
    # Rule 4: enforce LIMIT (exempt aggregation / grouped queries)
    # -------------------------

    has_group_by = "group by" in query
    has_having = "having" in query
    has_aggregation = bool(_AGGREGATION_KEYWORDS.search(query))
    if "limit" not in query and not (has_group_by or has_having or has_aggregation):
        state["error"] = "Query must include a LIMIT clause"
        return state

    return state