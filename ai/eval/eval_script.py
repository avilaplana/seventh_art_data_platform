import yaml
import requests
from pathlib import Path
import json
import time
import logging
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# API endpoint
url = "http://localhost:8001/query"

# Model & version
model = "qwen2.5-coder"
version = 6

# Paths
questions_to_eval = Path("/usr/app/ai/eval/questions_analytics.yml")
results_file = Path(f"/usr/app/ai/eval/{model}_v{version}_results.txt")
summary_file = Path(f"/usr/app/ai/eval/{model}_v{version}_summary.json")

# Load YAML
with open(questions_to_eval, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

questions = data.get("questions", [])

# Accumulate all results in a list
all_results = []

for q in questions:
    question_id = q.get("id")
    logger.info(f"Evaluating question ID: {question_id}")
    question_text = q.get("natural_language")
    question_sql_expected = q.get("sql_expected")
    question_difficulty = q.get("difficulty")
    if not question_text:
        continue

    payload = {
        "question": question_text,
        "model": model,
        "version": version
    }

    try:
        response = requests.post(url, json=payload, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        result = response.json()

        # Build record
        record = {
            "id": question_id,
            "difficulty": question_difficulty,
            "question": question_text,
            "sql_generated": result.get("sql"),
            "error": result.get("error"),
            "metrics": result.get("metrics"),
            "sql_expected": question_sql_expected
        }

    except requests.RequestException as e:
        logger.error(f"Failed to query '{question_text}': {e}")
        record = {
            "id": question_id,
            "difficulty": question_difficulty,
            "question": question_text,
            "error": str(e)
        }

    all_results.append(record)

    # Optional small delay to avoid overloading local server
    time.sleep(0.1)

# Write all results as a JSON array
with open(results_file, "w", encoding="utf-8") as f_out:
    json.dump(all_results, f_out, indent=2)

logger.info(f"Saved {len(all_results)} results to {results_file}")

# -------------------------------------------------------
# Scoring
# -------------------------------------------------------

def is_pass(record: dict) -> bool:
    """A result passes if it has a non-empty sql and no error field."""
    sql = record.get("sql_generated") or ""
    error = record.get("error")
    return bool(sql.strip()) and not error

total = len(all_results)
pass_count = sum(1 for r in all_results if is_pass(r))
fail_count = total - pass_count
pass_rate = round(pass_count / total * 100, 1) if total > 0 else 0.0

# Breakdown by difficulty
difficulty_stats: dict = defaultdict(lambda: {"total": 0, "pass": 0, "fail": 0})
for r in all_results:
    diff = r.get("difficulty") or "unknown"
    difficulty_stats[diff]["total"] += 1
    if is_pass(r):
        difficulty_stats[diff]["pass"] += 1
    else:
        difficulty_stats[diff]["fail"] += 1

# Error count by type
error_counts: dict = defaultdict(int)
for r in all_results:
    if not is_pass(r):
        err = r.get("error") or "no_sql_generated"
        # Classify by first meaningful token
        key = err.split(":")[0].strip() if err else "unknown"
        error_counts[key] += 1

# Build summary
summary = {
    "model": model,
    "version": version,
    "total": total,
    "pass": pass_count,
    "fail": fail_count,
    "pass_rate_pct": pass_rate,
    "by_difficulty": {k: dict(v) for k, v in difficulty_stats.items()},
    "error_counts": dict(error_counts),
}

# Persist summary
with open(summary_file, "w", encoding="utf-8") as f_sum:
    json.dump(summary, f_sum, indent=2)

# Print human-readable summary
logger.info("=" * 50)
logger.info("EVAL SUMMARY")
logger.info("=" * 50)
logger.info(f"Total questions : {total}")
logger.info(f"Pass            : {pass_count}")
logger.info(f"Fail            : {fail_count}")
logger.info(f"Pass rate       : {pass_rate}%")
if difficulty_stats:
    logger.info("--- By difficulty ---")
    for diff, stats in sorted(difficulty_stats.items()):
        diff_rate = round(stats["pass"] / stats["total"] * 100, 1) if stats["total"] > 0 else 0.0
        logger.info(f"  {diff:12s}: {stats['pass']}/{stats['total']} ({diff_rate}%)")
if error_counts:
    logger.info("--- Error breakdown ---")
    for err_type, count in sorted(error_counts.items(), key=lambda x: -x[1]):
        logger.info(f"  {err_type}: {count}")
logger.info(f"Summary saved to {summary_file}")