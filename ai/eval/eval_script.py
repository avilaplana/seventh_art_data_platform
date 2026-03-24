import yaml
import requests
from pathlib import Path
import json
import time

# API endpoint
url = "http://localhost:8001/query"

# Model & version
model = "qwen2.5-coder"
version = 6

# Paths
questions_to_eval = Path("/usr/app/ai/eval/questions_analytics.yml")
results_file = Path(f"/usr/app/ai/eval/{model}_v{version}_results.txt")

# Load YAML
with open(questions_to_eval, "r", encoding="utf-8") as f:
    data = yaml.safe_load(f)

questions = data.get("questions", [])

# Accumulate all results in a list
all_results = []

for q in questions:
    question_id = q.get("id")
    print(f"Evaluating question ID: {question_id}")
    question_text = q.get("natural_language")
    question_sql_expected = q.get("sql_expected")
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
            "question": question_text,
            "sql_generated": result.get("sql"),
            "error": result.get("error"),
            "metrics": result.get("metrics"),
            "sql_expected": question_sql_expected            
        }

    except requests.RequestException as e:
        print(f"Failed to query '{question_text}': {e}")
        record = {
            "id": question_id,
            "question": question_text,
            "error": str(e)
        }

    all_results.append(record)

    # Optional small delay to avoid overloading local server
    time.sleep(0.1)

# Write all results as a JSON array
with open(results_file, "w", encoding="utf-8") as f_out:
    json.dump(all_results, f_out, indent=2)

print(f"Saved {len(all_results)} results to {results_file}")