import re
import json
from collections import defaultdict

# === 載入 JSON 檔 ===
with open("parsed_slow_log.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# === SQL 樣板轉換函式 ===
def normalize_sql(sql: str) -> str:
    sql = sql.lower()
    sql = re.sub(r"\s+", " ", sql)
    sql = re.sub(r"'[^']*'", "?", sql)
    sql = re.sub(r'"[^"]*"', "?", sql)
    sql = re.sub(r"\b\d+\.\d+\b", "?", sql)
    sql = re.sub(r"\b\d+\b", "?", sql)
    sql = re.sub(r"\(\s*(\?,\s*)*\?\s*\)", "(?)", sql)
    return sql.strip()

# === SQL 類型判斷 ===
def get_sql_type(sql: str) -> str:
    match = re.match(r"^\s*(\w+)", sql.lower())
    if not match:
        return "OTHER"
    keyword = match.group(1).upper()
    if keyword in {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "CALL"}:
        return keyword
    return "OTHER"

# === 分組統計 ===
sql_groups = defaultdict(list)

for item in data:
    sql = item.get("sql")
    if sql:
        norm_sql = normalize_sql(sql)
        sql_groups[norm_sql].append(item)

# === 組裝結果 ===
summary = []

for norm_sql, entries in sql_groups.items():
    count = len(entries)
    total_time = sum(e["query_time"] for e in entries if e.get("query_time") is not None)
    avg_time = total_time / count if count else 0
    sql_type = get_sql_type(norm_sql)

    summary.append({
        "template": norm_sql,
        "type": sql_type,
        "count": count,
        "avg_query_time": round(avg_time, 4)
    })

# === 輸出 JSON 檔 ===
with open("normalized_sql_summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)

print("✅ 已輸出分析報告至：normalized_sql_summary.json")
