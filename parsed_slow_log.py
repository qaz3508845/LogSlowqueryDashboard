import re
import json
import os

# === 設定檔案路徑 ===
input_path = "mysql-slow.log-20250531"         # 慢查詢 log 檔案
output_path = "parsed_slow_log.json"           # JSON 結果檔案

def parse_and_analyze_log(input_path, output_path):
    # === 讀取 LOG 檔 ===
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    # === 依照每筆查詢切割 ===
    entries = re.split(r"(?=^# Time: )", content, flags=re.MULTILINE)
    parsed_entries = []

    # Table 擷取用正規表達式
    table_pattern = re.compile(r"(?:from|join)\s+`?(\w+)`?(?:\s+as|\s+\w+)?", re.IGNORECASE)

    for entry in entries:
        if not entry.strip():
            continue

        parsed = {
            "time": None,
            "user": None,
            "host": None,
            "thread_id": None,
            "schema": None,
            "qc_hit": None,
            "query_time": None,
            "lock_time": None,
            "rows_sent": None,
            "rows_examined": None,
            "rows_affected": None,
            "bytes_sent": None,
            "timestamp": None,
            "sql": None,
            "tables_used": []
        }

        # 擷取各欄位資訊
        if m := re.search(r"# Time: (.+)", entry):
            parsed["time"] = m.group(1).strip()

        if m := re.search(r"# User@Host: (.+)\[(.+)\] @  \[(.*)\]", entry):
            parsed["user"] = m.group(2).strip()
            parsed["host"] = m.group(3).strip()

        if m := re.search(r"# Thread_id: (\d+)\s+Schema: (\w+)\s+QC_hit: (\w+)", entry):
            parsed["thread_id"] = int(m.group(1))
            parsed["schema"] = m.group(2)
            parsed["qc_hit"] = m.group(3)

        if m := re.search(r"# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)", entry):
            parsed["query_time"] = float(m.group(1))
            parsed["lock_time"] = float(m.group(2))
            parsed["rows_sent"] = int(m.group(3))
            parsed["rows_examined"] = int(m.group(4))

        if m := re.search(r"# Rows_affected: (\d+)\s+Bytes_sent: (\d+)", entry):
            parsed["rows_affected"] = int(m.group(1))
            parsed["bytes_sent"] = int(m.group(2))

        if m := re.search(r"SET timestamp=(\d+);", entry):
            parsed["timestamp"] = int(m.group(1))

        if m := re.search(r"SET timestamp=\d+;\n(.+)", entry, re.DOTALL):
            sql = m.group(1).strip()
            parsed["sql"] = sql

            # 分析 SQL 用到哪些資料表
            tables = table_pattern.findall(sql)
            parsed["tables_used"] = sorted(set(tables))

        parsed_entries.append(parsed)

    # === 輸出 JSON ===
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(parsed_entries, f, ensure_ascii=False, indent=2)

    print(f"✅ 已完成解析與分析，結果儲存到：{output_path}")

# 執行主程式
parse_and_analyze_log(input_path, output_path)
