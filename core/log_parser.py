"""
MySQL 慢查詢 LOG 解析器
"""

import re
from typing import List
from models.schemas import QueryEntry


class LogParser:
    """MySQL 慢查詢 LOG 解析器"""
    
    def __init__(self):
        self.table_pattern = re.compile(
            r"(?:from|join)\s+`?(\w+)`?(?:\s+as|\s+\w+)?", 
            re.IGNORECASE
        )
    
    def parse_slow_log(self, content: str) -> List[QueryEntry]:
        """
        解析慢查詢 LOG 內容
        
        Args:
            content: LOG 檔案內容
            
        Returns:
            List[QueryEntry]: 解析後的查詢記錄列表
        """
        entries = re.split(r"(?=^# Time: )", content, flags=re.MULTILINE)
        parsed_entries = []
        
        for entry in entries:
            if not entry.strip():
                continue
                
            parsed_entry = self._parse_single_entry(entry)
            if parsed_entry:
                parsed_entries.append(parsed_entry)
        
        return parsed_entries
    
    def _parse_single_entry(self, entry: str) -> QueryEntry:
        """
        解析單個查詢記錄
        
        Args:
            entry: 單個查詢記錄的文字
            
        Returns:
            QueryEntry: 解析後的查詢記錄
        """
        parsed = QueryEntry()
        
        # 解析時間
        if m := re.search(r"# Time: (.+)", entry):
            parsed.time = m.group(1).strip()
        
        # 解析用戶和主機
        if m := re.search(r"# User@Host: (.+)\[(.+)\] @  \[(.*)\]", entry):
            parsed.user = m.group(2).strip()
            parsed.host = m.group(3).strip()
        
        # 解析線程 ID、Schema、QC_hit
        if m := re.search(r"# Thread_id: (\d+)\s+Schema: (\w+)\s+QC_hit: (\w+)", entry):
            parsed.thread_id = int(m.group(1))
            parsed.schema = m.group(2)
            parsed.qc_hit = m.group(3)
        
        # 解析查詢時間相關資訊
        if m := re.search(r"# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)", entry):
            parsed.query_time = float(m.group(1))
            parsed.lock_time = float(m.group(2))
            parsed.rows_sent = int(m.group(3))
            parsed.rows_examined = int(m.group(4))
        
        # 解析影響行數和傳送位元組數
        if m := re.search(r"# Rows_affected: (\d+)\s+Bytes_sent: (\d+)", entry):
            parsed.rows_affected = int(m.group(1))
            parsed.bytes_sent = int(m.group(2))
        
        # 解析時間戳記
        if m := re.search(r"SET timestamp=(\d+);", entry):
            parsed.timestamp = int(m.group(1))
        
        # 解析 SQL 語句和使用的表格
        if m := re.search(r"SET timestamp=\d+;\n(.+)", entry, re.DOTALL):
            sql = m.group(1).strip()
            parsed.sql = sql
            
            # 分析使用的表格
            tables = self.table_pattern.findall(sql)
            parsed.tables_used = sorted(set(tables))
        
        return parsed 