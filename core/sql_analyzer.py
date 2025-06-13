"""
SQL 查詢分析器
"""

import re
import statistics
from typing import List, Dict, Any
from collections import defaultdict, Counter
from models.schemas import QueryEntry, SummaryEntry, PerformanceStats


class SQLAnalyzer:
    """SQL 查詢分析器"""
    
    @staticmethod
    def normalize_sql(sql: str) -> str:
        """
        SQL 樣板轉換函式
        
        Args:
            sql: 原始 SQL 語句
            
        Returns:
            str: 正規化後的 SQL 樣板
        """
        sql = sql.lower()
        sql = re.sub(r"\s+", " ", sql)
        sql = re.sub(r"'[^']*'", "?", sql)
        sql = re.sub(r'"[^"]*"', "?", sql)
        sql = re.sub(r"\b\d+\.\d+\b", "?", sql)
        sql = re.sub(r"\b\d+\b", "?", sql)
        sql = re.sub(r"\(\s*(\?,\s*)*\?\s*\)", "(?)", sql)
        return sql.strip()
    
    @staticmethod
    def get_sql_type(sql: str) -> str:
        """
        判斷 SQL 類型
        
        Args:
            sql: SQL 語句
            
        Returns:
            str: SQL 類型 (SELECT, INSERT, UPDATE, DELETE, REPLACE, CALL, OTHER)
        """
        match = re.match(r"^\s*(\w+)", sql.lower())
        if not match:
            return "OTHER"
        keyword = match.group(1).upper()
        if keyword in {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "CALL"}:
            return keyword
        return "OTHER"
    
    def create_summary_data(self, raw_data: List[QueryEntry]) -> List[SummaryEntry]:
        """
        建立統計摘要資料
        
        Args:
            raw_data: 原始查詢資料列表
            
        Returns:
            List[SummaryEntry]: 統計摘要列表
        """
        sql_groups = defaultdict(list)
        
        for item in raw_data:
            if item.sql:
                norm_sql = self.normalize_sql(item.sql)
                sql_groups[norm_sql].append(item)
        
        summary = []
        for norm_sql, entries in sql_groups.items():
            count = len(entries)
            total_time = sum(
                e.query_time for e in entries 
                if e.query_time is not None
            )
            avg_time = total_time / count if count else 0
            sql_type = self.get_sql_type(norm_sql)
            
            # 收集所有涉及的表格
            all_tables = set()
            for entry in entries:
                all_tables.update(entry.tables_used)
            
            summary_entry = SummaryEntry(
                template=norm_sql,
                type=sql_type,
                count=count,
                avg_query_time=round(avg_time, 4),
                tables_used=sorted(list(all_tables))
            )
            summary.append(summary_entry)
        
        return summary
    
    def build_template_to_raw_mapping(self, raw_data: List[QueryEntry]) -> Dict[str, List[Dict[str, Any]]]:
        """
        建立樣板對應原始資料的映射
        
        Args:
            raw_data: 原始查詢資料列表
            
        Returns:
            Dict[str, List[Dict[str, Any]]]: 樣板對應關係字典
        """
        template_to_raw = defaultdict(list)
        
        for item in raw_data:
            if item.sql:
                normalized = self.normalize_sql(item.sql)
                template_to_raw[normalized].append({
                    "original_sql": item.sql,
                    "query_time": item.query_time or 0,
                    "time": item.time or "",
                    "user": item.user or "",
                    "host": item.host or "",
                    "rows_examined": item.rows_examined or 0,
                    "rows_sent": item.rows_sent or 0,
                    "lock_time": item.lock_time or 0,
                    "timestamp": item.timestamp or 0,
                    "thread_id": item.thread_id or 0,
                    "schema": item.schema or "",
                    "tables_used": item.tables_used
                })
        
        return dict(template_to_raw)
    
    def calculate_performance_stats(self, raw_data: List[QueryEntry]) -> Dict[str, Any]:
        """
        計算效能統計資料
        
        Args:
            raw_data: 原始查詢資料列表
            
        Returns:
            Dict[str, Any]: 完整的效能統計資料
        """
        # 基本統計
        query_times = [
            item.query_time for item in raw_data 
            if item.query_time is not None
        ]
        
        if not query_times:
            return {"error": "無查詢時間資料"}
        
        # SQL 類型統計
        type_stats = Counter()
        time_by_type = defaultdict(list)
        
        for item in raw_data:
            if item.sql:
                sql_type = self.get_sql_type(item.sql)
                type_stats[sql_type] += 1
                if item.query_time:
                    time_by_type[sql_type].append(item.query_time)
        
        # 時間分布統計
        time_ranges = {
            "0-1s": 0,
            "1-5s": 0, 
            "5-10s": 0,
            "10-30s": 0,
            "30s+": 0
        }
        
        for qt in query_times:
            if qt < 1:
                time_ranges["0-1s"] += 1
            elif qt < 5:
                time_ranges["1-5s"] += 1
            elif qt < 10:
                time_ranges["5-10s"] += 1
            elif qt < 30:
                time_ranges["10-30s"] += 1
            else:
                time_ranges["30s+"] += 1
        
        # 用戶統計
        user_stats = Counter()
        for item in raw_data:
            if item.user:
                user_stats[item.user] += 1
        
        # 表格使用統計
        table_stats = Counter()
        for item in raw_data:
            for table in item.tables_used:
                table_stats[table] += 1
        
        return {
            "basic_stats": {
                "total_queries": len(raw_data),
                "avg_query_time": round(statistics.mean(query_times), 4),
                "median_query_time": round(statistics.median(query_times), 4),
                "max_query_time": round(max(query_times), 4),
                "min_query_time": round(min(query_times), 4)
            },
            "type_stats": dict(type_stats.most_common()),
            "time_ranges": time_ranges,
            "user_stats": dict(user_stats.most_common(10)),
            "table_stats": dict(table_stats.most_common(20)),
            "type_performance": {
                sql_type: {
                    "count": len(times),
                    "avg_time": round(statistics.mean(times), 4),
                    "max_time": round(max(times), 4)
                } for sql_type, times in time_by_type.items() if times
            }
        } 