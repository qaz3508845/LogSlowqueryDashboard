"""
查詢相關 API 路由
"""

from fastapi import APIRouter, Query, HTTPException
from core.data_manager import DataManager
from core.sql_analyzer import SQLAnalyzer
from typing import Optional, List, Dict, Any


def create_query_routes(data_manager: DataManager):
    """創建查詢相關路由"""
    
    # 在函數內創建 router，確保每次都是新的實例
    router = APIRouter(prefix="/api", tags=["queries"])

    @router.get("/raw_sqls/{template_index}")
    async def get_raw_sqls(template_index: int):
        """取得指定樣板的原始SQL列表"""
        if 0 <= template_index < len(data_manager.current_analysis.summary_data):
            template = data_manager.current_analysis.summary_data[template_index].template
            raw_sqls = data_manager.current_analysis.template_to_raw_dict.get(template, [])
            return {"raw_sqls": raw_sqls}
        return {"error": "模板索引無效"}
    
    @router.get("/raw_queries")
    async def get_raw_queries(
        page: int = Query(1, ge=1),
        size: int = Query(50, ge=1, le=1000),
        search: str = Query("", description="搜尋關鍵字"),
        min_time: float = Query(0, ge=0, description="最小查詢時間"),
        sql_type: str = Query("", description="SQL類型篩選"),
        user_filter: str = Query("", description="用戶篩選"),
        table_filter: str = Query("", description="表格篩選")
    ):
        """取得原始查詢列表，支援分頁和篩選"""
        
        # 篩選資料
        filtered_data = []
        for item in data_manager.current_analysis.raw_data:
            if not item.sql:
                continue
                
            # 查詢時間篩選
            if (item.query_time or 0) < min_time:
                continue
                
            # SQL類型篩選
            if sql_type:
                if SQLAnalyzer.get_sql_type(item.sql) != sql_type:
                    continue
                    
            # 用戶篩選
            if user_filter and user_filter.lower() not in (item.user or "").lower():
                continue
                
            # 表格篩選
            if table_filter:
                item_tables = item.tables_used
                table_filters = [t.strip().lower() for t in table_filter.split(',') if t.strip()]
                if table_filters:
                    found = False
                    for table_name in item_tables:
                        for filter_table in table_filters:
                            if filter_table in table_name.lower():
                                found = True
                                break
                        if found:
                            break
                    if not found:
                        continue
                
            # 搜尋關鍵字篩選
            if search and search.lower() not in (item.sql or "").lower():
                continue
                
            filtered_data.append(item)
        
        # 排序（依查詢時間降序）
        filtered_data.sort(key=lambda x: x.query_time or 0, reverse=True)
        
        # 分頁
        total = len(filtered_data)
        start = (page - 1) * size
        end = start + size
        page_data = filtered_data[start:end]
        
        # 格式化資料
        formatted_data = []
        for item in page_data:
            formatted_data.append({
                "sql": item.sql,
                "query_time": item.query_time or 0,
                "lock_time": item.lock_time or 0,
                "rows_examined": item.rows_examined or 0,
                "rows_sent": item.rows_sent or 0,
                "user": item.user or "",
                "host": item.host or "",
                "time": item.time or "",
                "schema": item.schema or "",
                "thread_id": item.thread_id or 0,
                "sql_type": SQLAnalyzer.get_sql_type(item.sql),
                "tables_used": item.tables_used
            })
        
        return {
            "data": formatted_data,
            "total": total,
            "page": page,
            "size": size,
            "total_pages": (total + size - 1) // size
        }
    
    @router.get("/performance_stats")
    async def get_performance_stats():
        """取得效能分析統計資料"""
        analyzer = SQLAnalyzer()
        stats = analyzer.calculate_performance_stats(data_manager.current_analysis.raw_data)
        return stats
    
    @router.get("/tables_list")
    async def get_tables_list():
        """取得所有使用的表格列表"""
        tables_set = set()
        
        # 從原始資料中收集所有表格
        for item in data_manager.current_analysis.raw_data:
            if hasattr(item, 'tables_used') and item.tables_used:
                tables_set.update(item.tables_used)
        
        # 排序並返回
        tables_list = sorted(list(tables_set))
        return {"tables": tables_list}
    
    return router 