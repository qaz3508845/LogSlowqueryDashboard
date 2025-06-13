"""
資料結構定義
"""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class QueryEntry:
    """單個查詢記錄"""
    time: Optional[str] = None
    user: Optional[str] = None
    host: Optional[str] = None
    thread_id: Optional[int] = None
    schema: Optional[str] = None
    qc_hit: Optional[str] = None
    query_time: Optional[float] = None
    lock_time: Optional[float] = None
    rows_sent: Optional[int] = None
    rows_examined: Optional[int] = None
    rows_affected: Optional[int] = None
    bytes_sent: Optional[int] = None
    timestamp: Optional[int] = None
    sql: Optional[str] = None
    tables_used: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if self.tables_used is None:
            self.tables_used = []


@dataclass  
class SummaryEntry:
    """SQL樣板統計摘要"""
    template: str
    type: str
    count: int
    avg_query_time: float
    tables_used: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if self.tables_used is None:
            self.tables_used = []


@dataclass
class AnalysisMetadata:
    """分析檔案元資料"""
    original_filename: str
    upload_time: str
    total_queries: int
    total_templates: int
    merge_info: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceStats:
    """效能統計資料"""
    total_queries: int
    avg_query_time: float
    median_query_time: float
    max_query_time: float
    min_query_time: float


@dataclass
class CurrentAnalysis:
    """當前分析資料"""
    name: str
    summary_data: List[SummaryEntry]
    template_to_raw_dict: Dict[str, List[Dict[str, Any]]]
    raw_data: List[QueryEntry] 