"""
分析資料管理器
"""

import json
import shutil
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime
from models.schemas import QueryEntry, SummaryEntry, AnalysisMetadata, CurrentAnalysis
from core.log_parser import LogParser
from core.sql_analyzer import SQLAnalyzer


class DataManager:
    """分析資料管理器"""
    
    def __init__(self, data_dir: str = "analysis_data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.log_parser = LogParser()
        self.sql_analyzer = SQLAnalyzer()
        self.current_analysis = CurrentAnalysis(
            name="預設分析",
            summary_data=[],
            template_to_raw_dict={},
            raw_data=[]
        )
    
    def load_analysis_data(self, analysis_name: str = "預設分析") -> CurrentAnalysis:
        """
        載入指定的分析資料
        
        Args:
            analysis_name: 分析檔案名稱
            
        Returns:
            CurrentAnalysis: 當前分析資料
        """
        if analysis_name == "預設分析":
            # 載入預設資料
            try:
                with open("normalized_sql_summary.json", "r", encoding="utf-8") as f:
                    summary_dict_list = json.load(f)
                with open("parsed_slow_log.json", "r", encoding="utf-8") as f:
                    raw_dict_list = json.load(f)
                    
                # 轉換為資料類別
                summary_data = [
                    SummaryEntry(**item) for item in summary_dict_list
                ]
                raw_data = [
                    QueryEntry(**item) for item in raw_dict_list
                ]
            except FileNotFoundError:
                summary_data = []
                raw_data = []
        else:
            # 載入指定分析檔案
            analysis_path = self.data_dir / analysis_name
            if not analysis_path.exists():
                raise FileNotFoundError(f"分析檔案不存在: {analysis_name}")
                
            with open(analysis_path / "summary.json", "r", encoding="utf-8") as f:
                summary_dict_list = json.load(f)
            with open(analysis_path / "raw_data.json", "r", encoding="utf-8") as f:
                raw_dict_list = json.load(f)
                
            # 轉換為資料類別
            summary_data = [
                SummaryEntry(**item) for item in summary_dict_list
            ]
            raw_data = [
                QueryEntry(**item) for item in raw_dict_list
            ]
        
        # 建立樣板對應關係
        template_to_raw_dict = self.sql_analyzer.build_template_to_raw_mapping(raw_data)
        
        # 更新當前分析
        self.current_analysis = CurrentAnalysis(
            name=analysis_name,
            summary_data=summary_data,
            template_to_raw_dict=template_to_raw_dict,
            raw_data=raw_data
        )
        
        return self.current_analysis
    
    def save_analysis(self, analysis_name: str, log_content: str, original_filename: str) -> Dict[str, Any]:
        """
        儲存新的分析資料
        
        Args:
            analysis_name: 分析檔案名稱
            log_content: LOG 檔案內容
            original_filename: 原始檔案名稱
            
        Returns:
            Dict[str, Any]: 儲存結果資訊
        """
        # 建立分析目錄
        analysis_path = self.data_dir / analysis_name
        analysis_path.mkdir(exist_ok=True)
        
        try:
            # 儲存原始檔案
            log_file_path = analysis_path / f"original_{original_filename}"
            with open(log_file_path, "w", encoding="utf-8") as f:
                f.write(log_content)
            
            # 解析 LOG
            raw_data = self.log_parser.parse_slow_log(log_content)
            
            # 建立統計摘要
            summary_data = self.sql_analyzer.create_summary_data(raw_data)
            
            # 轉換為字典格式以儲存 JSON
            raw_data_dict = [self._query_entry_to_dict(item) for item in raw_data]
            summary_data_dict = [self._summary_entry_to_dict(item) for item in summary_data]
            
            # 儲存分析結果
            with open(analysis_path / "raw_data.json", "w", encoding="utf-8") as f:
                json.dump(raw_data_dict, f, ensure_ascii=False, indent=2)
            
            with open(analysis_path / "summary.json", "w", encoding="utf-8") as f:
                json.dump(summary_data_dict, f, ensure_ascii=False, indent=2)
            
            # 儲存元資料
            metadata = AnalysisMetadata(
                original_filename=original_filename,
                upload_time=datetime.now().isoformat(),
                total_queries=len(raw_data),
                total_templates=len(summary_data)
            )
            
            with open(analysis_path / "metadata.json", "w", encoding="utf-8") as f:
                json.dump(self._metadata_to_dict(metadata), f, ensure_ascii=False, indent=2)
            
            return {
                "success": True,
                "message": f"LOG檔案 '{original_filename}' 上傳並分析完成",
                "analysis_name": analysis_name,
                "total_queries": len(raw_data),
                "total_templates": len(summary_data)
            }
            
        except Exception as e:
            # 清理失敗的目錄
            if analysis_path.exists():
                shutil.rmtree(analysis_path)
            raise e
    
    def delete_analysis(self, analysis_name: str) -> Dict[str, Any]:
        """
        刪除指定的分析檔案
        
        Args:
            analysis_name: 分析檔案名稱
            
        Returns:
            Dict[str, Any]: 刪除結果資訊
        """
        if analysis_name == "預設分析":
            raise ValueError("無法刪除預設分析")
        
        analysis_path = self.data_dir / analysis_name
        if not analysis_path.exists():
            raise FileNotFoundError("分析檔案不存在")
        
        shutil.rmtree(analysis_path)
        
        # 如果刪除的是當前分析，切換回預設
        if self.current_analysis.name == analysis_name:
            self.load_analysis_data("預設分析")
        
        return {
            "success": True,
            "message": f"分析檔案 '{analysis_name}' 已刪除",
            "current_analysis": self.current_analysis.name
        }
    
    def get_analysis_files(self) -> List[Dict[str, Any]]:
        """
        取得可用的分析檔案列表
        
        Returns:
            List[Dict[str, Any]]: 分析檔案列表
        """
        analysis_files = []
        
        # 檢查是否有預設分析檔案
        has_default_files = (
            Path("parsed_slow_log.json").exists() and 
            Path("normalized_sql_summary.json").exists()
        )
        
        # 只有當預設分析檔案存在且有資料時才顯示
        if has_default_files and self.current_analysis.name == "預設分析" and len(self.current_analysis.raw_data) > 0:
            analysis_files.append({
                "name": "預設分析",
                "is_current": True,
                "metadata": {
                    "total_queries": len(self.current_analysis.raw_data),
                    "total_templates": len(self.current_analysis.summary_data),
                    "upload_time": "內建資料"
                }
            })
        
        # 用戶上傳的分析檔案
        if self.data_dir.exists():
            for item in self.data_dir.iterdir():
                if item.is_dir() and (item / "summary.json").exists():
                    try:
                        with open(item / "metadata.json", "r", encoding="utf-8") as f:
                            metadata = json.load(f)
                    except:
                        metadata = {"upload_time": "未知", "total_queries": 0, "total_templates": 0}
                    
                    analysis_files.append({
                        "name": item.name,
                        "is_current": self.current_analysis.name == item.name,
                        "metadata": metadata
                    })
        
        return analysis_files
    
    def merge_analysis(self, merged_name: str, source_files: List[str]) -> Dict[str, Any]:
        """
        合併多個分析檔案
        
        Args:
            merged_name: 合併後的檔案名稱
            source_files: 來源檔案列表
            
        Returns:
            Dict[str, Any]: 合併結果資訊
        """
        if len(source_files) < 2:
            raise ValueError("至少需要選擇 2 個檔案進行合併")
        
        # 檢查合併檔案名稱是否已存在
        merged_path = self.data_dir / merged_name
        if merged_path.exists():
            raise ValueError("合併檔案名稱已存在，請選擇其他名稱")
        
        # 收集所有原始資料
        all_raw_data = []
        source_metadata = []
        
        for source_name in source_files:
            if source_name == "預設分析":
                if self.current_analysis.name == "預設分析":
                    all_raw_data.extend(self.current_analysis.raw_data)
                    source_metadata.append({
                        "name": "預設分析",
                        "queries": len(self.current_analysis.raw_data),
                        "templates": len(self.current_analysis.summary_data)
                    })
                continue
            
            # 載入分析檔案
            source_path = self.data_dir / source_name
            if not source_path.exists() or not (source_path / "raw_data.json").exists():
                continue
            
            try:
                with open(source_path / "raw_data.json", "r", encoding="utf-8") as f:
                    source_raw_dict = json.load(f)
                    source_raw_data = [QueryEntry(**item) for item in source_raw_dict]
                    all_raw_data.extend(source_raw_data)
                
                # 載入元資料
                try:
                    with open(source_path / "metadata.json", "r", encoding="utf-8") as f:
                        metadata = json.load(f)
                        source_metadata.append({
                            "name": source_name,
                            "queries": metadata.get("total_queries", len(source_raw_data)),
                            "templates": metadata.get("total_templates", 0)
                        })
                except:
                    source_metadata.append({
                        "name": source_name,
                        "queries": len(source_raw_data),
                        "templates": 0
                    })
                    
            except Exception as e:
                print(f"無法載入分析檔案 {source_name}: {e}")
                continue
        
        if not all_raw_data:
            raise ValueError("無法載入任何有效的分析資料")
        
        # 建立合併統計摘要
        merged_summary = self.sql_analyzer.create_summary_data(all_raw_data)
        
        # 建立合併目錄
        merged_path.mkdir(exist_ok=True)
        
        # 轉換為字典格式
        raw_data_dict = [self._query_entry_to_dict(item) for item in all_raw_data]
        summary_data_dict = [self._summary_entry_to_dict(item) for item in merged_summary]
        
        # 儲存合併後的資料
        with open(merged_path / "raw_data.json", "w", encoding="utf-8") as f:
            json.dump(raw_data_dict, f, ensure_ascii=False, indent=2)
        
        with open(merged_path / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary_data_dict, f, ensure_ascii=False, indent=2)
        
        # 建立合併元資料
        merged_metadata = {
            "original_filename": "merged_analysis",
            "upload_time": datetime.now().isoformat(),
            "total_queries": len(all_raw_data),
            "total_templates": len(merged_summary),
            "merge_info": {
                "merged_from": source_files,
                "merge_time": datetime.now().isoformat(),
                "source_details": source_metadata
            }
        }
        
        with open(merged_path / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(merged_metadata, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "message": f"成功合併 {len(source_files)} 個分析檔案",
            "merged_name": merged_name,
            "total_queries": len(all_raw_data),
            "total_templates": len(merged_summary),
            "source_count": len(source_files),
            "source_files": source_files
        }
    
    @staticmethod
    def _query_entry_to_dict(entry: QueryEntry) -> Dict[str, Any]:
        """將 QueryEntry 轉換為字典"""
        return {
            "time": entry.time,
            "user": entry.user,
            "host": entry.host,
            "thread_id": entry.thread_id,
            "schema": entry.schema,
            "qc_hit": entry.qc_hit,
            "query_time": entry.query_time,
            "lock_time": entry.lock_time,
            "rows_sent": entry.rows_sent,
            "rows_examined": entry.rows_examined,
            "rows_affected": entry.rows_affected,
            "bytes_sent": entry.bytes_sent,
            "timestamp": entry.timestamp,
            "sql": entry.sql,
            "tables_used": entry.tables_used
        }
    
    @staticmethod
    def _summary_entry_to_dict(entry: SummaryEntry) -> Dict[str, Any]:
        """將 SummaryEntry 轉換為字典"""
        return {
            "template": entry.template,
            "type": entry.type,
            "count": entry.count,
            "avg_query_time": entry.avg_query_time,
            "tables_used": entry.tables_used
        }
    
    @staticmethod
    def _metadata_to_dict(metadata: AnalysisMetadata) -> Dict[str, Any]:
        """將 AnalysisMetadata 轉換為字典"""
        return {
            "original_filename": metadata.original_filename,
            "upload_time": metadata.upload_time,
            "total_queries": metadata.total_queries,
            "total_templates": metadata.total_templates,
            "merge_info": metadata.merge_info
        } 