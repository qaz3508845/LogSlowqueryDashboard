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
    
    def get_analysis_files(self) -> Dict[str, Any]:
        """獲取所有分析檔案列表"""
        analysis_files = []
        
        # 檢查分析目錄中的分析檔案
        if self.data_dir.exists():
            for analysis_path in self.data_dir.iterdir():
                if analysis_path.is_dir() and (analysis_path / "summary.json").exists():
                    try:
                        metadata = self._load_metadata(analysis_path.name)
                        analysis_files.append({
                            "name": analysis_path.name,
                            "is_current": analysis_path.name == self.current_analysis.name,
                            "metadata": metadata
                        })
                    except Exception as e:
                        print(f"⚠️ 無法載入 {analysis_path.name} 的資訊: {e}")
                
        return {
            "analysis_files": sorted(analysis_files, key=lambda x: x["metadata"].get("upload_time", ""), reverse=True)
        }

    def _load_metadata(self, analysis_name: str) -> Dict[str, Any]:
        """載入分析檔案的元資料"""
        metadata_file = self.data_dir / analysis_name / "metadata.json"
        if metadata_file.exists():
            try:
                with open(metadata_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"⚠️ 載入元資料失敗: {e}")
        
        # 返回預設元資料
        return {
            "total_queries": 0,
            "total_templates": 0,
            "upload_time": "未知"
        }

    def get_current_analysis_info(self) -> Dict[str, Any]:
        """獲取當前分析的基本資訊"""
        # 移除自動載入邏輯，避免在切換分析檔案後又被自動切換回第一個檔案
        # if not self.current_analysis.summary_data and len(self.current_analysis.raw_data) == 0:
        #     # 如果當前沒有資料，嘗試載入第一個可用的分析檔案
        #     files_data = self.get_analysis_files()
        #     if files_data["analysis_files"]:
        #         try:
        #             first_analysis = files_data["analysis_files"][0]["name"]
        #             self.load_analysis_data(first_analysis)
        #             print(f"🔄 自動載入分析檔案: {first_analysis}")
        #         except Exception as e:
        #             print(f"⚠️ 自動載入失敗: {e}")

        metadata = self._load_metadata(self.current_analysis.name)
        return {
            "name": self.current_analysis.name,
            "total_queries": metadata.get("total_queries", 0),
            "total_templates": metadata.get("total_templates", 0),
            "upload_time": metadata.get("upload_time", "未知")
        }

    def get_template_data(self) -> List[Dict[str, Any]]:
        """獲取樣板統計資料（用於模板）"""
        template_data = []
        for item in self.current_analysis.summary_data:
            template_data.append({
                "template": item.template,
                "type": item.type,
                "count": item.count,
                "avg_query_time": item.avg_query_time,
                "tables_used": item.tables_used
            })
        return template_data

    def get_basic_stats(self) -> Dict[str, Any]:
        """獲取基本統計資訊"""
        if not self.current_analysis.raw_data:
            return {
                "total_queries": 0,
                "avg_time": 0.0,
                "max_time": 0.0,
                "median_time": 0.0
            }

        # 過濾掉 None 值
        query_times = [item.query_time for item in self.current_analysis.raw_data if item.query_time is not None]
        if not query_times:
            return {
                "total_queries": len(self.current_analysis.raw_data),
                "avg_time": 0.0,
                "max_time": 0.0,
                "median_time": 0.0
            }

        query_times.sort()
        
        return {
            "total_queries": len(self.current_analysis.raw_data),
            "avg_time": sum(query_times) / len(query_times),
            "max_time": max(query_times),
            "median_time": query_times[len(query_times) // 2]
        }
    
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