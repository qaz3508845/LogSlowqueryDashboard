"""
檔案上傳相關 API 路由
"""

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from typing import List
from core.data_manager import DataManager

router = APIRouter(prefix="/api", tags=["upload"])


def create_upload_routes(data_manager: DataManager):
    """創建上傳相關路由"""
    
    @router.post("/upload_log")
    async def upload_log(
        file: UploadFile = File(...),
        analysis_name: str = Form(...)
    ):
        """上傳單個檔案分析"""
        
        try:
            # 讀取檔案內容
            content = await file.read()
            log_content = content.decode("utf-8", errors="ignore")
            
            # 確保檔案名稱不為 None
            filename = file.filename or "unknown_file"
            
            # 儲存並分析
            result = data_manager.save_analysis(analysis_name, log_content, filename)
            
            return result
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"處理檔案時發生錯誤: {str(e)}")
    
    @router.post("/upload_multiple_logs")
    async def upload_multiple_logs(
        files: List[UploadFile] = File(...),
        analysis_name: str = Form(...)
    ):
        """批量上傳多個檔案並合併分析"""
        
        if not files:
            raise HTTPException(status_code=400, detail="請至少選擇一個檔案")
        
        if len(files) > 20:
            raise HTTPException(status_code=400, detail="一次最多上傳 20 個檔案")
        
        try:
            # 收集所有檔案內容
            all_content = []
            file_info = []
            
            for file in files:
                filename = file.filename or "unknown_file"
                    
                # 讀取檔案內容
                content = await file.read()
                try:
                    # 嘗試使用 UTF-8 解碼
                    log_content = content.decode("utf-8", errors="ignore")
                except:
                    try:
                        # 如果失敗，嘗試其他編碼
                        log_content = content.decode("gbk", errors="ignore")
                    except:
                        log_content = content.decode("latin-1", errors="ignore")
                
                all_content.append(log_content)
                file_info.append({
                    "filename": filename,
                    "size": len(content)
                })
            
            if not all_content:
                raise HTTPException(status_code=400, detail="沒有有效的檔案內容")
            
            # 合併所有檔案內容
            merged_content = "\n".join(all_content)
            
            # 生成合併檔案名稱
            if len(files) == 1:
                merged_filename = files[0].filename or "unknown_file"
            else:
                merged_filename = f"merged_{len(files)}_files"
            
            # 儲存並分析
            result = data_manager.save_analysis(analysis_name, merged_content, merged_filename)
            
            # 增加批量上傳的詳細資訊
            result["upload_type"] = "multiple"
            result["files_count"] = len(file_info)
            result["files_info"] = file_info
            result["total_size"] = sum(info["size"] for info in file_info)
            
            return result
            
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"處理多檔案時發生錯誤: {str(e)}")
    
    return router 