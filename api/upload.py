"""
檔案上傳相關 API 路由
"""

from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from core.data_manager import DataManager

router = APIRouter(prefix="/api", tags=["upload"])


def create_upload_routes(data_manager: DataManager):
    """創建上傳相關路由"""
    
    @router.post("/upload_log")
    async def upload_log(
        file: UploadFile = File(...),
        analysis_name: str = Form(...)
    ):
        """上傳並分析LOG檔案"""
        
        # 檢查檔案類型
        if not file.filename or not file.filename.endswith(('.log', '.txt')):
            raise HTTPException(status_code=400, detail="只支援 .log 或 .txt 檔案")
        
        try:
            # 讀取檔案內容
            content = await file.read()
            log_content = content.decode("utf-8", errors="ignore")
            
            # 儲存並分析
            result = data_manager.save_analysis(analysis_name, log_content, file.filename)
            
            return result
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"處理檔案時發生錯誤: {str(e)}")
    
    return router 