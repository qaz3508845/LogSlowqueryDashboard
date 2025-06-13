"""
分析管理相關 API 路由
"""

from fastapi import APIRouter, HTTPException, Request
from core.data_manager import DataManager

router = APIRouter(prefix="/api", tags=["analysis"])


def create_analysis_routes(data_manager: DataManager):
    """創建分析管理相關路由"""
    
    @router.post("/switch_analysis/{analysis_name}")
    async def switch_analysis(analysis_name: str):
        """切換到指定的分析檔案"""
        try:
            data_manager.load_analysis_data(analysis_name)
            return {
                "success": True,
                "message": f"已切換到分析檔案: {analysis_name}",
                "current_analysis": data_manager.current_analysis.name
            }
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"切換分析檔案失敗: {str(e)}")
    
    @router.get("/analysis_files")
    async def get_analysis_files():
        """取得可用的分析檔案列表"""
        analysis_files = data_manager.get_analysis_files()
        return {"analysis_files": analysis_files}
    
    @router.delete("/analysis_files/{analysis_name}")
    async def delete_analysis(analysis_name: str):
        """刪除指定的分析檔案"""
        try:
            result = data_manager.delete_analysis(analysis_name)
            return result
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"刪除失敗: {str(e)}")
    
    @router.post("/merge_analysis")
    async def merge_analysis(request: Request):
        """合併多個分析檔案"""
        try:
            body = await request.json()
            merged_name = body.get("merged_name")
            source_files = body.get("source_files", [])
            
            if not merged_name:
                raise HTTPException(status_code=400, detail="請提供合併檔案名稱")
            
            result = data_manager.merge_analysis(merged_name, source_files)
            return result
            
        except HTTPException:
            raise
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"合併檔案時發生錯誤: {str(e)}")
    
    return router 