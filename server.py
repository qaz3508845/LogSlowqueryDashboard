"""
SQL 慢查詢分析 Dashboard - 主程式
"""

import os
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# 導入自定義模組
from core.data_manager import DataManager
from core.sql_analyzer import SQLAnalyzer
from api.upload import create_upload_routes
from api.analysis import create_analysis_routes
from api.queries import create_query_routes

# 建立 FastAPI 應用程式
app = FastAPI(
    title="SQL 慢查詢分析 Dashboard",
    description="MySQL 慢查詢 LOG 分析工具",
    version="2.0.0"
)

# 靜態檔案和模板設定
static_dir = Path("static")
if static_dir.exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")
else:
    print("⚠️ static 目錄不存在，跳過靜態檔案掛載")

templates_dir = Path("templates")
if templates_dir.exists():
    templates = Jinja2Templates(directory="templates")
else:
    print("⚠️ templates 目錄不存在")
    templates = None

# 初始化資料管理器
data_manager = DataManager()

# 嘗試載入預設資料
try:
    data_manager.load_analysis_data()
    print("✅ 成功載入預設分析資料")
except Exception as e:
    print(f"⚠️ 無法載入預設資料: {e}")
    # 移除自動載入邏輯，保持預設分析狀態
    # # 如果沒有預設資料，嘗試載入第一個可用的分析檔案
    # analysis_files = data_manager.get_analysis_files()
    # if analysis_files:
    #     first_analysis = analysis_files[0]["name"]
    #     try:
    #         data_manager.load_analysis_data(first_analysis)
    #         print(f"✅ 自動載入分析檔案: {first_analysis}")
    #     except Exception as load_error:
    #         print(f"⚠️ 無法載入分析檔案 {first_analysis}: {load_error}")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """主頁 - 新版介面"""
    # 移除自動載入邏輯，避免頁面載入時自動切換分析檔案
    # try:
    #     files_data = data_manager.get_analysis_files()
    #     if files_data.get("analysis_files") and data_manager.current_analysis.name == "預設分析":
    #         # 如果當前是預設分析且有可用檔案，則載入第一個檔案
    #         first_file = files_data["analysis_files"][0]["name"]
    #         data_manager.load_analysis_data(first_file)
    #         print(f"🔄 自動載入分析檔案: {first_file}")
    # except Exception as e:
    #     print(f"⚠️ 自動載入分析檔案失敗: {e}")
    
    return templates.TemplateResponse("modern_index.html", {"request": request})


@app.get("/legacy", response_class=HTMLResponse)
async def legacy_index(request: Request):
    """舊版主頁（Bootstrap版本）"""
    try:
        # 獲取分析資料
        data = data_manager.get_template_data()
        stats = data_manager.get_basic_stats()
        current_analysis = data_manager.current_analysis.name
        
        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "data": data,
                "stats": stats,
                "current_analysis": current_analysis
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"載入頁面失敗: {str(e)}")


# 註冊 API 路由
upload_router = create_upload_routes(data_manager)
analysis_router = create_analysis_routes(data_manager)
query_router = create_query_routes(data_manager)

app.include_router(upload_router)
app.include_router(analysis_router)
app.include_router(query_router)


@app.get("/health")
async def health_check():
    """健康檢查端點"""
    return {
        "status": "healthy",
        "current_analysis": data_manager.current_analysis.name,
        "total_queries": len(data_manager.current_analysis.raw_data),
        "total_templates": len(data_manager.current_analysis.summary_data)
    }


# HTMX 組件端點
@app.get("/components/summary", response_class=HTMLResponse)
async def component_summary(request: Request):
    """樣板統計組件"""
    try:
        data = data_manager.get_template_data()
        return templates.TemplateResponse(
            "components/summary.html",
            {
                "request": request,
                "data": data
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入樣板統計失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/components/queries", response_class=HTMLResponse)
async def component_queries(request: Request):
    """查詢列表組件"""
    try:
        # 這裡會實現查詢列表的載入
        return templates.TemplateResponse(
            "components/queries.html",
            {
                "request": request
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入查詢列表失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/components/analysis", response_class=HTMLResponse)
async def component_analysis(request: Request):
    """效能分析組件"""
    try:
        stats = data_manager.get_basic_stats()
        return templates.TemplateResponse(
            "components/analysis.html",
            {
                "request": request,
                "stats": stats
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入效能分析失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/components/manage", response_class=HTMLResponse)
async def component_manage(request: Request):
    """檔案管理組件"""
    try:
        return templates.TemplateResponse(
            "components/file_manager.html",
            {
                "request": request
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入檔案管理失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/api/analysis_files_list", response_class=HTMLResponse)
async def analysis_files_list(request: Request):
    """檔案列表組件"""
    try:
        files_data = data_manager.get_analysis_files()
        return templates.TemplateResponse(
            "components/files_list.html",
            {
                "request": request,
                "files": files_data.get("analysis_files", [])
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入檔案列表失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/api/analysis_files_modal", response_class=HTMLResponse)
async def analysis_files_modal(request: Request):
    """切換分析檔案模態框"""
    try:
        files_data = data_manager.get_analysis_files()
        return templates.TemplateResponse(
            "components/switch_modal.html",
            {
                "request": request,
                "files": files_data.get("analysis_files", [])
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-4 text-red-600">載入檔案選擇失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/api/summary_data", response_class=HTMLResponse)
async def summary_data(request: Request):
    """樣板統計資料表格"""
    try:
        template_data = data_manager.get_template_data()
        return templates.TemplateResponse(
            "components/summary_table.html",
            {
                "request": request,
                "data": template_data
            }
        )
    except Exception as e:
        return HTMLResponse(
            f'<div class="p-6 text-red-600">載入樣板統計失敗: {str(e)}</div>',
            status_code=500
        )


@app.get("/api/current_analysis_info", response_class=HTMLResponse)
async def current_analysis_info(request: Request):
    """獲取當前分析檔案資訊"""
    try:
        current_info = data_manager.get_current_analysis_info()
        return HTMLResponse(f"{current_info['name']}")
    except Exception as e:
        return HTMLResponse("載入失敗", status_code=500)


if __name__ == "__main__":
    # 開發模式啟動
    print("🚀 啟動 SQL 慢查詢分析 Dashboard")
    print("📊 現代化版本: http://localhost:8000/")
    print("🔗 舊版本: http://localhost:8000/legacy")
    
    uvicorn.run(
        "server:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=["LogSlowqueryDashboard"]
    )
