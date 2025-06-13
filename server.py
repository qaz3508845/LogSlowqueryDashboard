"""
SQL 慢查詢分析 Dashboard - 主程式
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

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
    # 如果沒有預設資料，嘗試載入第一個可用的分析檔案
    analysis_files = data_manager.get_analysis_files()
    if analysis_files:
        first_analysis = analysis_files[0]["name"]
        try:
            data_manager.load_analysis_data(first_analysis)
            print(f"✅ 自動載入分析檔案: {first_analysis}")
        except Exception as load_error:
            print(f"⚠️ 無法載入分析檔案 {first_analysis}: {load_error}")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """主頁面路由"""
    if templates is None:
        return HTMLResponse("<h1>模板目錄不存在，請檢查 templates 目錄</h1>")
    
    # 確保資料存在
    if not data_manager.current_analysis.summary_data and len(data_manager.current_analysis.raw_data) == 0:
        # 如果當前沒有資料，嘗試載入第一個可用的分析檔案
        analysis_files = data_manager.get_analysis_files()
        if analysis_files:
            try:
                first_analysis = analysis_files[0]["name"]
                data_manager.load_analysis_data(first_analysis)
                print(f"🔄 自動載入分析檔案: {first_analysis}")
            except Exception as e:
                print(f"⚠️ 自動載入失敗: {e}")
    
    # 轉換資料為模板需要的格式
    summary_data_dict = []
    for item in data_manager.current_analysis.summary_data:
        summary_data_dict.append({
            "template": item.template,
            "type": item.type,
            "count": item.count,
            "avg_query_time": item.avg_query_time,
            "tables_used": item.tables_used
        })
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "data": summary_data_dict,
        "template_to_raw": data_manager.current_analysis.template_to_raw_dict,
        "current_analysis": data_manager.current_analysis.name
    })


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


if __name__ == "__main__":
    import uvicorn
    print("🚀 啟動 SQL 慢查詢分析 Dashboard v2.0...")
    print("📊 服務器地址: http://localhost:8000")
    print("🔧 模組化架構已啟用")
    uvicorn.run(app, host="0.0.0.0", port=8000)
