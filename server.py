from fastapi import FastAPI, Request, Query, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import json
import os
import shutil
import re
from collections import defaultdict, Counter
from datetime import datetime
import statistics
from pathlib import Path
import uuid

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# 設定資料目錄
DATA_DIR = Path("analysis_data")
DATA_DIR.mkdir(exist_ok=True)

# 全域變數儲存當前選擇的分析檔案
current_analysis = {
    "name": "預設分析",
    "summary_data": [],
    "template_to_raw_dict": {},
    "raw_data": []
}

def load_analysis_data(analysis_name="預設分析"):
    """載入指定的分析資料"""
    global current_analysis
    
    if analysis_name == "預設分析":
        # 載入預設資料
        try:
            with open("normalized_sql_summary.json", "r", encoding="utf-8") as f:
                summary_data = json.load(f)
            with open("parsed_slow_log.json", "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            summary_data = []
            raw_data = []
    else:
        # 載入指定分析檔案
        analysis_path = DATA_DIR / analysis_name
        try:
            with open(analysis_path / "summary.json", "r", encoding="utf-8") as f:
                summary_data = json.load(f)
            with open(analysis_path / "raw_data.json", "r", encoding="utf-8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="分析檔案不存在")
    
    # 建立樣板對應關係
    template_to_raw = defaultdict(list)
    for item in raw_data:
        if item.get("sql"):
            normalized = normalize_sql(item["sql"])
            template_to_raw[normalized].append({
                "original_sql": item["sql"],
                "query_time": item.get("query_time", 0),
                "time": item.get("time", ""),
                "user": item.get("user", ""),
                "host": item.get("host", ""),
                "rows_examined": item.get("rows_examined", 0),
                "rows_sent": item.get("rows_sent", 0),
                "lock_time": item.get("lock_time", 0),
                "timestamp": item.get("timestamp", 0),
                "thread_id": item.get("thread_id", 0),
                "schema": item.get("schema", ""),
                "tables_used": item.get("tables_used", [])
            })
    
    current_analysis = {
        "name": analysis_name,
        "summary_data": summary_data,
        "template_to_raw_dict": dict(template_to_raw),
        "raw_data": raw_data
    }

def normalize_sql(sql: str) -> str:
    """SQL樣板轉換函式"""
    sql = sql.lower()
    sql = re.sub(r"\s+", " ", sql)
    sql = re.sub(r"'[^']*'", "?", sql)
    sql = re.sub(r'"[^"]*"', "?", sql)
    sql = re.sub(r"\b\d+\.\d+\b", "?", sql)
    sql = re.sub(r"\b\d+\b", "?", sql)
    sql = re.sub(r"\(\s*(\?,\s*)*\?\s*\)", "(?)", sql)
    return sql.strip()

def get_sql_type(sql: str) -> str:
    """SQL類型判斷"""
    match = re.match(r"^\s*(\w+)", sql.lower())
    if not match:
        return "OTHER"
    keyword = match.group(1).upper()
    if keyword in {"SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE", "CALL"}:
        return keyword
    return "OTHER"

def parse_slow_log(content: str) -> list:
    """解析慢查詢LOG內容"""
    entries = re.split(r"(?=^# Time: )", content, flags=re.MULTILINE)
    parsed_entries = []
    
    table_pattern = re.compile(r"(?:from|join)\s+`?(\w+)`?(?:\s+as|\s+\w+)?", re.IGNORECASE)
    
    for entry in entries:
        if not entry.strip():
            continue
            
        parsed = {
            "time": None, "user": None, "host": None, "thread_id": None,
            "schema": None, "qc_hit": None, "query_time": None, "lock_time": None,
            "rows_sent": None, "rows_examined": None, "rows_affected": None,
            "bytes_sent": None, "timestamp": None, "sql": None, "tables_used": []
        }
        
        # 解析各個欄位
        if m := re.search(r"# Time: (.+)", entry):
            parsed["time"] = m.group(1).strip()
        
        if m := re.search(r"# User@Host: (.+)\[(.+)\] @  \[(.*)\]", entry):
            parsed["user"] = m.group(2).strip()
            parsed["host"] = m.group(3).strip()
        
        if m := re.search(r"# Thread_id: (\d+)\s+Schema: (\w+)\s+QC_hit: (\w+)", entry):
            parsed["thread_id"] = int(m.group(1))
            parsed["schema"] = m.group(2)
            parsed["qc_hit"] = m.group(3)
        
        if m := re.search(r"# Query_time: ([\d.]+)\s+Lock_time: ([\d.]+)\s+Rows_sent: (\d+)\s+Rows_examined: (\d+)", entry):
            parsed["query_time"] = float(m.group(1))
            parsed["lock_time"] = float(m.group(2))
            parsed["rows_sent"] = int(m.group(3))
            parsed["rows_examined"] = int(m.group(4))
        
        if m := re.search(r"# Rows_affected: (\d+)\s+Bytes_sent: (\d+)", entry):
            parsed["rows_affected"] = int(m.group(1))
            parsed["bytes_sent"] = int(m.group(2))
        
        if m := re.search(r"SET timestamp=(\d+);", entry):
            parsed["timestamp"] = int(m.group(1))
        
        if m := re.search(r"SET timestamp=\d+;\n(.+)", entry, re.DOTALL):
            sql = m.group(1).strip()
            parsed["sql"] = sql
            tables = table_pattern.findall(sql)
            parsed["tables_used"] = sorted(set(tables))
        
        parsed_entries.append(parsed)
    
    return parsed_entries

def create_summary_data(raw_data: list) -> list:
    """建立統計摘要資料"""
    sql_groups = defaultdict(list)
    
    for item in raw_data:
        sql = item.get("sql")
        if sql:
            norm_sql = normalize_sql(sql)
            sql_groups[norm_sql].append(item)
    
    summary = []
    for norm_sql, entries in sql_groups.items():
        count = len(entries)
        total_time = sum(e["query_time"] for e in entries if e.get("query_time") is not None)
        avg_time = total_time / count if count else 0
        sql_type = get_sql_type(norm_sql)
        
        # 收集所有涉及的表格
        all_tables = set()
        for entry in entries:
            all_tables.update(entry.get("tables_used", []))
        
        summary.append({
            "template": norm_sql,
            "type": sql_type,
            "count": count,
            "avg_query_time": round(avg_time, 4),
            "tables_used": sorted(list(all_tables))
        })
    
    return summary

# 初始化載入預設資料
try:
    load_analysis_data()
except Exception as e:
    print(f"⚠️ 無法載入預設資料: {e}")

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    # 確保資料存在
    if not current_analysis["summary_data"] or len(current_analysis["raw_data"]) == 0:
        # 如果沒有資料，嘗試載入第一個可用的分析檔案
        if DATA_DIR.exists():
            for item in DATA_DIR.iterdir():
                if item.is_dir() and (item / "summary.json").exists():
                    try:
                        load_analysis_data(item.name)
                        print(f"自動載入分析檔案: {item.name}")
                        break
                    except:
                        continue
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "data": current_analysis["summary_data"],
        "template_to_raw": current_analysis["template_to_raw_dict"],
        "current_analysis": current_analysis["name"]
    })

@app.post("/api/upload_log")
async def upload_log(
    file: UploadFile = File(...),
    analysis_name: str = Form(...)
):
    """上傳並分析LOG檔案"""
    
    # 檢查檔案類型
    if not file.filename or not file.filename.endswith(('.log', '.txt')):
        raise HTTPException(status_code=400, detail="只支援 .log 或 .txt 檔案")
    
    # 建立分析目錄
    analysis_path = DATA_DIR / analysis_name
    analysis_path.mkdir(exist_ok=True)
    
    try:
        # 儲存原始檔案
        log_file_path = analysis_path / f"original_{file.filename}"
        with open(log_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        # 讀取並解析LOG內容
        with open(log_file_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        
        # 解析LOG
        raw_data = parse_slow_log(content)
        
        # 建立統計摘要
        summary_data = create_summary_data(raw_data)
        
        # 儲存分析結果
        with open(analysis_path / "raw_data.json", "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
        
        with open(analysis_path / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary_data, f, ensure_ascii=False, indent=2)
        
        # 儲存元資料
        metadata = {
            "original_filename": file.filename,
            "upload_time": datetime.now().isoformat(),
            "total_queries": len(raw_data),
            "total_templates": len(summary_data)
        }
        with open(analysis_path / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return {
            "success": True,
            "message": f"LOG檔案 '{file.filename}' 上傳並分析完成",
            "analysis_name": analysis_name,
            "total_queries": len(raw_data),
            "total_templates": len(summary_data)
        }
        
    except Exception as e:
        # 清理失敗的目錄
        if analysis_path.exists():
            shutil.rmtree(analysis_path)
        raise HTTPException(status_code=500, detail=f"處理檔案時發生錯誤: {str(e)}")

@app.post("/api/switch_analysis/{analysis_name}")
async def switch_analysis(analysis_name: str):
    """切換到指定的分析檔案"""
    try:
        load_analysis_data(analysis_name)
        return {
            "success": True,
            "message": f"已切換到分析檔案: {analysis_name}",
            "current_analysis": current_analysis["name"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"切換分析檔案失敗: {str(e)}")

@app.get("/api/analysis_files")
async def get_analysis_files():
    """取得可用的分析檔案列表"""
    analysis_files = []
    
    # 檢查是否有真正的預設分析檔案（parsed_slow_log.json 和 normalized_sql_summary.json）
    has_default_files = (
        os.path.exists("parsed_slow_log.json") and 
        os.path.exists("normalized_sql_summary.json")
    )
    
    # 只有當預設分析檔案存在且有資料時才顯示
    if has_default_files and current_analysis["name"] == "預設分析" and len(current_analysis["raw_data"]) > 0:
        analysis_files.append({
            "name": "預設分析",
            "is_current": True,
            "metadata": {
                "total_queries": len(current_analysis["raw_data"]),
                "total_templates": len(current_analysis["summary_data"]),
                "upload_time": "內建資料"
            }
        })
    
    # 用戶上傳的分析檔案
    if DATA_DIR.exists():
        for item in DATA_DIR.iterdir():
            if item.is_dir() and (item / "summary.json").exists():
                try:
                    with open(item / "metadata.json", "r", encoding="utf-8") as f:
                        metadata = json.load(f)
                except:
                    metadata = {"upload_time": "未知", "total_queries": 0, "total_templates": 0}
                
                analysis_files.append({
                    "name": item.name,
                    "is_current": current_analysis["name"] == item.name,
                    "metadata": metadata
                })
    
    return {"analysis_files": analysis_files}

@app.delete("/api/analysis_files/{analysis_name}")
async def delete_analysis(analysis_name: str):
    """刪除指定的分析檔案"""
    if analysis_name == "預設分析":
        raise HTTPException(status_code=400, detail="無法刪除預設分析")
    
    analysis_path = DATA_DIR / analysis_name
    if not analysis_path.exists():
        raise HTTPException(status_code=404, detail="分析檔案不存在")
    
    try:
        shutil.rmtree(analysis_path)
        
        # 如果刪除的是當前分析，切換回預設
        if current_analysis["name"] == analysis_name:
            load_analysis_data("預設分析")
        
        return {
            "success": True,
            "message": f"分析檔案 '{analysis_name}' 已刪除",
            "current_analysis": current_analysis["name"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"刪除失敗: {str(e)}")

@app.post("/api/merge_analysis")
async def merge_analysis(request: Request):
    """合併多個分析檔案"""
    try:
        body = await request.json()
        merged_name = body.get("merged_name")
        source_files = body.get("source_files", [])
        
        if not merged_name:
            raise HTTPException(status_code=400, detail="請提供合併檔案名稱")
        
        if len(source_files) < 2:
            raise HTTPException(status_code=400, detail="至少需要選擇 2 個檔案進行合併")
        
        # 檢查合併檔案名稱是否已存在
        merged_path = DATA_DIR / merged_name
        if merged_path.exists():
            raise HTTPException(status_code=400, detail="合併檔案名稱已存在，請選擇其他名稱")
        
        # 收集所有原始資料
        all_raw_data = []
        source_metadata = []
        
        for source_name in source_files:
            if source_name == "預設分析":
                # 如果是預設分析，使用當前載入的資料
                if current_analysis["name"] == "預設分析":
                    all_raw_data.extend(current_analysis["raw_data"])
                    source_metadata.append({
                        "name": "預設分析",
                        "queries": len(current_analysis["raw_data"]),
                        "templates": len(current_analysis["summary_data"])
                    })
                continue
            
            # 載入分析檔案
            source_path = DATA_DIR / source_name
            if not source_path.exists() or not (source_path / "raw_data.json").exists():
                continue
            
            try:
                with open(source_path / "raw_data.json", "r", encoding="utf-8") as f:
                    source_raw_data = json.load(f)
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
            raise HTTPException(status_code=400, detail="無法載入任何有效的分析資料")
        
        # 建立合併統計摘要
        merged_summary = create_summary_data(all_raw_data)
        
        # 建立合併目錄
        merged_path.mkdir(exist_ok=True)
        
        # 儲存合併後的原始資料
        with open(merged_path / "raw_data.json", "w", encoding="utf-8") as f:
            json.dump(all_raw_data, f, ensure_ascii=False, indent=2)
        
        # 儲存合併後的統計摘要
        with open(merged_path / "summary.json", "w", encoding="utf-8") as f:
            json.dump(merged_summary, f, ensure_ascii=False, indent=2)
        
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
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"合併檔案時發生錯誤: {str(e)}")

@app.get("/api/raw_sqls/{template_index}")
async def get_raw_sqls(template_index: int):
    """取得指定樣板的原始SQL列表"""
    if 0 <= template_index < len(current_analysis["summary_data"]):
        template = current_analysis["summary_data"][template_index]["template"]
        raw_sqls = current_analysis["template_to_raw_dict"].get(template, [])
        return {"raw_sqls": raw_sqls}
    return {"error": "模板索引無效"}

@app.get("/api/raw_queries")
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
    for item in current_analysis["raw_data"]:
        if not item.get("sql"):
            continue
            
        # 查詢時間篩選
        if item.get("query_time", 0) < min_time:
            continue
            
        # SQL類型篩選
        if sql_type:
            if get_sql_type(item["sql"]) != sql_type:
                continue
                
        # 用戶篩選
        if user_filter and user_filter.lower() not in (item.get("user", "").lower()):
            continue
            
        # 表格篩選
        if table_filter:
            item_tables = item.get("tables_used", [])
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
        if search and search.lower() not in item.get("sql", "").lower():
            continue
            
        filtered_data.append(item)
    
    # 排序（依查詢時間降序）
    filtered_data.sort(key=lambda x: x.get("query_time", 0), reverse=True)
    
    # 分頁
    total = len(filtered_data)
    start = (page - 1) * size
    end = start + size
    page_data = filtered_data[start:end]
    
    # 格式化資料
    formatted_data = []
    for item in page_data:
        formatted_data.append({
            "sql": item["sql"],
            "query_time": item.get("query_time", 0),
            "lock_time": item.get("lock_time", 0),
            "rows_examined": item.get("rows_examined", 0),
            "rows_sent": item.get("rows_sent", 0),
            "user": item.get("user", ""),
            "host": item.get("host", ""),
            "time": item.get("time", ""),
            "schema": item.get("schema", ""),
            "thread_id": item.get("thread_id", 0),
            "sql_type": get_sql_type(item["sql"]),
            "tables_used": item.get("tables_used", [])
        })
    
    return {
        "data": formatted_data,
        "total": total,
        "page": page,
        "size": size,
        "total_pages": (total + size - 1) // size
    }

@app.get("/api/performance_stats")
async def get_performance_stats():
    """取得效能分析統計資料"""
    
    # 基本統計
    query_times = [item.get("query_time", 0) for item in current_analysis["raw_data"] if item.get("query_time")]
    
    if not query_times:
        return {"error": "無查詢時間資料"}
    
    # SQL類型統計
    type_stats = Counter()
    time_by_type = defaultdict(list)
    
    for item in current_analysis["raw_data"]:
        if item.get("sql"):
            sql_type = get_sql_type(item["sql"])
            type_stats[sql_type] += 1
            if item.get("query_time"):
                time_by_type[sql_type].append(item["query_time"])
    
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
    for item in current_analysis["raw_data"]:
        if item.get("user"):
            user_stats[item["user"]] += 1
    
    # 表格使用統計
    table_stats = Counter()
    for item in current_analysis["raw_data"]:
        for table in item.get("tables_used", []):
            table_stats[table] += 1
    
    return {
        "basic_stats": {
            "total_queries": len(current_analysis["raw_data"]),
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

if __name__ == "__main__":
    import uvicorn
    print("🚀 啟動 SQL 慢查詢分析 Dashboard...")
    print("📊 服務器地址: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
