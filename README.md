# MySQL SlowQuery Dashboard

一個專業的 MySQL 慢查詢分析 Dashboard，提供直觀的 Web 介面來分析和監控資料庫效能。

## ✨ 主要功能

### 📊 四大分析模組
- **樣板統計**：SQL 查詢樣板分析，支援多維度篩選
- **原始查詢列表**：完整的慢查詢記錄，支援排序和搜尋
- **效能分析**：視覺化圖表統計，包含圓餅圖、長條圖等
- **檔案管理**：多檔案上傳、切換、合併功能

### 🎯 核心特色
- **多檔案管理**：支援上傳多個 LOG 檔案並切換分析
- **檔案合併功能**：可將多個分析結果合併為新的報告
- **智能篩選**：SQL 類型、執行時間、表格名稱、關鍵字等多維度篩選
- **完整排序**：所有表格支援點擊標題排序（升序/降序）
- **詳細彈窗**：點擊 SQL 查詢可查看完整內容和複製功能
- **響應式設計**：適應不同螢幕大小，支援行動裝置

### 🔍 進階功能
- **快速篩選按鈕**：> 10s, > 30s, > 1分鐘, 檢查行數 > 10萬/100萬
- **SQL 詳細資訊**：完整的執行統計和表格使用分析
- **效能評級**：自動評估 SQL 效能等級（優秀/良好/普通/較差/極差）
- **視覺化圖表**：SQL 類型分布、執行時間分布、用戶活動統計等

## 🚀 快速開始

### 環境要求
- Python 3.7+
- 現代瀏覽器（Chrome, Firefox, Safari, Edge）

### 安裝步驟

1. **克隆專案**
```bash
git clone https://github.com/yourusername/mysql-slowquery-dashboard.git
cd mysql-slowquery-dashboard
```

2. **安裝依賴**
```bash
pip install fastapi uvicorn jinja2 python-multipart
```

3. **啟動服務器**
```bash
python server.py
```

4. **開啟瀏覽器**
訪問 http://localhost:8000

## 📁 檔案結構

```
mysql-slowquery-dashboard/
├── server.py              # FastAPI 主服務器
├── templates/
│   └── index.html         # 前端 Dashboard 介面
├── parsed_slow_log.py     # LOG 檔案解析工具
├── normalized_sql_summary.py  # SQL 樣板統計工具
├── analysis_data/         # 用戶上傳的分析資料（gitignore）
├── README.md             # 專案說明
└── .gitignore           # Git 忽略檔案
```

## 💻 使用方法

### 1. 上傳 LOG 檔案
- 點擊「上傳新的 LOG 檔案」按鈕
- 選擇 MySQL 慢查詢 LOG 檔案（.log 或 .txt）
- 輸入分析檔案名稱
- 等待系統自動解析和分析

### 2. 切換分析檔案
- 點擊「切換分析檔案」按鈕
- 選擇要查看的分析檔案
- 系統會自動載入對應資料

### 3. 合併分析檔案
- 點擊「合併分析檔案」按鈕
- 選擇要合併的檔案（至少 2 個）
- 輸入新的合併檔案名稱
- 系統會產生合併報告

### 4. 篩選和分析
- 使用各種篩選條件縮小分析範圍
- 點擊表格標題進行排序
- 點擊 SQL 查詢查看詳細資訊

## 🎨 介面預覽

- **樣板統計**：展示 SQL 樣板、執行次數、平均時間、使用表格
- **原始查詢列表**：完整的慢查詢記錄，支援多維度篩選
- **效能分析**：豐富的視覺化圖表和統計資訊
- **檔案管理**：便捷的多檔案管理功能

## 🔧 技術架構

### 後端
- **FastAPI**：現代、快速的 Python Web 框架
- **Jinja2**：模板引擎
- **Python multipart**：檔案上傳支援

### 前端
- **Bootstrap 5**：響應式 UI 框架
- **jQuery**：JavaScript 函式庫
- **DataTables**：進階表格功能（排序、分頁、搜尋）
- **Chart.js**：圖表視覺化
- **Select2**：多選下拉選單
- **Font Awesome**：圖標字體

## 📊 支援的 MySQL LOG 格式

支援標準的 MySQL 慢查詢 LOG 格式，包含以下欄位：
- Time（時間戳）
- User@Host（用戶和主機）
- Query_time（查詢時間）
- Lock_time（鎖定時間）
- Rows_sent（回傳行數）
- Rows_examined（檢查行數）
- SQL 查詢語句