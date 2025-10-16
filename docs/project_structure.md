# 東南旅遊機票資料爬蟲系統 - 專案結構

## 專案概述

本文件描述東南旅遊機票資料爬蟲系統的專案結構，基於類別圖和序列圖設計。此專案旨在從東南旅遊網站的GraphQL API抓取機票資料，並將資料存儲到Cloud Storage和BigQuery中。

## 目錄結構

```
southeast_travel_crawler/
│
├── main.py                      # 主程式入口點
├── .env                         # 環境變量
├── requirements.txt             # 專案依賴
├── README.md                    # 專案說明文件
│
├── config/                      # 配置文件目錄
│   ├── __init__.py
│   ├── config.yaml              # 主要配置文件
│   └── config_manager.py        # 配置管理器類
│
├── controllers/                 # 控制器目錄
│   ├── __init__.py
│   ├── crawler_controller.py    # 爬蟲控制器
│   ├── api_client.py            # API客戶端
│   └── task_manager.py          # 任務管理器
│
├── models/                      # 數據模型目錄
│   ├── __init__.py
│   ├── flight_info.py           # 航班信息類
│   ├── flight_segment.py        # 航班段類
│   └── crawl_task.py            # 爬蟲任務類
│
├── parsers/                     # 解析器目錄
│   ├── __init__.py
│   └── json_parser.py           # JSON解析器
│
├── processors/                  # 處理器目錄
│   ├── __init__.py
│   └── data_processor.py        # 數據處理器
│
├── storage/                     # 存儲管理目錄
│   ├── __init__.py
│   └── storage_manager.py       # 存儲管理器
│
└── utils/                       # 工具類目錄
    ├── __init__.py
    ├── log_manager.py           # 日誌管理器
    └── debug_tools.py           # 除錯工具
```
