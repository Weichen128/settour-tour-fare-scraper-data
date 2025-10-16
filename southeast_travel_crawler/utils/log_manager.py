#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日誌管理器模組

此模組提供日誌管理功能，用於記錄系統中的關鍵操作和錯誤信息。
"""

import logging
from datetime import datetime
import os

class LogManager:
    """
    日誌管理器類別
    
    負責管理系統日誌，記錄關鍵操作、錯誤信息和任務狀態。
    """
    _instance = None  # 用來儲存唯一實例

    def __new__(cls, *args, **kwargs):
        """
        實現單例模式

        確保LogManager類別只有一個實例，並提供唯一的日誌管理器實例。

        參數:
            *args: 可變參數
            **kwargs: 關鍵字參數

        返回:
            LogManager實例
        """
        if cls._instance is None:
            cls._instance = super(LogManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, config_manager):
        """
        初始化日誌管理器

        創建一個新的日誌管理器實例，設置日誌級別和輸出文件。
        
        參數:
            config_manager (ConfigManager): 配置管理器實例
        """

        self.config_manager = config_manager
        self.log_level = self.config_manager.config["logging"]["level"]
        self.log_file = os.path.join(os.getcwd(), self.config_manager.config["logging"]["file_path"])

        self.logger = logging.getLogger('southeast_travel_crawler')
        self.logger.setLevel(self.log_level)
        
        # 創建格式化器
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 創建控制台處理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
        
        # 如果提供了日誌文件，則創建文件處理器
        if self.log_file:
            file_handler = logging.FileHandler(self.log_file, encoding='utf-8')
            file_handler.setLevel(self.log_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def log_info(self, message):
        """
        記錄信息
        
        記錄一般信息到日誌。
        
        參數:
            message (str): 要記錄的信息
        """
        self.logger.info(message)
    
    def log_debug(self, message):
        """
        記錄調試信息
        
        記錄調試信息到日誌。
        """
        self.logger.debug(message)

    def log_error(self, message, exception=None):
        """
        記錄錯誤
        
        記錄錯誤信息和異常到日誌。
        
        參數:
            message (str): 錯誤描述
            exception (Exception): 捕獲的異常
        """
        if exception:
            error_details = f"{message}: {str(exception)}"
            exc_info = True
        else:
            error_details = message
            exc_info = False
        self.logger.error(error_details, exc_info=exc_info)

    def log_warning(self, message):
        """
        記錄警告信息
        
        記錄警告信息到日誌。

        參數:
            message (str): 警告描述
        """
        self.logger.warning(message)

    def log_task_status(self, task_id, status):
        """
        記錄任務狀態
        
        記錄爬蟲任務的狀態變更。
        
        參數:
            task_id (str): 任務ID
            status (str): 任務狀態，如'pending'、'running'、'completed'、'failed'
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_message = f"任務 {task_id} 狀態變更為 {status} 於 {timestamp}"
        self.logger.info(status_message)
