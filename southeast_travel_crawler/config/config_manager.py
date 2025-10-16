#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置管理器模組

此模組提供配置管理功能，用於管理系統配置，如瀏覽器設置、重試策略、存儲配置等。
"""

import os
import yaml


class ConfigManager:
    """
    配置管理器類別
    
    負責管理系統配置，包括瀏覽器設置、重試策略、存儲配置等。
    
    屬性:
        config (dict): 配置字典，包含所有系統配置項
    """

    def __init__(self):
        """
        初始化配置管理器
        
        初始化空配置字典。
        """
        self.config = {}
        self.config_file = None

    def load_config(self, config_file):
        """
        加載配置文件
        
        從指定的YAML文件加載配置。
        
        參數:
            config_file (str): 配置文件路徑
            
        返回:
            bool: 加載成功返回True，否則返回False
            
        異常:
            FileNotFoundError: 如果配置文件不存在
            yaml.YAMLError: 如果配置文件格式錯誤
        """
        try:
            if not os.path.exists(config_file):
                raise FileNotFoundError(f"配置文件不存在: {config_file}")
                
            with open(config_file, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
                self.config_file = config_file
            return True
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise e

    def get_api_config(self):
        """
        獲取API配置
        
        返回API相關配置，包括API URL、API Key等。
        
        返回:
            dict: API配置字典
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")
            
        return self.config.get('api', {})

    def get_retry_config(self):
        """
        獲取重試配置
        
        返回重試策略配置，包括最大重試次數、重試間隔等。
        
        返回:
            dict: 重試配置字典
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")
            
        return self.config.get('retry', {})

    def get_storage_config(self):
        """
        獲取存儲配置
        
        返回存儲相關配置，包括Cloud Storage和BigQuery的配置。
        
        返回:
            dict: 存儲配置字典
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")
            
        return self.config.get('storage', {})
        
    def get_log_config(self):
        """
        獲取日誌配置
        
        返回日誌相關配置，包括日誌級別、日誌文件路徑等。
        
        返回:
            dict: 日誌配置字典
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")
            
        return self.config.get('logging', {})

    def get_website_config(self):
        """
        獲取網站配置
        
        返回網站相關配置，包括航班搜索頁面URL等。

        返回:
            dict: 網站配置字典
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")
            
        return self.config.get('website', {})
    
    def get_flight_tasks_fixed_month(self) -> list:
        """
        獲取固定月份日期爬蟲任務

        返回:
            list: 固定月份日期爬蟲任務列表
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")

        return self.config.get('flight_tasks_fixed_month', [])
    
    def get_flight_tasks_holidays(self) -> list:
        """
        獲取節日爬蟲任務

        返回:
            list: 節日爬蟲任務列表
        """
        if not self.config:
            raise ValueError("配置尚未加載，請先呼叫load_config方法")

        return self.config.get('flight_tasks_holidays', [])
