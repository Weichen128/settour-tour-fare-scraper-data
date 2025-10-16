"""
儲存管理器模組 - 負責處理數據儲存到 Cloud Storage 和 BigQuery
"""

import json
import os
from typing import Dict, List, Any
import pandas as pd

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

class StorageManager:
    """管理數據儲存到 Cloud Storage 和 BigQuery 的類"""
    
    def __init__(self, config_manager=None, log_manager=None):
        """
        初始化儲存管理器
        
        Args:
            config_manager: 配置管理器實例，用於獲取儲存配置。如果為 None，將嘗試從環境變數獲取配置。
            log_manager: 日誌管理器實例，用於記錄操作和錯誤。
        """
        self.log_manager = log_manager
        self.config_manager = config_manager
        self.storage_config = self._get_storage_config()
        
        # 初始化 Cloud Storage 客戶端
        self.storage_client = storage.Client() if self._check_gcp_env() else None
            
        # 初始化 BigQuery 客戶端
        self.bq_client = bigquery.Client() if self._check_gcp_env() else None

        if self.storage_client is None or self.bq_client is None:
            raise ValueError("未設定儲存配置，請檢查 config 是否包含 project_id")
    
    def _check_gcp_env(self) -> bool:
        """
        檢查是否在 Google Cloud 環境中運行
        
        Returns:
            bool: 如果在 GCP 環境中返回 True，否則返回 False
        """
        # 檢查是否有 GCP 專案設定
        return (
            self.storage_config is not None
            and self.storage_config.get('bigquery') is not None 
            and self.storage_config.get('bigquery').get('project_id') is not None
        )
    
    def _get_storage_config(self) -> Dict[str, Any]:
        """
        獲取儲存配置，優先從配置管理器獲取，如果不可用則返回預設配置
        
        Returns:
            Dict[str, Any]: 儲存配置字典
        """
        if self.config_manager and hasattr(self.config_manager, 'get_storage_config'):
            return self.config_manager.get_storage_config()
        else:
            raise ValueError("未設定儲存配置")
    
    def save_to_cloud_storage(self, json_data: str, filename: str) -> bool:
        """
        保存 JSON 數據到 Cloud Storage
        
        Args:
            json_data: 要儲存的 JSON 格式數據（字串）
            filename: 儲存的檔案名稱
            
        Returns:
            tuple: 操作成功返回 True，失敗返回 False
        """
        # 如果不在 GCP 環境或無法使用 Cloud Storage，儲存到本地
        if not self.storage_client:
            return self._save_to_local(json_data, filename)
            
        try:
            bucket_name = self.storage_config.get('cloud_storage').get('bucket_name')

            self.log_manager.log_info(f"嘗試將數據保存到 Cloud Storage 儲存桶: {bucket_name}")
            
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_string(json_data, content_type='application/json')
            
            self.log_manager.log_info(f"成功將數據儲存至 gs://{bucket_name}/{filename}")
            return True, None
        except GoogleCloudError as e:
            self.log_manager.log_error(f"Cloud Storage 操作錯誤", e)
            return False, e
        except Exception as e:
            import traceback
            self.log_manager.log_error(f"儲存到 Cloud Storage 時發生錯誤，堆疊: {traceback.format_exc()}", e)
            # 嘗試退回到本地儲存
            return self._save_to_local(json_data, filename), traceback.format_exc()
    
    def _save_to_local(self, json_data: str, filename: str) -> bool:
        """
        將數據儲存到本地檔案系統（作為備份方案）
        
        Args:
            json_data: 要儲存的 JSON 格式數據（字串）
            filename: 儲存的檔案名稱
            
        Returns:
            bool: 操作成功返回 True，失敗返回 False
        """
        try:
            local_path = self.storage_config.get('local_storage_path', './data')
            os.makedirs(local_path, exist_ok=True)
            
            file_path = os.path.join(local_path, filename)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_data)
                
            self.log_manager.log_info(f"成功將數據儲存至本地路徑: {file_path}")
            return True
        except Exception as e:
            import traceback
            self.log_manager.log_error(f"儲存到本地時發生錯誤，堆疊: {traceback.format_exc()}", e)
            return False
    
    def save_to_bigquery(self, table_data: pd.DataFrame) -> bool:
        """
        保存表格數據到 BigQuery
        
        Args:
            table_data: DataFrame 要儲存的表格數據
            
        Returns:
            tuple: 操作成功返回 True，失敗返回 False
        """
        try:
            dataset_id = self.storage_config.get('bigquery').get('dataset_id')
            project_id = self.storage_config.get('bigquery').get('project_id')
            table_id = self.storage_config.get('bigquery').get('table_id')
            table_ref = f"{project_id}.{dataset_id}.{table_id}"
            
            self.log_manager.log_info(f"嘗試將數據保存到 BigQuery 表格: {table_ref}")
            
            # 將數據轉換為字典列表用於 BigQuery 載入
            table_data.to_gbq(table_ref, project_id=project_id, if_exists='append')
            
            
            self.log_manager.log_info(f"成功將數據儲存至 BigQuery 表格: {table_ref}")
            return True, None
        except GoogleCloudError as e:
            self.log_manager.log_error(f"BigQuery 操作錯誤", e)
            return False, e
        except Exception as e:
            import traceback
            self.log_manager.log_error(f"儲存到 BigQuery 時發生錯誤，堆疊: {traceback.format_exc()}", e)
            # 嘗試退回到本地儲存
            # 將 DataFrame 轉換為 JSON 字串
            json_data = table_data.to_json(orient='records', force_ascii=False)
            return self._save_to_local(json_data, f"{table_id}.json"), traceback.format_exc()
    
    def get_storage_config(self) -> Dict[str, Any]:
        """
        獲取儲存配置
        
        Returns:
            Dict[str, Any]: 儲存配置字典
        """
        return self.storage_config

    def save_binary_to_cloud_storage(self, binary_data: bytes, filename: str, content_type: str = None) -> bool:
        """
        保存二進制數據到 Cloud Storage
        
        Args:
            binary_data: 要儲存的二進制數據
            filename: 儲存的檔案名稱
            content_type: 文件的 MIME 類型，可選（例如 'image/png'）
            
        Returns:
            tuple: 操作成功返回 (True, None)，失敗返回 (False, error)
        """
        # 如果不在 GCP 環境或無法使用 Cloud Storage，儲存到本地
        if not self.storage_client:
            return self._save_binary_to_local(binary_data, filename)
            
        try:
            bucket_name = self.storage_config.get('cloud_storage').get('bucket_name')

            self.log_manager.log_info(f"嘗試將二進制數據保存到 Cloud Storage 儲存桶: {bucket_name}")
            
            bucket = self.storage_client.bucket(bucket_name)
            blob = bucket.blob(filename)
            blob.upload_from_string(binary_data, content_type=content_type)
            
            self.log_manager.log_info(f"成功將二進制數據儲存至 gs://{bucket_name}/{filename}")
            return True, None
        except GoogleCloudError as e:
            self.log_manager.log_error(f"Cloud Storage 操作錯誤", e)
            return False, e
        except Exception as e:
            import traceback
            self.log_manager.log_error(f"儲存二進制數據到 Cloud Storage 時發生錯誤，堆疊: {traceback.format_exc()}", e)
            raise e
