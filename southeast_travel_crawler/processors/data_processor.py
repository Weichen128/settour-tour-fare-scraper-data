"""
數據處理器模組 - 負責處理、轉換和驗證來自網頁解析器的資料
"""

import json
from datetime import datetime
from typing import List, Optional
import pandas as pd
import time

from models import FlightInfo
from storage.storage_manager import StorageManager
from utils.log_manager import LogManager

class DataProcessor:
    """處理爬取的原始數據，轉換為標準格式並準備儲存"""
    
    def __init__(self,
                 storage_manager: Optional[StorageManager] = None,
                 log_manager: Optional[LogManager] = None):
        """
        初始化數據處理器
        
        Args:
            storage_manager: 儲存管理器實例，用於數據儲存操作
            log_manager: 日誌管理器實例，用於記錄操作和錯誤
        """
        self.log_manager = log_manager
        self.storage_manager = storage_manager
        self.raw_data = None
        self.processed_data = None
        self.json_data = None
        self.table_data = None
    
    def process_data(self, raw_data: List[FlightInfo]) -> List[FlightInfo]:
        """
        處理從網頁解析器獲取的原始數據，轉換為 FlightInfo 對象
        
        Args:
            raw_data: 網頁解析器提取的 FlightInfo 對象列表
            
        Returns:
            包含處理後的 FlightInfo 對象的列表
        """
        self.log_manager.log_info(f"開始處理 {len(raw_data)} 筆航班資料")
        self.raw_data = raw_data
        self.processed_data = []
        
        for flight_data in self.raw_data: 
            if self.validate_data(flight_data):
                self.processed_data.append(flight_data)
            else:
                self.log_manager.log_warning(f"航班資料驗證失敗，已跳過")
        
        self.log_manager.log_info(f"成功處理 {len(self.processed_data)} 筆航班資料")
        return self.processed_data
    
    def convert_to_json(self) -> str:
        """
        將處理後的數據轉換為 JSON 字符串
        
        Returns:
            包含所有航班信息的 JSON 字符串
        """
        if not self.processed_data:
            self.log_manager.log_warning("嘗試轉換空數據為JSON，返回空陣列")
            return "[]"
        
        flight_dicts = [flight.to_dict() for flight in self.processed_data]
        self.json_data = json.dumps(flight_dicts, ensure_ascii=False, indent=2)
        return self.json_data
    
    def convert_to_table(self) -> pd.DataFrame:
        """
        將處理後的數據轉換為適合儲存到資料庫表的格式
        
        Returns:
            DataFrame: 表格格式的數據列表
        """
        if not self.processed_data:
            self.log_manager.log_error("嘗試轉換空數據為表格格式", Exception("嘗試轉換空數據為表格格式"))
            raise ValueError("嘗試轉換空數據為表格格式")
            
        table_data = []
        current_timestamp = time.time()
        
        for flight in self.processed_data:
            row = {
                # 基本信息
                "去程日期": flight.departure_date.strftime("%Y-%m-%d") if flight.departure_date else None,
                "回程日期": flight.return_date.strftime("%Y-%m-%d") if flight.return_date else None,
                "票面價格": int(flight.price) if flight.price else None,
                "稅金": int(flight.tax) if flight.tax else None,
                "crawl_time": current_timestamp,
            }
            
            # 處理去程航段 (最多3個航段)
            for i in range(min(3, len(flight.outbound_segments))):
                segment = flight.outbound_segments[i]
                segment_num = i + 1
                
                row[f"去程航班編號{segment_num}"] = segment.flight_number
                row[f"去程艙等{segment_num}"] = segment.cabin_class
            
            # 處理回程航段 (最多3個航段)
            for i in range(min(3, len(flight.inbound_segments))):
                segment = flight.inbound_segments[i]
                segment_num = i + 1
                
                row[f"回程航班編號{segment_num}"] = segment.flight_number
                row[f"回程艙等{segment_num}"] = segment.cabin_class
            
            # 確保所有航班編號和艙等欄位都存在
            # 去程航段 2-3
            for segment_num in range(len(flight.outbound_segments) + 1, 4):
                row[f"去程航班編號{segment_num}"] = None
                row[f"去程艙等{segment_num}"] = None
            
            # 回程航段 2-3
            for segment_num in range(len(flight.inbound_segments) + 1, 4):
                row[f"回程航班編號{segment_num}"] = None
                row[f"回程艙等{segment_num}"] = None
            
            table_data.append(row)
        
        # 轉換為pandas DataFrame
        self.table_data = pd.DataFrame(table_data)
        return self.table_data
    
    def validate_data(self, flight_info: FlightInfo) -> bool:
        """
        驗證航班信息的完整性和有效性
        
        Args:
            flight_info: 要驗證的 FlightInfo 對象
            
        Returns:
            如果數據有效則返回 True，否則返回 False
        """
        # 檢查必要字段
        if not flight_info.outbound_segments and not flight_info.inbound_segments:
            self.log_manager.log_warning("缺少航班編號")
            return False
        
        # 檢查價格
        if flight_info.price <= 0:
            self.log_manager.log_warning("航班價格無效")
            return False
        
        # 檢查日期有效性
        if flight_info.departure_date and flight_info.return_date:
            if flight_info.departure_date > flight_info.return_date:
                self.log_manager.log_warning("出發日期晚於返回日期")
                return False
        
        # 檢查至少有一個航段
        if not flight_info.outbound_segments and not flight_info.inbound_segments:
            self.log_manager.log_warning("航班沒有任何航段信息")
            return False
        
        # 檢查航段必要字段
        for segment in flight_info.outbound_segments + flight_info.inbound_segments:
            if not segment.flight_number:
                self.log_manager.log_warning("航段缺少航班編號")
                return False
        
        return True
    
    def save_to_storage(self, filename: str) -> bool:
        """
        將處理後的數據保存到儲存系統
        
        Args:
            filename: 儲存的文件名
            
        Returns:
            操作是否成功
        """
        if not self.storage_manager:
            self.log_manager.log_error("未配置儲存管理器，無法保存數據", Exception("未配置儲存管理器"))
            return False

        # 確保已經有處理好的數據
        if not self.processed_data:
            self.log_manager.log_warning("沒有處理好的數據可保存")
            return False
        
        # 確保已轉換為JSON和表格格式
        if not self.json_data:
            self.log_manager.log_debug("數據未轉換為JSON格式，正在轉換")
            self.convert_to_json()
        
        if self.table_data is None or (hasattr(self.table_data, 'empty') and self.table_data.empty):
            self.log_manager.log_debug("數據未轉換為表格格式，正在轉換")
            self.convert_to_table()

        # 保存到Cloud Storage
        gcs_path = f"{filename}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        success_gcs, error_message_gcs = self.storage_manager.save_to_cloud_storage(
            json_data=self.json_data,
            filename=gcs_path
        )
        
        if success_gcs:
            self.log_manager.log_info(f"成功將JSON數據保存到Cloud Storage: {gcs_path}")
        else:
            self.log_manager.log_error(f"保存到Cloud Storage時發生錯誤: {error_message_gcs}", 
                                      Exception("保存到Cloud Storage時發生錯誤"))
            return False

        # 保存到BigQuery
        success_bq, error_message_bq = self.storage_manager.save_to_bigquery(table_data=self.table_data)
        
        if success_bq:
            self.log_manager.log_info("成功將表格數據保存到BigQuery")
        else:
            self.log_manager.log_error(f"保存到BigQuery時發生錯誤: {error_message_bq}", 
                                      Exception("保存到BigQuery時發生錯誤"))
            return False

        self.log_manager.log_info(f"數據成功保存到所有目標位置")
        return True
