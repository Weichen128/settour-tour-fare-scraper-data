"""
東南旅遊機票資料爬蟲系統 - 爬蟲任務模型

此模組定義了爬蟲任務 (CrawlTask) 資料模型，用於表示單個爬蟲任務的狀態與結果。
"""
from datetime import datetime
from typing import Dict, List, Any, Optional
from .flight_info import FlightInfo


class CrawlTask:
    """
    表示單個爬蟲任務

    屬性:
        task_id (str): 任務ID
        parameters (Dict): 爬蟲參數
        status (str): 任務狀態
        start_time (datetime): 開始時間
        end_time (datetime): 結束時間
        result (List[FlightInfo]): 任務結果
    """

    def __init__(
        self,
        task_id: str,
        parameters: Dict[str, Any],
        status: str = "pending",
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        result: Optional[List[FlightInfo]] = None
    ):
        """
        初始化爬蟲任務實例

        參數:
            task_id (str): 任務ID
            parameters (Dict[str, Any]): 爬蟲參數
            status (str, optional): 任務狀態，預設為 "pending"
            start_time (datetime, optional): 開始時間
            end_time (datetime, optional): 結束時間
            result (List[FlightInfo], optional): 任務結果
        """
        self.task_id = task_id
        self.parameters = parameters
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.result = result if result is not None else []

    def to_dict(self) -> Dict[str, Any]:
        """
        將爬蟲任務轉換為字典格式

        返回:
            Dict[str, Any]: 包含爬蟲任務資訊的字典
        """
        return {
            "task_id": self.task_id,
            "parameters": self.parameters,
            "status": self.status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "result": [flight_info.to_dict() for flight_info in self.result]
        }
