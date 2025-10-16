"""
東南旅遊機票資料爬蟲系統 - 航班資訊模型

此模組定義了航班資訊 (FlightInfo) 資料模型，用於表示單個機票的完整資訊，
包含去程和回程的所有航段。
"""
import json
from datetime import date
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from .flight_segment import FlightSegment


@dataclass
class FlightInfo:
    """
    表示單個機票的資訊，包含去程和回程的所有航段

    屬性:
        departure_date (date): 出發日期
        return_date (date): 返回日期
        price (float): 票價
        tax (float): 稅金
        outbound_segments (List[FlightSegment]): 去程航段列表
        inbound_segments (List[FlightSegment]): 回程航段列表
        search_id (Optional[str]): 搜尋ID，用於關聯去程和回程
    """
    departure_date: Optional[date] = None
    return_date: Optional[date] = None
    price: float = 0.0
    tax: float = 0.0
    outbound_segments: List[FlightSegment] = field(default_factory=list)
    inbound_segments: List[FlightSegment] = field(default_factory=list)
    search_id: Optional[str] = None

    def to_json(self) -> str:
        """
        將航班資訊轉換為 JSON 格式

        返回:
            str: 包含航班資訊的 JSON 字串
        """
        return json.dumps(self.to_dict(), default=str)

    def to_dict(self) -> Dict[str, Any]:
        """
        將航班資訊轉換為字典格式

        返回:
            Dict[str, Any]: 包含航班資訊的字典
        """
        return {
            "departure_date": self.departure_date.strftime("%Y-%m-%d") if self.departure_date else None,
            "return_date": self.return_date.strftime("%Y-%m-%d") if self.return_date else None,
            "price": self.price,
            "tax": self.tax,
            "outbound_segments": [segment.to_dict() for segment in self.outbound_segments],
            "inbound_segments": [segment.to_dict() for segment in self.inbound_segments],
            "search_id": self.search_id
        }
