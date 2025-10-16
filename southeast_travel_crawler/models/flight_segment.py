"""
東南旅遊機票資料爬蟲系統 - 航班段模型

此模組定義了航班段 (FlightSegment) 資料模型，用於表示單個航班段的詳細資訊。
"""
import json
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class FlightSegment:
    """
    表示單個航班段的詳細資訊

    屬性:
        flight_number (str): 航班編號
        cabin_class (str): 艙等
    """
    flight_number: Optional[str] = None
    cabin_class: Optional[str] = None

    def to_json(self) -> str:
        """
        將航班段資訊轉換為 JSON 格式

        返回:
            str: 包含航班段資訊的 JSON 字串
        """
        return json.dumps(self.to_dict())

    def to_dict(self) -> Dict[str, Any]:
        """
        將航班段資訊轉換為字典格式

        返回:
            Dict[str, Any]: 包含航班段資訊的字典
        """
        return {
            "flight_number": self.flight_number,
            "cabin_class": self.cabin_class
        }
