from config.config_manager import ConfigManager
from datetime import datetime
import calendar
from typing import Dict, List

class FlightTasksFixedMonthProcessors:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager

    def process_flight_tasks(self) -> List[Dict]:
        """
        處理固定月份日期爬蟲任務列表

        返回:
            List[Dict]: 處理後的爬蟲任務列表
            範例格式
            [
                {
                    'name': '範例：台北到新加坡 2025-05-19出發 2025-05-20回程',
                    'url_params': {
                        'DepCity1': 'TPE',
                        'ArrCity1': 'SIN',
                        'DepCountry1': 'TW',
                        'ArrCountry1': 'SG',
                        'DepDate1': '21/07/2025',
                        'DepDate2': '27/07/2025',
                        'Rtow': 1
                    }
                }
            ]
        """
        fixed_month_task_list = self._get_fixed_month_task_list()

        # 儲存處理後的爬蟲任務列表
        processed_flight_tasks = []

        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month

        # 將固定月份日期爬蟲任務列表中的任務進行處理
        for task in fixed_month_task_list:
            # 根據任務中的 Month 參數，計算出距離現在月份往後推的月份
            month_offset = task["url_params"]["Month"]
            target_month = current_month + month_offset
            target_year = current_year
            
            # 處理跨年的情況
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            # 獲取該月份的天數，用於驗證日期有效性
            days_in_month = calendar.monthrange(target_year, target_month)[1]
            
            processed_task = task.copy()
            processed_task["url_params"] = task["url_params"].copy()
            
            dep_day = int(task["url_params"]["DepDate1"])
            return_day = int(task["url_params"]["DepDate2"])
            
            # 確保日期不超過該月份的最大天數
            dep_day = min(dep_day, days_in_month)
            return_day = min(return_day, days_in_month)
            
            dep_date_str = f"{target_year}-{target_month:02d}-{dep_day:02d}"
            return_date_str = f"{target_year}-{target_month:02d}-{return_day:02d}"
            
            # 更新任務參數
            processed_task["url_params"]["DepDate1"] = dep_date_str
            processed_task["url_params"]["DepDate2"] = return_date_str
            
            # 移除原始的 Month 參數，因為已經轉換為具體日期
            if "Month" in processed_task["url_params"]:
                del processed_task["url_params"]["Month"]
            
            dep_city = task["url_params"].get("DepCity1", "")
            arr_city = task["url_params"].get("ArrCity1", "")
            
            processed_task["name"] = f"範例：{dep_city}到{arr_city} {target_year}-{target_month:02d}-{dep_day:02d}出發 {target_year}-{target_month:02d}-{return_day:02d}回程"
            
            processed_flight_tasks.append(processed_task)
            
        return processed_flight_tasks

    def _get_fixed_month_task_list(self) -> List[Dict]:
        """
        獲取固定月份日期爬蟲任務列表

        返回:
            List[Dict]: 固定月份日期爬蟲任務列表
        """
        return self.config_manager.get_flight_tasks_fixed_month()
