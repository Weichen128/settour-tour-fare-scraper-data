from config.config_manager import ConfigManager
from datetime import datetime, timedelta
from typing import Dict, List
import requests
import json

class FlightTasksHolidaysProcessors:
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager

    def process_flight_tasks(self) -> List[Dict]:
        """
        處理節日爬蟲任務列表

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
        # 獲取基礎任務列表
        holidays_task_list = self._get_holidays_task_list()
        
        # 處理後的任務列表
        processed_flight_tasks = []
        
        current_date = datetime.now()
        current_year = current_date.year
        current_month = current_date.month
        
        # 遍歷每個基礎任務
        for base_task in holidays_task_list:
            # 根據任務中的 Month 參數，計算出目標月份
            month_offset = base_task["url_params"]["Month"]
            target_month = current_month + month_offset
            target_year = current_year
            
            # 處理跨年的情況
            while target_month > 12:
                target_month -= 12
                target_year += 1
            
            # 獲取該月份的節假日資料
            taiwan_holidays = self._fetch_taiwan_holidays(target_year, target_month)
            
            # 遍歷該月份的每個節假日
            for holiday in taiwan_holidays:
                if not holiday.get('description'):
                    continue
                elif self._is_skip_holiday(holiday, base_task):
                    continue
                    
                # 獲取爬取日期範圍
                date_ranges = self._get_crawl_date_ranges(holiday)
                
                # 為每個日期範圍生成任務
                dep_date = date_ranges[0] 
                ret_date = date_ranges[1]
                processed_task = base_task.copy()
                processed_task["url_params"] = base_task["url_params"].copy()
                
                # 格式化日期為 DD/MM/YYYY
                dep_date_str = dep_date.strftime("%Y-%m-%d")
                ret_date_str = ret_date.strftime("%Y-%m-%d")
                
                # 更新任務參數
                processed_task["url_params"]["DepDate1"] = dep_date_str
                processed_task["url_params"]["DepDate2"] = ret_date_str
                
                # 移除原始的 Month 參數，因為已經轉換為具體日期
                if "Month" in processed_task["url_params"]:
                    del processed_task["url_params"]["Month"]
                
                # 生成任務名稱
                dep_city = base_task["url_params"].get("DepCity1", "")
                arr_city = base_task["url_params"].get("ArrCity1", "")
                holiday_desc = holiday.get('description', '')
                
                processed_task["name"] = f"{dep_city}到{arr_city} {holiday_desc} {dep_date.strftime('%Y-%m-%d')}出發 {ret_date.strftime('%Y-%m-%d')}回程"
                
                processed_flight_tasks.append(processed_task)
                    
        return processed_flight_tasks
    
    def _is_skip_holiday(self, holiday: Dict, base_task: Dict) -> bool:
        """
        判斷是否是用特殊規則跳過此日期

        參數:
            holiday: 節假日資料

        返回:
            bool: 是否是用特殊規則跳過此日期
        """
        month = base_task["url_params"].get("Month", None)
        date_str = holiday.get('date', '')
        holiday_date = datetime.strptime(date_str, "%Y%m%d")
        day = holiday_date.day

        if any(keyword in holiday.get('description', '') for keyword in ['春節', '農曆除夕']):
            return True


        # HACK: 需求中描述 爬取「往後第2個月的5號～10號」以及「往後第6個月的24號～28號」
        # 1. 若 國定假日 有在「固定區間」資料內，則僅爬取「固定區間」資料。
        # 2. 若 國定假日 沒有在「固定區間」資料內，則要爬取「固定區間」資料與「國定假日區間」資料。
        # 為滿足此需求因此如此設計
        if month == 2 and 5 <= day <= 10:
            return True
        elif month == 6 and 24 <= day <= 28:
            return True
        else:
            return False

    def _get_holidays_task_list(self) -> List[Dict]:
        """
        獲取節日爬蟲任務列表

        返回:
            List[Dict]: 節日爬蟲任務列表
        """
        return self.config_manager.get_flight_tasks_holidays()
    
    def _fetch_taiwan_holidays(self, target_year: int, target_month: int) -> List[Dict]:
        """
        從外部API獲取指定年月的台灣節假日資料
        
        參數:
            target_year: 目標年份
            target_month: 目標月份
            
        返回:
            List[Dict]: 該月份的節假日資料列表
        """
        url = f"https://cdn.jsdelivr.net/gh/ruyut/TaiwanCalendar/data/{target_year}.json"
        holidays_data = []
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                year_data = response.content.decode('utf-8-sig')
                year_data = json.loads(year_data)
                # 只保留指定月份且有description的節假日
                for holiday in year_data:
                    if (holiday.get('isHoliday') and 
                        holiday.get('description') != '' and
                        holiday['date'].startswith(f"{target_year}{target_month:02d}")):
                        holidays_data.append(holiday)
                holidays_data = self._remove_holiday_with_compensatory_day(holidays_data)
        except requests.RequestException as e:
            print(f"無法獲取 {target_year} 年 {target_month} 月節假日資料: {e}")
            raise e
                
        return holidays_data
    
    def _remove_holiday_with_compensatory_day(self, holidays_data: List[Dict]) -> List[Dict]:
        """
        剔除API描述中補假的國定假日

        如果資料中包含"補"這個字，則剔除
        
        參數:
            holidays_data: 節假日資料列表
            
        返回:
            List[Dict]: 剔除補假後的節假日資料列表
        """
        return [holiday for holiday in holidays_data if '補' not in holiday.get('description')]
    
    def _get_crawl_date_ranges(self, holiday: Dict) -> tuple:
        """
        根據節假日和星期幾，返回需要爬取的日期範圍
        
        參數:
            holiday: 節假日資料
            
        返回:
            tuple: (出發日期, 回程日期)
        """
        # 解析日期
        date_str = holiday['date']
        holiday_date = datetime.strptime(date_str, "%Y%m%d")
        weekday = holiday['week']
        description = holiday.get('description', '')
        
        # 根據不同情況設定爬取日期
        if '開國紀念日' in description and weekday == '三':
            # 開國紀念日落在週三的特殊規則
            crawl_dates = (holiday_date - timedelta(days=4), holiday_date)
        elif '小年夜' in description:
            # 春節規則（以小年夜為基準）
            if weekday == '一':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=4))
            elif weekday == '二':
                crawl_dates = (holiday_date - timedelta(days=3), holiday_date + timedelta(days=3))
            elif weekday == '三':
                crawl_dates = (holiday_date - timedelta(days=4), holiday_date + timedelta(days=2))
            elif weekday == '四':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=4))
            elif weekday == '五':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=4))
            elif weekday == '六':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=3))
            elif weekday == '日':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=3))
        else:
            # 一般國定假日規則
            if weekday == '一':
                crawl_dates = (holiday_date - timedelta(days=4), holiday_date)
            elif weekday == '二':
                crawl_dates = (holiday_date - timedelta(days=4), holiday_date)
            elif weekday == '三':
                crawl_dates = (holiday_date , holiday_date + timedelta(days=3))
            elif weekday == '四':
                crawl_dates = (holiday_date - timedelta(days=1), holiday_date + timedelta(days=3))
            elif weekday == '五':
                crawl_dates = (holiday_date - timedelta(days=2), holiday_date + timedelta(days=2))
            elif weekday == '六':
                crawl_dates = (holiday_date - timedelta(days=3), holiday_date + timedelta(days=1))
            elif weekday == '日':
                crawl_dates = (holiday_date - timedelta(days=4), holiday_date)
                
        return crawl_dates
 