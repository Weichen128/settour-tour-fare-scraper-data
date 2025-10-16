from controllers.crawler_controller import CrawlerController
from processors.flight_tasks_fixed_month_processors import FlightTasksFixedMonthProcessors
from processors.flight_tasks_holidays_processors import FlightTasksHolidaysProcessors
from config.config_manager import ConfigManager
import os
import json

def main():
    """
    東南旅遊機票爬蟲系統主入口
    """
    crawler_controller = CrawlerController()
    final_result = {"status": "error", "message": "未執行任何任務"} # 初始化預設結果

    # 載入配置文件
    config_manager = ConfigManager()
    config_manager.load_config(os.path.join(os.getcwd(), "southeast_travel_crawler/config/config.yaml"))

    # 處理固定月份日期爬蟲任務
    flight_tasks_fixed_month_processors = FlightTasksFixedMonthProcessors(config_manager)
    # 處理節日爬蟲任務
    flight_tasks_holidays_processors = FlightTasksHolidaysProcessors(config_manager)
    
    # 沒有提供參數，嘗試從配置文件讀取預定義任務
    try:
        flight_tasks = crawler_controller.config_manager.config.get("flight_tasks", [])
        flight_tasks_fixed_month = flight_tasks_fixed_month_processors.process_flight_tasks()
        flight_tasks.extend(flight_tasks_fixed_month)
        flight_tasks_holidays = flight_tasks_holidays_processors.process_flight_tasks()
        flight_tasks.extend(flight_tasks_holidays)
        if flight_tasks:
            print(f"從配置中讀取到 {len(flight_tasks)} 個預定義任務")
            # 調用 batch_crawling 執行預定義任務
            final_result = crawler_controller.batch_crawling(flight_tasks)
            print(f"預定義批量任務執行狀態: 總計 {final_result.get('total_tasks', 0)} 個任務，已完成 {final_result.get('completed_tasks', 0)} 個")
        else:
            print("錯誤: 未提供 URL 或任務文件，且配置中沒有預定義任務。請使用 --url 或 --tasks 指定任務。")
            final_result = {"status": "error", "message": "未提供任務參數，且配置中無預定義任務"}
    except Exception as e:
        print(f"執行預定義任務時出錯: {str(e)}")
        final_result = {"status": "error", "message": f"執行預定義任務出錯: {str(e)}"}
        import traceback
        traceback.print_exc()

    return final_result

if __name__ == "__main__":
    # 執行主邏輯
    result = main()

    # 將最終結果輸出為JSON格式
    print("\n--- 最終執行結果 ---")
    # 使用 default=str 處理 datetime 對象
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
