from config.config_manager import ConfigManager
from utils.log_manager import LogManager
from processors.data_processor import DataProcessor
from storage.storage_manager import StorageManager
from controllers.task_manager import TaskManager
from controllers.api_client import ApiClient
from parsers.json_parser import JsonParser
import uuid
import datetime
import time
import threading
import os
from typing import List, Dict, Optional, Any

class CrawlerController:
    """
    東南旅遊機票資料爬蟲系統 - 爬蟲控制器
    
    作為系統的主要入口點，協調整個爬蟲流程，管理任務執行
    """
    
    def __init__(self):
        """
        初始化爬蟲控制器
        """
        self.config_manager = ConfigManager()
        config_path = os.path.join(os.getcwd(), "southeast_travel_crawler/config/config.yaml")
        self.config_manager.load_config(config_path)
        self.log_manager = LogManager(self.config_manager)
        self.task_manager = TaskManager(
            max_concurrent_tasks=self.config_manager.config["task"]["max_concurrent_tasks"]
        )
        # 設置任務管理器的回調函數
        self.task_manager.set_crawler_callback(self._execute_crawling_task)
        self.log_manager.log_info("東南旅遊爬蟲控制器初始化完成")
    
    def _execute_crawling_task(self, task_id):
        """
        執行單個爬蟲任務的內部方法，用作任務管理器的回調函數
        
        使用東南旅遊的 GraphQL API 獲取機票資料，解析後存儲到指定位置
        
        Args:
            task_id: 任務ID
            
        Returns:
            任務執行結果
        """
        # 初始化API客戶端為 None，便於在 finally 區塊中檢查是否需要關閉
        api_client = None
        
        try:
            # 通過直接執行start_crawling的內部邏輯來避免重複釋放任務槽
            task = self.task_manager.get_task_status(task_id)
            if task is None:
                return {"status": "error", "message": f"找不到任務 {task_id}"}
            
            # 更新任務的開始時間
            task.start_time = datetime.datetime.now()
            created_time = task.parameters.get("created_time")
            created_time_str = created_time.strftime("%Y-%m-%d %H:%M:%S.%f") if created_time else "未知"
            self.log_manager.log_info(f"開始執行爬蟲任務 {task_id}，創建於 {created_time_str}，實際開始於 {task.start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
            
            # 檢查是否是重試任務，如果是則記錄日誌
            if hasattr(task, 'retry_info') and getattr(task, 'retry_count', 0) > 0:
                original_start = getattr(task, 'original_start_time', task.start_time)
                elapsed = (datetime.datetime.now() - original_start).total_seconds()
                self.log_manager.log_debug(
                    f"這是任務 {task_id} 的第 {getattr(task, 'retry_count', 0)} 次重試，距離原始開始時間已過 {elapsed:.2f} 秒"
                )
            
            # 如果是第一次執行任務，保存原始開始時間
            if not hasattr(task, 'original_start_time'):
                task.original_start_time = task.start_time
            
            # 更新任務狀態為運行中
            task.status = "running"
            self.log_manager.log_task_status(task_id, "running")
            
            # 構建GraphQL查詢負載
            query_payload = self._build_query_payload(task, searchLeg=1, searchId="")
            
            # 初始化API客戶端
            api_client = ApiClient(
                config_manager=self.config_manager, 
                log_manager=self.log_manager
            )
            
            # 發送GraphQL請求
            self.log_manager.log_info(f"發送GraphQL請求: {task_id}")
            api_response = api_client.send_graphql_request(query_payload)
            
            # 解析去程API響應
            json_parser = JsonParser(
                log_manager=self.log_manager, 
                config_manager=self.config_manager
            )
            json_parser.parse_api_response(api_response)
            outbound_flights = json_parser.get_structured_data()
            
            # 所有可能的去回程組合結果
            all_flight_combinations = []
            
            # 解析回程API響應
            self.log_manager.log_debug(f"開始處理 {len(outbound_flights)} 個去程航班的回程組合")
            for outbound_flight in outbound_flights:
                time.sleep(2)
                if not outbound_flight.search_id:
                    self.log_manager.log_warning(f"去程航班缺少 search_id，無法查詢回程")
                    continue
                    
                # 使用去程的 search_id 查詢對應回程
                query_payload = self._build_query_payload(task, searchLeg=2, searchId=outbound_flight.search_id)
                inbound_api_response = api_client.send_graphql_request(query_payload)

                # 解析回程資料並與去程組合
                flight_combinations = json_parser.parse_inbound_response(inbound_api_response, outbound_flight)
                
                if flight_combinations:
                    self.log_manager.log_debug(f"為去程航班 (search_id: {outbound_flight.search_id}) 找到 {len(flight_combinations)} 個回程組合")
                    all_flight_combinations.extend(flight_combinations)
                else:
                    self.log_manager.log_warning(f"去程航班 (search_id: {outbound_flight.search_id}) 沒有找到有效的回程組合")
            
            self.log_manager.log_info(f"共生成 {len(all_flight_combinations)} 個有效的去回程組合")
            flight_data = all_flight_combinations

            # 處理數據
            storage_manager = StorageManager(
                config_manager=self.config_manager, 
                log_manager=self.log_manager
            )
            data_processor = DataProcessor(
                log_manager=self.log_manager,
                storage_manager=storage_manager
            )
            data_processor.process_data(raw_data=flight_data)
            json_result = data_processor.convert_to_json()
            table_result = data_processor.convert_to_table()
            data_processor.save_to_storage(filename=f"southeast_travel_data_{task_id}")
            
            # 更新任務狀態為已完成，並使用 log_task_status 記錄
            task.status = "completed"
            task.end_time = datetime.datetime.now()
            self.log_manager.log_task_status(task_id, "completed")
            
            # 計算總執行時間，使用 original_start_time
            original_start = task.original_start_time
            if original_start is None:
                self.log_manager.log_info(f"任務 {task_id} 無原始開始時間記錄，使用當前開始時間")
                original_start = task.start_time
            
            total_execution_time = (task.end_time - original_start).total_seconds()
            
            task.result = {
                "flight_data": flight_data,
                "json": json_result,
                "table": table_result,
                "total_execution_time": f"{total_execution_time:.2f} 秒"
            }
            
            # 記錄總執行時間
            if hasattr(task, 'retry_info') and getattr(task, 'retry_count', 0) > 0:
                self.log_manager.log_debug(
                    f"爬蟲任務 {task_id} 完成，經過 {getattr(task, 'retry_count', 0)} 次重試，總耗時 {total_execution_time:.2f} 秒"
                )
            else:
                self.log_manager.log_debug(
                    f"爬蟲任務 {task_id} 完成，耗時 {total_execution_time:.2f} 秒，實際執行開始時間: {task.start_time.strftime('%Y-%m-%d %H:%M:%S.%f')}"
                )
                
            return {"status": "success", "task_id": task_id, "result": task.result}
            
        except Exception as e:
            error_message = f"爬蟲任務 {task_id} 執行出錯: {str(e)}"
            self.log_manager.log_error(error_message, e)
            
            # 更新任務狀態為失敗，並使用 log_task_status 記錄
            if task:
                task.status = "failed"
                task.end_time = datetime.datetime.now()
                task.error = str(e)
                self.log_manager.log_task_status(task_id, "failed")
                
                # 計算總執行時間
                if hasattr(task, 'original_start_time'):
                    total_execution_time = (task.end_time - task.original_start_time).total_seconds()
                    self.log_manager.log_info(
                        f"爬蟲任務 {task_id} 失敗，總耗時 {total_execution_time:.2f} 秒"
                    )
                
            return self.handle_error(e, task_id)
        finally:
            # 確保API客戶端資源被正確釋放
            if api_client is not None:
                try:
                    self.log_manager.log_debug(f"關閉任務 {task_id} 的API客戶端資源")
                    api_client.close_session()
                except Exception as close_error:
                    self.log_manager.log_error(f"關閉API客戶端時出錯: {str(close_error)}", close_error)
    
    def initialize(self, flight_number: Optional[str] = None, depart_date: Optional[str] = None, return_date: Optional[str] = None) -> Dict:
        """
        初始化爬蟲參數
        
        Args:
            flight_number: 航班編號（可選）
            depart_date: 出發日期（可選）
            return_date: 返回日期（可選）
            
        Returns:
            包含任務ID和初始狀態的字典
        """
        task_id = str(uuid.uuid4())
        created_time = datetime.datetime.now()
        task_params = {
            "task_id": task_id,
            "flight_number": flight_number,
            "depart_date": depart_date,
            "return_date": return_date,
            "status": "initialized",
            "created_time": created_time,  # 記錄任務創建時間
            "start_time": None,  # 開始時間暫設為 None，等到任務執行時再更新
            "end_time": None,
            "result": None
        }
        
        self.log_manager.log_info(f"初始化爬蟲任務 {task_id}，創建時間: {created_time.strftime('%Y-%m-%d %H:%M:%S.%f')}")
        self.task_manager.add_task(task_params)
        
        return {"task_id": task_id, "status": "initialized", "created_time": created_time}
    
    def _build_query_payload(self, task_data: Dict[str, Any], searchLeg: int = 1, searchId: str = "") -> Dict[str, Any]:
        """
        根據任務數據構建東南旅遊 GraphQL API 查詢參數
        
        Args:
            task_data: 任務數據字典，包含航班編號、出發日期、返回日期等
            searchLeg: 航班段序號，用於判斷是否為出發航班，1為出發航班，2為回程航班
            searchId: 航班段ID，搜尋去程則不需要
            
        Returns:
            Dict[str, Any]: GraphQL查詢參數，用於向東南旅遊API發送請求
        """
        # 從 CrawlTask 的 parameters 中獲取參數
        depart_date = task_data.parameters.get("url_params").get("DepDate1")
        return_date = task_data.parameters.get("url_params").get("DepDate2")
        
        # 獲取機場代碼
        dep_airport = task_data.parameters.get("url_params").get("DepCity1")
        arr_airport = task_data.parameters.get("url_params").get("ArrCity1")
        
        # 日期格式轉換 (YYYY-MM-DD -> YYYYMMDD)
        dep_date = depart_date.replace("-", "")
        ret_date = return_date.replace("-", "")
        
        # 構建航班段資訊
        flights = [
            {
                "depAirportCode": dep_airport,
                "arrAirportCode": arr_airport,
                "depDate": dep_date
            }
        ]
        
        # 如果有回程日期，添加回程航班段
        if ret_date:
            flights.append({
                "depAirportCode": arr_airport,
                "arrAirportCode": dep_airport,
                "depDate": ret_date
            })
        
        # 構建東南旅遊的GraphQL查詢
        query = """query ($portType: String, $companyNo: String, $adtCount: Int, $chdCount: Int, $pfpClass: String, $directFlightOnly: Boolean, $tripType: String, $airlineCode: String, $isDomestic: Boolean, $flights: [pfpFlightSegmentSearchFlightsData], $gdsType: String, $searchLeg: Int!, $searchId: String, $gdsSearch: String) {
  pfpFlightSegmentSearch(portType: $portType, companyNo: $companyNo, adtCount: $adtCount, chdCount: $chdCount, pfpClass: $pfpClass, directFlightOnly: $directFlightOnly, tripType: $tripType, airlineCode: $airlineCode, isDomestic: $isDomestic, flights: $flights, gdsType: $gdsType, searchLeg: $searchLeg, searchId: $searchId, gdsSearch: $gdsSearch) {
    error {
      msgCode
      msgDesc
      __typename
    }
    data {
      searchLeg
      flightList {
        segno
        transTime
        fullFight
        arrDate
        arrTime
        depDate
        depTime
        depAirportCode
        turnTimes
        flyTime
        arrAirportCode
        flightDays
        turnAirlineData {
          airportCode
          isDiffTurn
          nextDepAirportCode
          __typename
        }
        flightDetail {
          airEquipType
          transTime
          depDate
          depTime
          arrDate
          arrTime
          marketingAirlineCode
          operatingAirlineCode
          flightNumber
          operatingFlightNumber
          depAirportCode
          arrAirportCode
          flyTime
          flightDays
          stopPoint
          marriageGrp
          __typename
        }
        fareList {
          searchId
          fareSeq
          bccTp
          pfpClass
          pfpClassName
          accountCode
          mainAirlineCd
          fareType
          seat
          internalId
          uniqueOfferReference
          offerItemId
          isNdc
          ndcPaymentDl
          ndcTicketDl
          couponList {
            coupNo
            listTitle
            coupLimitValue
            __typename
          }
          baggageInfo {
            segno
            bags {
              quantity
              type
              __typename
            }
            __typename
          }
          fareInfo {
            totalPrice {
              price
              sourcePrice
              displayBeforeDiscountPrice
              __typename
            }
            tax {
              totalTax
              chdTax
              adtTax
              __typename
            }
            price {
              adt {
                price
                sourcePrice
                discountPercent
                __typename
              }
              chd {
                price
                sourcePrice
                discountPercent
                __typename
              }
              discount {
                discountPercent
                discountValue
                __typename
              }
              __typename
            }
            __typename
          }
          lowestLabelList {
            labelSeqno
            labelBegDt
            labelEndDt
            labelSubject
            labelContent
            __typename
          }
          __typename
        }
        __typename
      }
      gdsTime
      __typename
    }
    __typename
  }
}"""
        
        # 構建針對東南旅遊API的查詢變數
        variables = {
            "adtCount": "1",
            "chdCount": "0",
            "directFlightOnly": False,
            "pfpClass": "",
            "airlineCode": "",
            "flights": flights,
            "isDomestic": False,
            "searchLeg": searchLeg,
            "searchId": searchId,
            "gdsSearch": ""
        }
        
        # 返回完整的GraphQL查詢負載
        payload = {
            "operationName": None,
            "variables": variables,
            "query": query
        }
        
        self.log_manager.log_debug(f"構建東南旅遊GraphQL查詢負載: {payload}")
        return payload
    
    def start_crawling(self, task_id: str = None) -> Dict:
        """
        開始單個爬蟲任務
        
        Args:
            task_id: 任務ID
            
        Returns:
            任務執行結果
        """
        if task_id is None:
            # 如果沒有提供任務ID，獲取隊列中的第一個任務
            task = self.task_manager.get_next_task()
            if task is None:
                return {"status": "error", "message": "沒有可執行的任務"}
            task_id = task.task_id
        
        try:
            result = self._execute_crawling_task(task_id)
            return result
        finally:
            # 釋放任務槽位
            self.task_manager.release_task_slot()
    
    def batch_crawling(self, task_list: List[Dict]) -> Dict:
        """
        批次執行多個爬蟲任務
        
        Args:
            task_list: 任務參數列表
            
        Returns:
            批次任務執行結果
        """
        task_ids = []
        
        # 將所有任務添加到隊列
        batch_id = f"batch_{str(uuid.uuid4())[:8]}"
        self.log_manager.log_info(f"開始批次任務 {batch_id} 的任務初始化")
        
        for task_params in task_list:
            task_id = str(uuid.uuid4())
            created_time = datetime.datetime.now()
            
            task_params["task_id"] = task_id
            task_params["status"] = "initialized"
            task_params["created_time"] = created_time  # 記錄任務創建時間
            task_params["start_time"] = None  # 開始時間暫設為 None，等到任務執行時再更新
            
            self.task_manager.add_task(task_params)
            task_ids.append(task_id)
            
            # 每10個任務記錄一次進度，避免日誌過多
            if len(task_ids) % 10 == 0 or len(task_ids) == len(task_list):
                self.log_manager.log_debug(f"批次任務 {batch_id} 已初始化 {len(task_ids)}/{len(task_list)} 個任務")
        
        # 啟動批處理任務
        self.log_manager.log_info(f"批次任務 {batch_id} 初始化完成，共 {len(task_ids)} 個任務，開始處理")
        
        # 交給任務管理器處理批量任務
        self.task_manager.process_batch_tasks()
        
        # 等待任務完成或超時
        max_wait_time = self.config_manager.config["task"]["task_timeout"] * 60
        start_time = time.time()
        completed_tasks = []
        last_progress_report = start_time
        progress_interval = 5  # 每5秒報告一次進度
        
        while len(completed_tasks) < len(task_ids) and (time.time() - start_time) < max_wait_time:
            current_time = time.time()
            # 檢查新完成的任務
            for task_id in task_ids:
                if task_id not in completed_tasks:
                    task = self.task_manager.get_task_status(task_id)
                    if task and task.status in ["completed", "failed"]:
                        completed_tasks.append(task_id)
                        # 立即報告任務完成情況
                        self.log_manager.log_info(
                            f"任務 {task_id} 已{task.status}，進度: {len(completed_tasks)}/{len(task_ids)}"
                        )
            
            # 定期報告進度
            if current_time - last_progress_report >= progress_interval:
                last_progress_report = current_time
                self.log_manager.log_info(
                    f"批次任務 {batch_id} 進度: {len(completed_tasks)}/{len(task_ids)} 已完成"
                )
                
                # 檢查並報告運行中任務
                running_count = 0
                for task_id in task_ids:
                    if task_id not in completed_tasks:
                        task = self.task_manager.get_task_status(task_id)
                        if task and task.status == "running":
                            running_count += 1
                
                if running_count > 0:
                    self.log_manager.log_info(f"當前有 {running_count} 個任務正在運行")
            
            # 所有任務已完成，提前結束等待
            if len(completed_tasks) == len(task_ids):
                self.log_manager.log_info(f"批次任務 {batch_id} 所有任務已完成")
                break
                
            # 避免頻繁檢查，節省CPU資源
            time.sleep(0.5)
        
        elapsed_time = time.time() - start_time
        
        # 收集結果
        results = {
            "batch_id": batch_id,
            "total_tasks": len(task_ids),
            "completed_tasks": len(completed_tasks),
            "elapsed_time": f"{elapsed_time:.2f} 秒",
            "tasks": {}
        }
        
        for task_id in task_ids:
            task = self.task_manager.get_task_status(task_id)
            if task:
                results["tasks"][task_id] = {
                    "status": task.status,
                }
                
                # 添加創建時間
                if hasattr(task, "created_time") and task.created_time is not None:
                    results["tasks"][task_id]["created_time"] = task.created_time
                
                # 添加開始時間
                if hasattr(task, "start_time") and task.start_time is not None:
                    results["tasks"][task_id]["start_time"] = task.start_time
                
                # 安全地獲取 end_time (如果存在)
                if hasattr(task, "end_time") and task.end_time is not None:
                    results["tasks"][task_id]["end_time"] = task.end_time
                else:
                    # 如果任務已完成但沒有 end_time，添加當前時間作為 end_time
                    if task.status in ["completed", "failed"]:
                        results["tasks"][task_id]["end_time"] = datetime.datetime.now()
                
                if task.status == "completed" and hasattr(task, "result"):
                    results["tasks"][task_id]["result"] = "available"
                elif task.status == "failed" and hasattr(task, "error"):
                    results["tasks"][task_id]["error"] = task.error
        
        # 檢查是否有超時未完成的任務
        if len(completed_tasks) < len(task_ids):
            self.log_manager.log_error(
                f"批次任務 {batch_id} 已超時，有 {len(task_ids) - len(completed_tasks)} 個任務未完成"
            )
            results["timeout"] = True
        
        self.log_manager.log_info(f"批次任務 {batch_id} 執行完成，耗時 {elapsed_time:.2f} 秒")
        return results
    
    def handle_error(self, exception: Exception, task_id: Optional[str] = None) -> Dict:
        """
        處理錯誤情況，並執行重試邏輯
        
        Args:
            exception: 異常對象
            task_id: 任務ID（可選）
            
        Returns:
            錯誤處理結果
        """
        error_message = str(exception)
        error_type = type(exception).__name__
        
        self.log_manager.log_error(f"錯誤: {error_message}", exception)
        
        # 檢查是否需要重試
        retry_config = self.config_manager.config["retry"]
        should_retry = error_type in retry_config["retry_on_errors"]
        
        if task_id and should_retry:
            # 獲取任務
            task = self.task_manager.get_task_status(task_id)
            if task:
                retry_count = task.get("retry_count", 0)
                max_attempts = retry_config["max_attempts"]
                
                if retry_count <= max_attempts:
                    # 計算重試延遲時間
                    backoff_factor = retry_config["backoff_factor"]
                    retry_interval = retry_config["interval"] * (backoff_factor ** retry_count)
                    
                    # 更新重試計數
                    task.retry_count = retry_count + 1
                    # 更新任務狀態為重試中，並使用 log_task_status 記錄
                    task.status = "retrying"
                    self.log_manager.log_task_status(task_id, "retrying")
                    task.last_error = error_message
                    
                    self.log_manager.log_info(f"任務 {task_id} 將在 {retry_interval} 秒後重試 (嘗試 {retry_count + 1}/{max_attempts})")
                    
                    # 使用定時器在指定延遲後執行重試
                    timer = threading.Timer(
                        retry_interval, 
                        self._schedule_retry_task, 
                        args=[task_id]
                    )
                    timer.daemon = True  # 設置為守護線程，避免主程序退出時線程仍在運行
                    timer.start()
                    
                    # 直接返回重試信息
                    return {
                        "status": "retrying",
                        "task_id": task_id,
                        "retry_count": retry_count + 1,
                        "max_attempts": max_attempts,
                        "retry_interval": retry_interval,
                        "error": error_message
                    }
        
        # 無需重試或無法重試
        return {
            "status": "error",
            "error_type": error_type,
            "error_message": error_message,
            "task_id": task_id
        }
    
    def _schedule_retry_task(self, task_id: str) -> None:
        """
        將重試任務加入任務管理器的隊列，並確保不超過最大並行限制
        
        Args:
            task_id: 需要重試的任務ID
        """
        # 獲取任務信息
        task = self.task_manager.get_task_status(task_id)
        if not task:
            self.log_manager.log_error(f"無法重試任務 {task_id}：找不到任務信息")
            return
            
        # 確保任務仍處於等待重試狀態
        if task.status != "retrying":
            self.log_manager.log_info(f"任務 {task_id} 狀態已變更為 {task.status}，取消重試")
            return
            
        self.log_manager.log_info(f"開始重試任務 {task_id} (第 {task.get('retry_count', 0)} 次嘗試)")
        
        # 保存原始開始時間（首次執行時間）
        original_start_time = task.get("original_start_time")
        
        # 如果沒有原始開始時間（極少數情況），使用任務最早的有效開始時間
        if original_start_time is None:
            original_start_time = task.get("start_time")
            if original_start_time is None:
                # 如果還是沒有有效的時間，使用創建時間
                original_start_time = task.get("created_time") or datetime.datetime.now()
            self.log_manager.log_info(f"任務 {task_id} 沒有原始開始時間記錄，使用最早的可用時間")
        
        # 將任務重置為初始狀態以便重新執行
        # 但保留重試計數、原始開始時間和其他重要信息
        retry_info = {
            "retry_count": task.get("retry_count", 0),
            "last_error": task.get("last_error"),
            "original_start_time": original_start_time,
            "retry_history": task.get("retry_info", {}).get("retry_history", []) + [
                {
                    "retry_number": task.get("retry_count", 0),
                    "error": task.get("last_error"),
                    "retry_time": datetime.datetime.now().isoformat()
                }
            ]
        }
        
        # 保存原始任務參數但更新狀態
        task.status = "initialized"
        self.log_manager.log_task_status(task_id, "initialized")
        task.retry_info = retry_info
        task.original_start_time = original_start_time  # 保存到頂層，便於查詢
        task.start_time = None  # 重置開始時間為 None，等到實際執行時再更新
        
        # 重新加入隊列
        self.task_manager.add_task(task)
        self.log_manager.log_info(f"任務 {task_id} 已重新加入隊列末尾，等待執行")
        
        # 如果批處理尚未啟動，則嘗試啟動
        if hasattr(self.task_manager, "process_batch_tasks"):
            # 檢查工作線程是否已經啟動
            worker_threads_running = hasattr(self.task_manager, "worker_threads") and len(self.task_manager.worker_threads) > 0
            if not worker_threads_running:
                self.log_manager.log_info("啟動批次任務處理")
                self.task_manager.process_batch_tasks()
