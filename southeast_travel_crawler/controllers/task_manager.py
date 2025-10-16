"""
東南旅遊機票資料爬蟲系統 - 任務管理器

此模組實現任務管理器(TaskManager)類別,負責管理爬蟲任務的創建、執行與狀態追蹤。
"""
from typing import Dict, Any, Optional
import threading
import queue
import time
import uuid
import datetime  # 新增 datetime 模組
from models.crawl_task import CrawlTask

class TaskManager:
    """
    任務管理器，管理爬蟲任務隊列，控制並行任務數量，確保系統資源合理利用
    """
    
    def __init__(self, max_concurrent_tasks: int = 4):
        """
        初始化任務管理器
        
        Args:
            max_concurrent_tasks: 最大並行任務數，預設為4
        """
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_queue = queue.Queue()
        self.active_tasks = {}  # 活動任務字典，鍵為任務ID，值為任務參數
        self.tasks_data = {}    # 所有任務數據，包括已完成的任務
        self.task_slots = threading.Semaphore(max_concurrent_tasks)  # 用於控制並行任務數
        self.lock = threading.Lock()  # 用於同步訪問共享資源
        self.crawler_callback = None  # 爬蟲控制器回調函數
    
    def set_crawler_callback(self, callback_function):
        """
        設置爬蟲控制器回調函數
        
        Args:
            callback_function: 用於執行爬蟲任務的回調函數
        """
        self.crawler_callback = callback_function
    
    def add_task(self, task_params: Dict[str, Any]) -> str:
        """
        添加新任務到隊列
        
        Args:
            task_params: 任務參數字典
            
        Returns:
            任務ID
        """
        task_id = task_params.get("task_id")
        if not task_id:
            task_id = str(uuid.uuid4())
            task_params["task_id"] = task_id
        
        # 使用 CrawlTask 物件包裝任務參數
        crawl_task = CrawlTask(
            task_id=task_id,
            parameters=task_params,
            status="initialized",
            start_time=None,
            end_time=None,
            result=None
        )
        
        with self.lock:
            self.tasks_data[task_id] = crawl_task
            self.task_queue.put(task_id)
        
        return task_id
    
    def process_batch_tasks(self):
        """
        處理批量任務，利用多執行緒並行處理任務隊列
        """
        # 建立多個工作執行緒，數量等於最大並行任務數
        worker_threads = []
        for i in range(self.max_concurrent_tasks):
            worker_thread = threading.Thread(
                target=self._task_worker,
                name=f"worker-{i+1}"
            )
            worker_thread.daemon = True
            worker_threads.append(worker_thread)
            worker_thread.start()
        
        # 記錄已啟動的執行緒
        self.worker_threads = worker_threads
    
    def _task_worker(self):
        """
        任務工作執行緒方法，從隊列中取出任務並執行
        """
        while not self.is_queue_empty():
            # 等待獲取任務槽位
            if not self.task_slots.acquire(blocking=False):
                # 如果沒有可用槽位，等待一段時間後重試
                time.sleep(0.5)
                continue
            
            try:
                # 從隊列中取出任務ID
                task_id = self.task_queue.get(block=False)
            except queue.Empty:
                # 若隊列為空，釋放槽位並退出
                self.task_slots.release()
                break
            
            # 標記任務為活動狀態
            with self.lock:
                crawl_task = self.tasks_data.get(task_id)
                if crawl_task:
                    crawl_task.status = "running"
                    crawl_task.start_time = datetime.datetime.now()
                    self.active_tasks[task_id] = crawl_task
            
            # 執行爬蟲任務
            if self.crawler_callback:
                try:
                    # 使用回調函數執行爬蟲任務
                    result = self.crawler_callback(task_id)
                    
                    # 根據結果更新任務狀態
                    with self.lock:
                        crawl_task = self.tasks_data.get(task_id)
                        if crawl_task:
                            if result.get("status") == "success":
                                crawl_task.status = "completed"
                            else:
                                crawl_task.status = "failed"
                                crawl_task.error_info = result.get("error_message", "未知錯誤")
                            crawl_task.end_time = datetime.datetime.now()
                except Exception as e:
                    # 處理執行過程中的異常
                    with self.lock:
                        crawl_task = self.tasks_data.get(task_id)
                        if crawl_task:
                            crawl_task.status = "failed"
                            crawl_task.error_info = str(e)
                            crawl_task.end_time = datetime.datetime.now()
            else:
                # 如果沒有設置回調函數，模擬任務完成
                time.sleep(0.5)  # 模擬任務執行時間
                with self.lock:
                    crawl_task = self.tasks_data.get(task_id)
                    if crawl_task:
                        crawl_task.status = "completed"
                        crawl_task.end_time = datetime.datetime.now()
            
            # 標記任務已處理
            self.task_queue.task_done()
            
            # 從活動任務中移除
            with self.lock:
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
            
            # 釋放任務槽位
            self.task_slots.release()
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        獲取任務狀態
        
        Args:
            task_id: 任務ID
            
        Returns:
            任務參數字典或None（如果任務不存在）
        """
        with self.lock:
            return self.tasks_data.get(task_id)
    
    def get_next_task(self) -> Optional[Dict[str, Any]]:
        """
        獲取隊列中的下一個任務
        
        Returns:
            任務參數字典或None（如果隊列為空）
        """
        try:
            task_id = self.task_queue.get(block=False)
            self.task_queue.task_done()
            with self.lock:
                return self.tasks_data.get(task_id)
        except queue.Empty:
            return None
    
    def handle_task_failure(self, task_id: str, error: str = None):
        """
        處理任務失敗
        
        Args:
            task_id: 任務ID
            error: 錯誤信息（可選）
        """
        with self.lock:
            crawl_task = self.tasks_data.get(task_id)
            if crawl_task:
                crawl_task.status = "failed"
                crawl_task.error_info = error
                if task_id in self.active_tasks:
                    del self.active_tasks[task_id]
    
    def release_task_slot(self):
        """
        釋放任務槽位，允許執行下一個任務
        """
        self.task_slots.release()
    
    def is_queue_empty(self) -> bool:
        """
        檢查隊列是否為空
        
        Returns:
            隊列是否為空
        """
        return self.task_queue.empty()
