"""
API客戶端模組

此模組提供了一個專門用於與東南旅遊 GraphQL API 進行互動的客戶端類別。
該類別負責處理所有的 API 請求、響應處理和錯誤處理邏輯。

主要功能：
- 初始化和管理 HTTP 會話
- 發送 GraphQL 查詢請求
- 處理 API 響應和錯誤
- 提供日誌記錄功能

依賴項：
- requests: 用於發送 HTTP 請求
- typing: 用於類型提示
"""

import requests
import json
import time
from typing import Dict, Any, Optional
from config.config_manager import ConfigManager
from utils.log_manager import LogManager

class ApiClient:
    """
    API客戶端類別
    
    負責與東南旅遊的 GraphQL API 進行互動，處理所有的 API 請求、
    響應和錯誤處理邏輯。
    
    屬性：
        session (requests.Session): HTTP 會話實例
        config_manager (ConfigManager): 配置管理器實例
        log_manager (LogManager): 日誌管理器實例
        headers (Dict[str, str]): API 請求標頭
    """
    
    def __init__(self, config_manager: ConfigManager, log_manager: LogManager):
        """
        初始化 API 客戶端
        
        參數：
            config_manager (ConfigManager): 配置管理器實例
            log_manager (LogManager): 日誌管理器實例
        """
        self.session = None
        self.config_manager = config_manager
        self.log_manager = log_manager
        self.headers = {}
        self.api_config = config_manager.get_api_config()
        self.retry_config = config_manager.get_retry_config()
        self.initialize_session()
    
    def initialize_session(self) -> None:
        """
        初始化 HTTP 會話
        
        設置請求標頭、超時和其他會話相關配置。
        包括設置必要的認證信息和 API 特定的標頭。
        """
        self.session = requests.Session()
        
        # 從配置中獲取標頭
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': '*/*'
        }
        
        # 如果配置中有定義標頭，使用配置中的標頭
        if 'headers' in self.api_config and isinstance(self.api_config['headers'], dict):
            self.headers.update(self.api_config['headers'])
        
        # 確保必要的標頭存在
        if 'User-Agent' not in self.headers and 'user_agent' in self.api_config:
            self.headers['User-Agent'] = self.api_config['user_agent']
            
        if 'Origin' not in self.headers and 'origin' in self.api_config:
            self.headers['Origin'] = self.api_config['origin']
            
        if 'Referer' not in self.headers and 'referer' in self.api_config:
            self.headers['Referer'] = self.api_config['referer']
        
        # 加入認證信息（如果有）
        auth_token = self.api_config.get('auth_token')
        if auth_token:
            self.headers['Authorization'] = f'Bearer {auth_token}'
        
        # 設置請求超時
        self.timeout = self.api_config.get('timeout', 30)
        
        self.log_manager.log_debug("API 客戶端會話已初始化，標頭設置為: " + json.dumps(self.headers))
    
    def send_graphql_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        發送 GraphQL 請求到 API 端點
        
        參數：
            payload (Dict[str, Any]): GraphQL 查詢參數
            
        返回：
            Dict[str, Any]: API 響應數據
            
        異常：
            requests.RequestException: 當請求失敗時拋出
            ValueError: 當響應格式無效時拋出
        """
        if not self.session:
            self.initialize_session()
        
        # 從配置中獲取端點 URL
        endpoint_url = self.api_config.get('endpoint_url')
        if not endpoint_url:
            error_msg = "API 端點 URL 未設置"
            self.log_manager.log_error(error_msg, ValueError(error_msg))
            raise ValueError(error_msg)
        
        self.log_manager.log_debug(f"發送 GraphQL 請求到: {endpoint_url}")
        
        # 從配置中獲取重試設置
        max_retries = self.retry_config.get('max_attempts', 3)
        retry_delay = self.retry_config.get('interval', 1)
        backoff_factor = self.retry_config.get('backoff_factor', 2.0)
        
        for attempt in range(max_retries):
            try:
                response = self.session.post(
                    url=endpoint_url,
                    headers=self.headers,
                    json=payload,
                    timeout=self.timeout
                )
                
                # 記錄請求和響應（如果開啟除錯）
                if self.api_config.get('debug_request', False):
                    debug_reason = f"graphql_request_attempt_{attempt+1}"
                    # 使用 log_manager 記錄請求和響應
                    self.log_manager.log_debug(
                        f"API請求（嘗試 {attempt+1}）- URL: {endpoint_url}, 負載: {json.dumps(payload)[:500]}..."
                    )
                    self.log_manager.log_debug(
                        f"API回應（嘗試 {attempt+1}）- 狀態碼: {response.status_code}, 內容: {response.text[:500]}..."
                    )
                
                # 處理響應
                return self.handle_response(response)
                
            except requests.RequestException as e:
                wait_time = retry_delay * (backoff_factor ** attempt)  # 使用配置的退避因子
                
                self.log_manager.log_error(
                    f"API請求失敗（嘗試 {attempt+1}/{max_retries}）: {str(e)}，等待 {wait_time} 秒後重試", 
                    exception=e
                )
                
                # 如果已達到最大重試次數，則拋出異常
                if attempt == max_retries - 1:
                    self.handle_errors(e)
                    raise
                
                time.sleep(wait_time)
    
    def handle_response(self, response: requests.Response) -> Dict[str, Any]:
        """
        處理 API 響應
        
        參數：
            response (requests.Response): API 響應對象
            
        返回：
            Dict[str, Any]: 處理後的響應數據
            
        異常：
            ValueError: 當響應格式無效時拋出
        """
        # 檢查 HTTP 狀態碼
        if response.status_code != 200:
            error_msg = f"API 請求失敗，狀態碼: {response.status_code}, 內容: {response.text[:200]}"
            self.log_manager.log_error(error_msg, ValueError(error_msg))
            
            # 使用 log_manager 記錄詳細信息
            self.log_manager.log_debug(
                f"API請求詳情 - URL: {response.request.url}, 方法: {response.request.method}, "
                f"請求標頭: {dict(response.request.headers)}, 請求內容: {response.request.body}"
            )
            self.log_manager.log_debug(
                f"API回應詳情 - 狀態碼: {response.status_code}, 回應標頭: {dict(response.headers)}, "
                f"回應內容: {response.text[:1000]}"
            )
            
            raise ValueError(error_msg)
        
        # 嘗試解析 JSON 響應
        try:
            response_data = response.json()
        except json.JSONDecodeError as e:
            error_msg = f"無效的 JSON 響應: {str(e)}, 內容: {response.text[:200]}"
            self.log_manager.log_error(error_msg, e)
            raise ValueError(error_msg)
        
        # 檢查 GraphQL 錯誤
        if "errors" in response_data and response_data["errors"]:
            graphql_errors = response_data["errors"]
            error_msg = f"GraphQL 請求返回錯誤: {json.dumps(graphql_errors)}"
            self.log_manager.log_error(error_msg, ValueError(error_msg))
            
            # 使用 log_manager 記錄錯誤詳情
            self.log_manager.log_debug(
                f"GraphQL錯誤詳情 - 請求: {response.request.body}, 回應: {json.dumps(response_data)[:1000]}"
            )
            
            raise ValueError(error_msg)
        
        # 確保數據存在
        if "data" not in response_data:
            error_msg = "API 響應缺少 'data' 字段"
            self.log_manager.log_error(error_msg, ValueError(error_msg))
            raise ValueError(error_msg)
        
        self.log_manager.log_debug("API 響應處理成功")
        return response_data
    
    def handle_errors(self, exception: Exception) -> None:
        """
        處理請求過程中的錯誤
        
        參數：
            exception (Exception): 捕獲的異常
            
        異常：
            requests.RequestException: 當需要重試請求時拋出
        """
        error_msg = f"API 請求處理過程中發生錯誤: {str(exception)}"
        self.log_manager.log_error(error_msg, exception)
        
        # 根據錯誤類型進行不同處理
        if isinstance(exception, requests.Timeout):
            self.log_manager.log_error("API 請求超時", exception)
        elif isinstance(exception, requests.ConnectionError):
            self.log_manager.log_error("API 連接錯誤", exception)
        elif isinstance(exception, requests.HTTPError):
            self.log_manager.log_error(f"HTTP 錯誤，狀態碼: {exception.response.status_code}", exception)
        else:
            self.log_manager.log_error(f"未分類錯誤: {str(exception)}", exception)
        
        # 如果有 API 請求和響應數據，記錄它們
        if hasattr(exception, 'request') and hasattr(exception, 'response'):
            request_data = getattr(exception, 'request', None)
            response_data = getattr(exception, 'response', None)
            
            # 使用 log_manager 記錄請求和響應
            if request_data:
                self.log_manager.log_debug(f"出錯的API請求: {request_data}")
            if response_data:
                self.log_manager.log_debug(f"出錯的API回應: {response_data}")
    
    def close_session(self) -> None:
        """
        關閉 HTTP 會話
        
        釋放所有資源並確保會話被正確關閉。
        """
        if self.session:
            self.session.close()
            self.session = None
            self.log_manager.log_info("API 客戶端會話已關閉") 
