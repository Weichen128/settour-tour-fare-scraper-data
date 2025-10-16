"""
JSON解析器模組

此模組提供了一個專門用於解析東南旅遊 GraphQL API 返回的 JSON 數據的解析器類別。
該類別負責從原始 API 響應中提取和結構化機票資料。

主要功能：
- 解析 API 返回的 JSON 響應數據
- 提取航班基本信息，如航班編號、日期、價格等
- 提取航段資料，包括去程和回程
- 提取票價和稅金信息
- 將提取的數據轉換為結構化的內部數據模型
- 支持將去程和回程數據進行組合匹配

依賴項：
- dataclasses: 用於創建數據類
- typing: 用於類型提示
- json: 用於處理 JSON 數據
"""

import json
import traceback
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from models.flight_info import FlightInfo
from models.flight_segment import FlightSegment
from utils.log_manager import LogManager
from config.config_manager import ConfigManager

class JsonParser:
    """
    JSON解析器類別
    
    負責解析從東南旅遊 GraphQL API 獲取的 JSON 數據，
    提取去程航班信息，並能將回程數據與去程數據結合，處理一對多的關係。
    
    屬性：
        log_manager (LogManager): 日誌管理器實例
        config_manager (ConfigManager): 配置管理器實例
        api_response (Dict[str, Any]): API 返回的原始 JSON 數據
        structured_data (List[FlightInfo]): 結構化後的航班數據列表 (通常是去程航班)
    """
    
    def __init__(self, log_manager: LogManager, config_manager: ConfigManager):
        """
        初始化 JSON 解析器
        
        參數：
            log_manager (LogManager): 日誌管理器實例
            config_manager (ConfigManager): 配置管理器實例
        """
        self.log_manager = log_manager
        self.config_manager = config_manager
        self.api_response = None
        self.structured_data = []
    
    def parse_api_response(self, json_data: Dict[str, Any]) -> bool:
        """
        解析 API 返回的 JSON 數據 (主要用於去程)
        
        參數：
            json_data (Dict[str, Any]): API 返回的 JSON 數據
            
        返回：
            bool: 解析是否成功
        """
        try:
            self.api_response = json_data
            self.structured_data = []
            
            # 確認 API 響應包含有效數據
            if not json_data:
                self.log_manager.log_error("API 響應無效: 空的響應數據")
                return False
                
            if 'data' not in json_data:
                self.log_manager.log_error("API 響應無效: 缺少 'data' 欄位")
                self.log_manager.log_debug(f"API 響應: {json.dumps(json_data)[:200]}...")
                return False
                
            # 檢查 pfpFlightSegmentSearch 欄位是否存在
            if 'pfpFlightSegmentSearch' not in json_data['data']:
                self.log_manager.log_error("API 響應無效: 缺少 'data.pfpFlightSegmentSearch' 欄位")
                self.log_manager.log_debug(f"API 響應的 data 欄位: {json.dumps(json_data['data'])[:200]}...")
                return False
                
            flight_search_data = json_data['data']['pfpFlightSegmentSearch']
            
            # 檢查是否有錯誤
            if flight_search_data.get('error'):
                error_msg = flight_search_data['error']
                self.log_manager.log_error(f"API 響應包含錯誤: {error_msg}")
                return False
            
            # 檢查 data 欄位是否存在    
            if 'data' not in flight_search_data:
                self.log_manager.log_error("API 響應無效: 缺少 'data.pfpFlightSegmentSearch.data' 欄位")
                self.log_manager.log_debug(f"flight_search_data: {json.dumps(flight_search_data)[:200]}...")
                return False
                
            # 檢查 flightList 欄位是否存在
            if 'flightList' not in flight_search_data['data']:
                self.log_manager.log_error("API 響應無效: 缺少 'data.pfpFlightSegmentSearch.data.flightList' 欄位")
                self.log_manager.log_debug(f"flight_search_data.data: {json.dumps(flight_search_data['data'])[:200]}...")
                return False
                
            # 獲取航班列表
            flight_list = flight_search_data['data']['flightList']
            
            if not flight_list:
                self.log_manager.log_info("API 響應中沒有航班數據")
                return True
                
            # 處理每個航班數據
            for i, flight_item in enumerate(flight_list):
                try:
                    flight_info = self.extract_outbound_flight_data(flight_item)
                    if flight_info:  # 確保提取成功
                        self.structured_data.append(flight_info)
                    else:
                        self.log_manager.log_warning(f"處理第 {i+1} 個去程航班項目失敗或缺少關鍵信息，已跳過")
                except Exception as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"處理第 {i+1} 個航班項目時發生錯誤: {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"航班項目數據: {json.dumps(flight_item)[:500]}...")
                    # 繼續處理下一個航班項目
                    continue
                
            self.log_manager.log_debug(f"成功解析 {len(self.structured_data)}/{len(flight_list)} 個航班項目")
            return True
            
        except KeyError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析 API 響應時發生鍵錯誤: 缺少必要欄位 '{str(e)}'")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return False
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析 API 響應時發生類型錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return False
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析 API 響應時發生未預期錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return False
    
    def extract_outbound_flight_data(self, flight_item: Dict[str, Any]) -> Optional[FlightInfo]:
        """
        從航班項目數據中提取去程航班信息，包括 searchId
        
        參數：
            flight_item (Dict[str, Any]): 包含單個去程航班信息的字典
            
        返回：
            Optional[FlightInfo]: 結構化的去程航班信息對象，如果提取失敗則返回 None
        """
        # 提取基本航班信息
        try:
            flight_basic_info = self._extract_flight_info(flight_item)
        except KeyError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"提取航班基本信息時缺少必要欄位: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"提取航班基本信息時資料類型錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
        except ValueError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"提取航班基本信息時資料值無效: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
        
        # 提取航段數據
        try:
            outbound_segments = self._extract_segment_data(flight_item)
            if not outbound_segments:
                self.log_manager.log_warning(f"提取去程航段失敗或為空")
                return None
        except KeyError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"提取航段數據時缺少必要欄位: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"提取航段數據時資料類型錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
        
        # 提取 searchId 和票價信息（取第一個票價）
        fare_info = None
        search_id = None
        if flight_item.get('fareList'):
            if len(flight_item['fareList']) > 0:
                fare_item = flight_item['fareList'][0]
                try:
                    fare_info = self._extract_fare_info(fare_item)
                    search_id = fare_item.get('searchId')
                    if not search_id:
                        self.log_manager.log_warning(f"航班項目缺少 searchId，無法用於查詢回程")
                        return None
                except KeyError as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"提取票價信息或 searchId 時缺少必要欄位: {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"票價項目數據: {json.dumps(fare_item)[:500]}...")
                    return None
                except TypeError as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"提取票價信息或 searchId 時資料類型錯誤: {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"票價項目數據: {json.dumps(fare_item)[:500]}...")
                    return None
                
            else:
                self.log_manager.log_warning(f"航班項目的 fareList 為空，無法獲取 searchId")
                return None
        else:
            self.log_manager.log_warning(f"航班項目缺少 fareList 欄位，無法獲取 searchId")
            return None
            
        # 解析日期
        departure_date = None
        try:
            departure_date = self._parse_date(flight_basic_info.get('departure_date', ''))
            if not departure_date:
                self.log_manager.log_warning(f"無法解析出發日期: '{flight_basic_info.get('departure_date', '')}'")
                return None
        except ValueError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析出發日期時日期格式錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            self.log_manager.log_debug(f"日期字串: '{flight_basic_info.get('departure_date', '')}'")
            return None
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析出發日期時資料類型錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            self.log_manager.log_debug(f"日期字串: '{flight_basic_info.get('departure_date', '')}'")
            return None
        
        # 創建 FlightInfo 對象
        try:
            flight_info = FlightInfo(
                departure_date=departure_date,
                return_date=None,  # 回程日期在處理回程時設定
                price=0.0,  # 暫時設為 0，最終價格由回程響應決定
                tax=0.0,    # 暫時設為 0，最終稅金由回程響應決定
                outbound_segments=outbound_segments,
                inbound_segments=[],  # 回程航段在處理回程時設定
                search_id=search_id  # 保存 searchId 以便後續查詢回程
            )
            return flight_info
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"創建 FlightInfo 對象時參數類型錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            self.log_manager.log_debug(f"參數值: departure_date={departure_date}, outbound_segments={outbound_segments}, search_id={search_id}")
            return None
    
    def parse_inbound_response(self, json_data: Dict[str, Any], outbound_flight_info: FlightInfo) -> List[FlightInfo]:
        """
        解析回程 API 響應並與提供的去程信息結合，生成完整的航班資訊列表。
        處理一個去程對應多個回程的情況。

        參數：
            json_data (Dict[str, Any]): 回程 API 返回的 JSON 數據
            outbound_flight_info (FlightInfo): 對應的去程航班資訊

        返回：
            List[FlightInfo]: 包含完整去回程資訊的航班列表 (每個元素是一個去回程組合)
        """
        complete_flights = []
        
        # 確認去程資訊有效
        if not outbound_flight_info or not outbound_flight_info.search_id:
            self.log_manager.log_error("缺少有效的去程航班信息或 search_id，無法處理回程響應")
            return []
            
        search_id_for_log = outbound_flight_info.search_id

        try:
            # 檢查 API 響應結構
            if not json_data:
                self.log_manager.log_error(f"回程 API 響應無效: 空的響應數據 (searchId: {search_id_for_log})")
                return []
                
            if 'data' not in json_data:
                self.log_manager.log_error(f"回程 API 響應無效: 缺少 'data' 欄位 (searchId: {search_id_for_log})")
                self.log_manager.log_debug(f"API 響應: {json.dumps(json_data)[:200]}...")
                return []
                
            if 'pfpFlightSegmentSearch' not in json_data['data']:
                self.log_manager.log_error(f"回程 API 響應無效: 缺少 'data.pfpFlightSegmentSearch' 欄位 (searchId: {search_id_for_log})")
                self.log_manager.log_debug(f"API 響應的 data 欄位: {json.dumps(json_data['data'])[:200]}...")
                return []
                
            flight_search_data = json_data['data']['pfpFlightSegmentSearch']
            
            if flight_search_data.get('error'):
                error_msg = flight_search_data['error']
                self.log_manager.log_error(f"回程 API 響應包含錯誤 (searchId: {search_id_for_log}): {error_msg}")
                return []
            
            if 'data' not in flight_search_data:
                self.log_manager.log_error(f"回程 API 響應無效: 缺少 'data.pfpFlightSegmentSearch.data' 欄位 (searchId: {search_id_for_log})")
                self.log_manager.log_debug(f"flight_search_data: {json.dumps(flight_search_data)[:200]}...")
                return []
                
            if 'flightList' not in flight_search_data['data']:
                self.log_manager.log_error(f"回程 API 響應無效: 缺少 'data.pfpFlightSegmentSearch.data.flightList' 欄位 (searchId: {search_id_for_log})")
                self.log_manager.log_debug(f"flight_search_data.data: {json.dumps(flight_search_data['data'])[:200]}...")
                return []
                
            # 獲取回程航班列表
            inbound_flight_list = flight_search_data['data']['flightList']
            
            # 新增回程航班列表長度的日誌，便於調試
            self.log_manager.log_debug(f"回程航班列表長度: {len(inbound_flight_list)} (searchId: {search_id_for_log})")
            
            if not inbound_flight_list:
                self.log_manager.log_info(f"回程 API 響應中沒有航班數據 (searchId: {search_id_for_log})")
                return []
                
            # 記錄所有回程航班號，便於調試
            flight_nums_summary = []
            for item in inbound_flight_list:
                flight_details = item.get('flightDetail', [])
                flight_nums = [detail.get('flightNumber') for detail in flight_details if 'flightNumber' in detail]
                if flight_nums:
                    flight_nums_summary.append(", ".join(flight_nums))
            
            if flight_nums_summary:
                self.log_manager.log_debug(f"所有回程航班號: {'; '.join(flight_nums_summary)} (searchId: {search_id_for_log})")
                
            # 處理每個回程航班選項
            for i, inbound_flight_item in enumerate(inbound_flight_list):
                try:
                    # 提取回程基本信息
                    inbound_basic_info = self._extract_flight_info(inbound_flight_item)
                    
                    # 提取回程日期
                    return_date = self._parse_date(inbound_basic_info.get('departure_date', ''))
                    if not return_date:
                        self.log_manager.log_warning(f"無法解析回程日期 (searchId: {search_id_for_log}, 項目: {i+1})")
                        continue
                    
                    # 提取回程航段
                    inbound_segments = self._extract_segment_data(inbound_flight_item)
                    if not inbound_segments:
                        self.log_manager.log_warning(f"提取回程航段失敗或為空 (searchId: {search_id_for_log}, 項目: {i+1})")
                        continue
                    
                    # 提取票價信息
                    fare_info = None
                    if inbound_flight_item.get('fareList') and len(inbound_flight_item['fareList']) > 0:
                        try:
                            fare_info = self._extract_fare_info(inbound_flight_item['fareList'][0])
                        except KeyError as e:
                            self.log_manager.log_error(f"提取回程票價信息時缺少必要欄位 (searchId: {search_id_for_log}, 項目: {i+1}): {str(e)}")
                            # 可以繼續而不中斷，票價設為0
                        except TypeError as e:
                            self.log_manager.log_error(f"提取回程票價信息時資料類型錯誤 (searchId: {search_id_for_log}, 項目: {i+1}): {str(e)}")
                            # 可以繼續而不中斷，票價設為0
                    
                    # 創建新的 FlightInfo 對象，複製去程資訊並添加回程資訊
                    try:
                        complete_flight = FlightInfo(
                            departure_date=outbound_flight_info.departure_date,  # 從去程複製
                            return_date=return_date,  # 從回程提取
                            price=fare_info.get('price', 0.0) if fare_info else 0.0,  # 從回程提取總價
                            tax=fare_info.get('tax', 0.0) if fare_info else 0.0,  # 從回程提取稅金
                            outbound_segments=list(outbound_flight_info.outbound_segments),  # 複製去程航段
                            inbound_segments=inbound_segments,  # 從回程提取
                            search_id=outbound_flight_info.search_id  # 保留原始 searchId
                        )
                        complete_flights.append(complete_flight)
                        
                        # 記錄航班詳情
                        self.log_manager.log_debug(f"成功創建去回程組合: 去程航班={', '.join([s.flight_number for s in outbound_flight_info.outbound_segments if s.flight_number])}, "
                                                f"回程航班={', '.join([s.flight_number for s in inbound_segments if s.flight_number])}, "
                                                f"回程日期={return_date}, 總價={complete_flight.price} (searchId: {search_id_for_log})")
                        
                    except TypeError as e:
                        stack_trace = traceback.format_exc()
                        self.log_manager.log_error(f"創建 FlightInfo 組合對象時參數類型錯誤 (searchId: {search_id_for_log}, 項目: {i+1}): {str(e)}")
                        self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                        continue
                        
                except KeyError as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"處理第 {i+1} 個回程航班項目時缺少必要欄位 (searchId: {search_id_for_log}): {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"回程航班項目數據: {json.dumps(inbound_flight_item)[:500]}...")
                    continue
                except TypeError as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"處理第 {i+1} 個回程航班項目時資料類型錯誤 (searchId: {search_id_for_log}): {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"回程航班項目數據: {json.dumps(inbound_flight_item)[:500]}...")
                    continue
                except ValueError as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"處理第 {i+1} 個回程航班項目時資料值無效 (searchId: {search_id_for_log}): {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"回程航班項目數據: {json.dumps(inbound_flight_item)[:500]}...")
                    continue
            
            self.log_manager.log_debug(f"成功為 searchId {search_id_for_log} 生成 {len(complete_flights)} 個有效的去回程組合")
            return complete_flights
            
        except KeyError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析回程響應時缺少必要欄位 (searchId: {search_id_for_log}): '{str(e)}'")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return []
        except TypeError as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析回程響應時資料類型錯誤 (searchId: {search_id_for_log}): {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return []
    
    def get_structured_data(self) -> List[FlightInfo]:
        """
        獲取結構化後的航班數據列表 (通常是去程資訊)
        
        返回：
            List[FlightInfo]: 結構化的航班數據列表
        """
        return self.structured_data
    
    def _extract_flight_info(self, flight_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取航班基本信息
        
        參數：
            flight_data (Dict[str, Any]): 航班數據字典
            
        返回：
            Dict[str, Any]: 提取的航班基本信息
        """
        flight_info = {}
        
        try:
            # 檢查必要欄位是否存在
            required_fields = ['depAirportCode', 'arrAirportCode', 'depDate', 'depTime', 'arrDate', 'arrTime']
            missing_fields = [field for field in required_fields if field not in flight_data]
            
            if missing_fields:
                self.log_manager.log_warning(f"航班數據缺少必要欄位: {', '.join(missing_fields)}")
            
            # 提取主要航班信息
            flight_info['departure_airport'] = flight_data.get('depAirportCode', '')
            if not flight_info['departure_airport']:
                self.log_manager.log_warning("缺少出發機場代碼")
                
            flight_info['arrival_airport'] = flight_data.get('arrAirportCode', '')
            if not flight_info['arrival_airport']:
                self.log_manager.log_warning("缺少到達機場代碼")
                
            flight_info['departure_date'] = flight_data.get('depDate', '')
            if not flight_info['departure_date']:
                self.log_manager.log_warning("缺少出發日期")
                
            flight_info['departure_time'] = flight_data.get('depTime', '')
            if not flight_info['departure_time']:
                self.log_manager.log_warning("缺少出發時間")
                
            flight_info['arrival_date'] = flight_data.get('arrDate', '')
            if not flight_info['arrival_date']:
                self.log_manager.log_warning("缺少到達日期")
                
            flight_info['arrival_time'] = flight_data.get('arrTime', '')
            if not flight_info['arrival_time']:
                self.log_manager.log_warning("缺少到達時間")
                
            flight_info['flight_time'] = flight_data.get('flyTime', 0)
            
        except KeyError as e:
            self.log_manager.log_error(f"提取航班基本信息時缺少關鍵欄位: {str(e)}")
            raise
        except TypeError as e:
            self.log_manager.log_error(f"提取航班基本信息時類型錯誤: {str(e)}")
            raise
        except Exception as e:
            self.log_manager.log_error(f"提取航班基本信息時發生未預期錯誤: {str(e)}")
            raise
        
        return flight_info
    
    def _extract_fare_info(self, fare_item: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取票價信息
        
        參數：
            fare_item (Dict[str, Any]): 票價數據字典
            
        返回：
            Dict[str, Any]: 提取的票價信息
        """
        fare_info = {}
        
        try:
            # 檢查必要欄位是否存在
            if 'pfpClassName' not in fare_item:
                self.log_manager.log_warning("票價數據缺少艙等名稱欄位")
                
            if 'fareInfo' not in fare_item:
                self.log_manager.log_warning("票價數據缺少價格資訊欄位")
                return fare_info
            
            # 提取價格數據
            price_data = self._extract_price_data(fare_item.get('fareInfo', {}))
            fare_info.update(price_data)
            
        except KeyError as e:
            self.log_manager.log_error(f"提取票價信息時缺少關鍵欄位: {str(e)}")
            raise
        except TypeError as e:
            self.log_manager.log_error(f"提取票價信息時類型錯誤: {str(e)}")
            raise
        except Exception as e:
            self.log_manager.log_error(f"提取票價信息時發生未預期錯誤: {str(e)}")
            raise
        
        return fare_info
    
    def _extract_segment_data(self, segment_data: Dict[str, Any]) -> List[FlightSegment]:
        """
        提取航段數據 (適用於去程和回程)
        
        參數：
            segment_data (Dict[str, Any]): 航段數據字典
            
        返回：
            List[FlightSegment]: 提取的航段對象列表
        """
        segments = []
        
        try:
            # 檢查必要欄位是否存在
            if 'flightDetail' not in segment_data:
                self.log_manager.log_warning("航段數據缺少 flightDetail 欄位")
                return segments
                
            cabin_class = ''
            # 從票價信息中獲取艙等
            if segment_data.get('fareList') and len(segment_data['fareList']) > 0:
                first_fare = segment_data['fareList'][0]
                class_name = first_fare.get('pfpClassName', '').split('、') if first_fare.get('pfpClassName') else ''
                class_type = first_fare.get('bccTp', '').split('、') if first_fare.get('bccTp') else ''

            for i, detail in enumerate(segment_data['flightDetail']):
                try:
                    # 檢查航班號是否存在
                    if 'flightNumber' not in detail:
                        self.log_manager.log_warning(f"第 {i+1} 個航段詳情缺少航班號")
                        continue

                    cabin_class = f"{class_name[i]}{class_type[i]}"
                        
                    segment = FlightSegment(
                        flight_number=f"{detail.get('marketingAirlineCode', '')}{detail.get('flightNumber', '')}",
                        cabin_class=cabin_class
                    )
                    segments.append(segment)
                except Exception as e:
                    stack_trace = traceback.format_exc()
                    self.log_manager.log_error(f"處理第 {i+1} 個航段詳情時發生錯誤: {str(e)}")
                    self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
                    self.log_manager.log_debug(f"航段詳情數據: {json.dumps(detail)[:200]}...")
                    continue
                    
        except KeyError as e:
            self.log_manager.log_error(f"提取航段數據時缺少關鍵欄位: {str(e)}")
            raise
        except TypeError as e:
            self.log_manager.log_error(f"提取航段數據時類型錯誤: {str(e)}")
            raise
        except Exception as e:
            self.log_manager.log_error(f"提取航段數據時發生未預期錯誤: {str(e)}")
            raise
        
        return segments
    
    def _extract_price_data(self, fare_info: Dict[str, Any]) -> Dict[str, float]:
        """
        提取價格和稅金數據
        
        參數：
            fare_info (Dict[str, Any]): 票價信息字典
            
        返回：
            Dict[str, float]: 包含價格和稅金的字典
        """
        price_data = {
            'price': 0.0,
            'tax': 0.0
        }
        
        try:
            if not fare_info:
                self.log_manager.log_warning("票價資訊為空")
                return price_data
                
            # 檢查總價欄位是否存在
            if 'totalPrice' not in fare_info:
                self.log_manager.log_warning("票價資訊缺少總價欄位")
            else:
                if 'price' not in fare_info['totalPrice']:
                    self.log_manager.log_warning("票價資訊的總價欄位缺少價格值")
                else:
                    try:
                        total_price = float(fare_info['totalPrice'].get('price', 0)) # 此為含稅價格
                    except (ValueError, TypeError) as e:
                        self.log_manager.log_error(f"轉換總價格為浮點數時發生錯誤: {str(e)}")
                        self.log_manager.log_debug(f"總價格值: '{fare_info['totalPrice'].get('price')}'")
            
            # 檢查稅金欄位是否存在
            if 'tax' not in fare_info:
                self.log_manager.log_warning("票價資訊缺少稅金欄位")
            else:
                if 'totalTax' not in fare_info['tax']:
                    self.log_manager.log_warning("票價資訊的稅金欄位缺少總稅金值")
                else:
                    try:
                        tax = float(fare_info['tax'].get('totalTax', 0)) # 此為稅金
                    except (ValueError, TypeError) as e:
                        self.log_manager.log_error(f"轉換稅金為浮點數時發生錯誤: {str(e)}")
                        self.log_manager.log_debug(f"稅金值: '{fare_info['tax'].get('totalTax')}'")

            price_data['price'] = total_price - tax # 此為未稅價格
            price_data['tax'] = tax
        
        except KeyError as e:
            self.log_manager.log_error(f"提取價格數據時缺少關鍵欄位: {str(e)}")
            raise
        except TypeError as e:
            self.log_manager.log_error(f"提取價格數據時類型錯誤: {str(e)}")
            raise
        except Exception as e:
            self.log_manager.log_error(f"提取價格數據時發生未預期錯誤: {str(e)}")
            raise
        
        return price_data
        
    def _parse_date(self, date_str: str) -> Optional[datetime.date]:
        """
        將日期字符串解析為日期對象
        
        參數：
            date_str (str): 格式為 'YYYYMMDD' 的日期字符串
            
        返回：
            Optional[datetime.date]: 解析後的日期對象，如果解析失敗則返回 None
        """
        try:
            if not date_str:
                self.log_manager.log_warning("日期字符串為空")
                return None
                
            if len(date_str) != 8:
                self.log_manager.log_warning(f"日期字符串長度不正確: '{date_str}', 應為8個字符 (YYYYMMDD)")
                return None
                
            # 檢查日期字符串是否只包含數字
            if not date_str.isdigit():
                self.log_manager.log_warning(f"日期字符串包含非數字字符: '{date_str}'")
                return None
                
            return datetime.strptime(date_str, '%Y%m%d').date()
            
        except ValueError as e:
            self.log_manager.log_error(f"日期格式錯誤: '{date_str}', {str(e)}")
            return None
        except Exception as e:
            stack_trace = traceback.format_exc()
            self.log_manager.log_error(f"解析日期時發生未預期錯誤: {str(e)}")
            self.log_manager.log_debug(f"錯誤詳情: {stack_trace}")
            return None
