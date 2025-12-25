"""
超速检测KPI
计算自车速度超过道路限速115%且速度>50kph的里程及次数
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

from .base_kpi import BaseKPI, KPIResult, StreamingData, BagTimeMapper
from ..data_loader.bag_reader import MessageAccessor
from ..utils.geo import haversine_distance
from ..constants import KPINames


@dataclass
class SpeedingEvent:
    """超速事件"""
    start_time: float
    end_time: float
    max_speed: float  # km/h
    speed_limit: float  # km/h
    distance: float  # 米
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float


class SpeedingKPI(BaseKPI):
    """超速检测KPI"""
    
    @property
    def name(self) -> str:
        return KPINames.SPEEDING
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/localization/localization",
            "/planning/trajectory",
            "/vehicle/chassis_domain_report"
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return [KPINames.MILEAGE]
    
    @property
    def provides(self) -> List[str]:
        return ["超速次数", "超速里程", "超速频率"]
    
    @property
    def supports_streaming(self) -> bool:
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 超速判定阈值
        self.speed_threshold_ratio = 1.15  # 道路限速的115%
        self.min_speed_threshold = 50.0  # 最低速度阈值 km/h
        self.min_event_duration = 1.0  # 最短事件持续时间（秒）
        
        # 自动驾驶状态
        self.auto_operator_type = 2
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算超速检测KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集超速数据（流式模式）
        """
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            loc_msg = frame.messages.get("/localization/localization")
            traj_msg = frame.messages.get("/planning/trajectory")
            chassis_msg = frame.messages.get("/vehicle/chassis_domain_report")
            
            if fm_msg is None or loc_msg is None:
                continue
            
            # 检查是否自动驾驶
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            is_auto = operator_type == self.auto_operator_type
            
            # 获取当前速度 (已经是 km/h)
            speed_kph = None
            if chassis_msg is not None:
                speed_kph = MessageAccessor.get_field(
                    chassis_msg, "motion_system.vehicle_speed", None)
            
            if speed_kph is None:
                # 备用：从定位获取速度 (m/s -> km/h)
                speed_mps = MessageAccessor.get_field(
                    loc_msg, "global_localization.velocity.x", None)
                if speed_mps is not None:
                    speed_kph = abs(speed_mps) * 3.6
            
            if speed_kph is None:
                continue
            
            speed_kph = abs(speed_kph)
            
            # 获取道路限速 (已经是 km/h)
            speed_limit = None
            if traj_msg is not None:
                speed_limit = MessageAccessor.get_field(
                    traj_msg, "speed_limit.speed_limit_parsa", None)
            
            # 获取位置
            lat = MessageAccessor.get_field(
                loc_msg, "global_localization.position.latitude", None)
            lon = MessageAccessor.get_field(
                loc_msg, "global_localization.position.longitude", None)
            
            if lat is None or lon is None:
                continue
            
            # 存储数据: (speed, speed_limit, lat, lon, is_auto, timestamp)
            streaming_data.speeding_data.append((
                speed_kph,
                speed_limit,  # 可能为 None
                lat,
                lon,
                is_auto,
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算超速检测KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.speeding_data) < 10:
            self._add_empty_results()
            return self.get_results()
        
        # 排序
        streaming_data.speeding_data.sort(key=lambda x: x[5])
        
        # 获取自动驾驶里程
        auto_mileage_km = kwargs.get('auto_mileage_km', 0)
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 检测超速事件
        speeding_events = []
        total_speeding_distance = 0.0
        
        current_event = None
        prev_data = None
        
        for data in streaming_data.speeding_data:
            speed, speed_limit, lat, lon, is_auto, timestamp = data
            
            # 只检测自动驾驶状态
            if not is_auto:
                # 结束当前事件
                if current_event is not None:
                    if current_event['end_time'] - current_event['start_time'] >= self.min_event_duration:
                        speeding_events.append(SpeedingEvent(
                            start_time=current_event['start_time'],
                            end_time=current_event['end_time'],
                            max_speed=current_event['max_speed'],
                            speed_limit=current_event['speed_limit'],
                            distance=current_event['distance'],
                            start_lat=current_event['start_lat'],
                            start_lon=current_event['start_lon'],
                            end_lat=current_event['end_lat'],
                            end_lon=current_event['end_lon']
                        ))
                    current_event = None
                prev_data = None
                continue
            
            # 判断是否超速
            is_speeding = False
            if speed_limit is not None and speed_limit > 0:
                # 速度 > 限速的115% 且 速度 > 50 km/h
                if speed > speed_limit * self.speed_threshold_ratio and speed > self.min_speed_threshold:
                    is_speeding = True
            
            if is_speeding:
                if current_event is None:
                    # 开始新事件
                    current_event = {
                        'start_time': timestamp,
                        'end_time': timestamp,
                        'max_speed': speed,
                        'speed_limit': speed_limit,
                        'distance': 0.0,
                        'start_lat': lat,
                        'start_lon': lon,
                        'end_lat': lat,
                        'end_lon': lon
                    }
                else:
                    # 更新事件
                    current_event['end_time'] = timestamp
                    current_event['max_speed'] = max(current_event['max_speed'], speed)
                    current_event['end_lat'] = lat
                    current_event['end_lon'] = lon
                    
                    # 计算距离增量
                    if prev_data is not None:
                        prev_lat, prev_lon = prev_data[2], prev_data[3]
                        dist = haversine_distance(prev_lat, prev_lon, lat, lon)
                        if dist < 100:  # 合理范围内
                            current_event['distance'] += dist
            else:
                # 结束当前事件
                if current_event is not None:
                    if current_event['end_time'] - current_event['start_time'] >= self.min_event_duration:
                        speeding_events.append(SpeedingEvent(
                            start_time=current_event['start_time'],
                            end_time=current_event['end_time'],
                            max_speed=current_event['max_speed'],
                            speed_limit=current_event['speed_limit'],
                            distance=current_event['distance'],
                            start_lat=current_event['start_lat'],
                            start_lon=current_event['start_lon'],
                            end_lat=current_event['end_lat'],
                            end_lon=current_event['end_lon']
                        ))
                    current_event = None
            
            prev_data = data
        
        # 处理最后一个事件
        if current_event is not None:
            if current_event['end_time'] - current_event['start_time'] >= self.min_event_duration:
                speeding_events.append(SpeedingEvent(
                    start_time=current_event['start_time'],
                    end_time=current_event['end_time'],
                    max_speed=current_event['max_speed'],
                    speed_limit=current_event['speed_limit'],
                    distance=current_event['distance'],
                    start_lat=current_event['start_lat'],
                    start_lon=current_event['start_lon'],
                    end_lat=current_event['end_lat'],
                    end_lon=current_event['end_lon']
                ))
        
        # 统计
        speeding_count = len(speeding_events)
        total_speeding_distance = sum(e.distance for e in speeding_events)
        speeding_distance_km = total_speeding_distance / 1000.0
        
        # 每百公里超速次数
        speeding_per_100km = (speeding_count / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        
        # 平均超速里程
        avg_speeding_mileage = (auto_mileage_km / speeding_count) if speeding_count > 0 else float('inf')
        
        # 最大超速比例
        max_speed_ratio = 0.0
        if speeding_events:
            max_speed_ratio = max((e.max_speed / e.speed_limit - 1) * 100 
                                  for e in speeding_events if e.speed_limit > 0)
        
        # 添加结果
        self.add_result(KPIResult(
            name="超速次数",
            value=speeding_count,
            unit="次",
            description=f"自动驾驶过程中超速事件数（速度>限速115%且>50kph）",
            details={
                'events': [
                    {
                        'start_time': e.start_time,
                        'end_time': e.end_time,
                        'duration': e.end_time - e.start_time,
                        'max_speed': e.max_speed,
                        'speed_limit': e.speed_limit,
                        'distance': e.distance,
                        'bag_name': bag_mapper.get_bag_name(e.start_time)
                    }
                    for e in speeding_events[:20]  # 最多记录20个
                ]
            }
        ))
        
        self.add_result(KPIResult(
            name="超速里程",
            value=round(speeding_distance_km, 3),
            unit="km",
            description="超速状态下行驶的总里程"
        ))
        
        self.add_result(KPIResult(
            name="超速频率",
            value=round(speeding_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的超速次数"
        ))
        
        self.add_result(KPIResult(
            name="平均超速里程",
            value=round(avg_speeding_mileage, 3) if avg_speeding_mileage != float('inf') else "∞",
            unit="km/次",
            description="平均每次超速之间的里程"
        ))
        
        if max_speed_ratio > 0:
            self.add_result(KPIResult(
                name="最大超速比例",
                value=round(max_speed_ratio, 1),
                unit="%",
                description="最严重超速事件中超过限速的比例"
            ))
        
        return self.get_results()
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="超速次数",
            value=0,
            unit="次",
            description="数据不足，无法检测超速"
        ))
        self.add_result(KPIResult(
            name="超速里程",
            value=0.0,
            unit="km",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="超速频率",
            value=0.0,
            unit="次/百公里",
            description="数据不足"
        ))
