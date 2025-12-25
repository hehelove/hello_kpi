"""
里程统计KPI
计算自动驾驶和非自动驾驶的里程与时间
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

from .base_kpi import BaseKPI, KPIResult, MergeStrategy, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.geo import haversine_distance


@dataclass
class MileageSegment:
    """里程段信息"""
    start_time: float
    end_time: float
    distance: float  # 米
    is_auto_driving: bool
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    duration: float = 0.0  # 实际累计时间（秒），排除间隔


class MileageKPI(BaseKPI):
    """里程统计KPI"""
    
    @property
    def name(self) -> str:
        return "里程统计"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/localization/localization"
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return []  # 无依赖，是基础 KPI
    
    @property
    def provides(self) -> List[str]:
        return ["总里程", "自动驾驶里程", "非自动驾驶里程"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 异常数据阈值
        self.min_lat = -90.0
        self.max_lat = 90.0
        self.min_lon = -180.0
        self.max_lon = 180.0
        self.max_speed = 200.0  # km/h, 用于剔除异常跳变
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """
        计算里程统计KPI
        
        通过流式模式实现，复用 collect() + compute_from_collected() 逻辑
        """
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _add_mileage_results(self, auto_distance: float, manual_distance: float,
                             auto_time: float, manual_time: float):
        """添加里程统计结果（内部方法，避免重复代码）"""
        total_distance = auto_distance + manual_distance
        total_time = auto_time + manual_time
        
        self.add_result(KPIResult(
            name="总里程",
            value=round(total_distance / 1000, 3),
            unit="km",
            description="总行驶里程",
            merge_strategy=MergeStrategy.SUM,
            details={
                'total_distance_m': total_distance,
                'total_time_s': total_time,
                'total_time_min': total_time / 60
            }
        ))
        
        self.add_result(KPIResult(
            name="自动驾驶里程",
            value=round(auto_distance / 1000, 3),
            unit="km",
            description="自动驾驶模式下的行驶里程",
            merge_strategy=MergeStrategy.SUM,
            details={
                'distance_m': auto_distance,
                'time_s': auto_time,
                'time_min': auto_time / 60,
                'percentage': round(auto_distance / total_distance * 100, 2) if total_distance > 0 else 0
            }
        ))
        
        self.add_result(KPIResult(
            name="非自动驾驶里程",
            value=round(manual_distance / 1000, 3),
            unit="km",
            description="非自动驾驶模式下的行驶里程",
            merge_strategy=MergeStrategy.SUM,
            details={
                'distance_m': manual_distance,
                'time_s': manual_time,
                'time_min': manual_time / 60,
                'percentage': round(manual_distance / total_distance * 100, 2) if total_distance > 0 else 0
            }
        ))
        
        self.add_result(KPIResult(
            name="自动驾驶时间",
            value=round(auto_time / 60, 2),
            unit="min",
            description="自动驾驶模式的持续时间",
            merge_strategy=MergeStrategy.SUM
        ))
        
        self.add_result(KPIResult(
            name="非自动驾驶时间",
            value=round(manual_time / 60, 2),
            unit="min",
            description="非自动驾驶模式的持续时间",
            merge_strategy=MergeStrategy.SUM
        ))
        
        # 计算平均速度
        if auto_time > 0:
            auto_avg_speed = (auto_distance / 1000) / (auto_time / 3600)
            self.add_result(KPIResult(
                name="自动驾驶平均速度",
                value=round(auto_avg_speed, 2),
                unit="km/h",
                description="自动驾驶模式下的平均速度",
                merge_strategy=MergeStrategy.WEIGHTED_AVG
            ))
    
    def _is_valid_position(self, lat: float, lon: float) -> bool:
        """检查位置是否有效"""
        if lat == 0 and lon == 0:
            return False
        if not (self.min_lat <= lat <= self.max_lat):
            return False
        if not (self.min_lon <= lon <= self.max_lon):
            return False
        return True
    
    def _compute_segments(self, 
                          positions: List[tuple],
                          auto_states: List[bool],
                          timestamps: List[float]) -> List[MileageSegment]:
        """
        计算里程段
        
        在驾驶状态变化或遇到时间间隔（bag间隔）时切分 segment，
        确保时间和里程计算不受 bag 间隔影响。
        """
        segments = []
        max_gap = 1.0  # 最大允许时间间隔（秒）
        
        current_segment_start = 0
        current_is_auto = auto_states[0]
        segment_distance = 0.0
        segment_time = 0.0
        valid_points = 0
        last_valid_idx = 0  # 上一个有效点的索引
        
        for i in range(1, len(positions)):
            dt = timestamps[i] - timestamps[i - 1]
            
            # 检查是否需要切分 segment：驾驶状态变化 或 时间间隔过大
            should_split = (
                auto_states[i] != current_is_auto or  # 驾驶状态变化
                dt > max_gap  # 时间间隔过大（bag 间隔）
            )
            
            # 如果需要切分，先保存当前 segment
            if should_split and valid_points > 0:
                segments.append(MileageSegment(
                    start_time=timestamps[current_segment_start],
                    end_time=timestamps[last_valid_idx],
                    distance=segment_distance,
                    is_auto_driving=current_is_auto,
                    start_lat=positions[current_segment_start][0],
                    start_lon=positions[current_segment_start][1],
                    end_lat=positions[last_valid_idx][0],
                    end_lon=positions[last_valid_idx][1],
                    duration=segment_time
                ))
                # 重置
                segment_distance = 0.0
                segment_time = 0.0
                valid_points = 0
                
            if should_split:
                # 开始新的 segment
                current_segment_start = i
                current_is_auto = auto_states[i]
            
            # 累加当前点到前一点的距离和时间（仅在非间隔时）
            if 0 < dt <= max_gap:
                lat1, lon1 = positions[i - 1]
                lat2, lon2 = positions[i]
                dist = haversine_distance(lat1, lon1, lat2, lon2)
                speed = dist / dt * 3.6  # km/h
                
                if speed <= self.max_speed:
                    segment_distance += dist
                    segment_time += dt
                    valid_points += 1
                    last_valid_idx = i
        
        # 保存最后一个 segment
        if valid_points > 0:
            segments.append(MileageSegment(
                start_time=timestamps[current_segment_start],
                end_time=timestamps[last_valid_idx],
                distance=segment_distance,
                is_auto_driving=current_is_auto,
                start_lat=positions[current_segment_start][0],
                start_lon=positions[current_segment_start][1],
                end_lat=positions[last_valid_idx][0],
                end_lon=positions[last_valid_idx][1],
                duration=segment_time
            ))
        
        return segments
    
    def _add_empty_results(self):
        """添加空结果 (数据不足时)"""
        self.add_result(KPIResult(
            name="总里程",
            value=0.0,
            unit="km",
            description="数据不足，无法计算里程"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集位置和驾驶状态数据（流式模式）
        
        Args:
            synced_frames: 同步后的帧列表
            streaming_data: 中间数据容器
        """
        for frame in synced_frames:
            if not frame.valid:
                continue
            
            loc_msg = frame.messages.get("/localization/localization")
            fm_msg = frame.messages.get("/function/function_manager")
            
            if loc_msg is None or fm_msg is None:
                continue
            
            lat = MessageAccessor.get_field(
                loc_msg, "global_localization.position.latitude")
            lon = MessageAccessor.get_field(
                loc_msg, "global_localization.position.longitude")
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            
            if lat is None or lon is None or operator_type is None:
                continue
            
            if not self._is_valid_position(lat, lon):
                continue
            
            is_auto = operator_type == self.auto_operator_type
            # 存储: (lat, lon, is_auto, timestamp)
            streaming_data.positions.append((lat, lon, is_auto, frame.timestamp))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算里程统计（流式模式）
        
        Args:
            streaming_data: 汇总后的中间数据
            
        Returns:
            KPI结果列表
        """
        self.clear_results()
        
        if len(streaming_data.positions) < 2:
            self._add_empty_results()
            return self.get_results()
        
        # 提取数据
        positions = [(p[0], p[1]) for p in streaming_data.positions]
        auto_states = [p[2] for p in streaming_data.positions]
        timestamps = [p[3] for p in streaming_data.positions]
        
        # 计算里程（复用现有逻辑）
        segments = self._compute_segments(positions, auto_states, timestamps)
        
        # 汇总统计
        auto_distance = sum(s.distance for s in segments if s.is_auto_driving)
        manual_distance = sum(s.distance for s in segments if not s.is_auto_driving)
        auto_time = sum(s.duration for s in segments if s.is_auto_driving)
        manual_time = sum(s.duration for s in segments if not s.is_auto_driving)
        
        # 添加结果（复用内部方法）
        self._add_mileage_results(auto_distance, manual_distance, auto_time, manual_time)
        
        return self.get_results()

