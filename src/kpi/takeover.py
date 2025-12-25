"""
接管统计KPI
统计自动驾驶过程中的人工接管次数
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, MergeStrategy, StreamingData
from ..data_loader.bag_reader import MessageAccessor


@dataclass
class TakeoverEvent:
    """接管事件"""
    timestamp: float
    duration_before_takeover: float  # 接管前自动驾驶持续时间
    is_valid: bool = True  # 是否为有效接管（自动驾驶 >= 1s）
    
    
class TakeoverKPI(BaseKPI):
    """接管统计KPI"""
    
    @property
    def name(self) -> str:
        return "接管统计"
    
    @property
    def required_topics(self) -> List[str]:
        return ["/function/function_manager"]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]  # 依赖里程统计的结果
    
    @property
    def provides(self) -> List[str]:
        return ["接管次数", "接管率"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        # 有效接管的最小自动驾驶持续时间（秒）
        self.min_auto_duration = self.config.get('kpi', {}).get(
            'takeover', {}).get('min_auto_duration', 1.0)
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算接管统计KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _detect_takeovers(self, 
                          states: List[bool],
                          timestamps: List[float]) -> List[TakeoverEvent]:
        """
        检测接管事件
        
        接管：operator_type 从自动驾驶(True)变为非自动驾驶(False)
        有效接管：自动驾驶持续时间 >= min_auto_duration (默认 1s)
        """
        events = []
        
        # 找到首次进入自动驾驶的时刻
        first_auto_idx = None
        for i, is_auto in enumerate(states):
            if is_auto:
                first_auto_idx = i
                break
        
        if first_auto_idx is None:
            return events
        
        # 从首次进入自动驾驶后开始检测
        auto_start_time = None
        
        for i in range(first_auto_idx, len(states)):
            is_auto = states[i]
            
            if is_auto and auto_start_time is None:
                # 进入自动驾驶
                auto_start_time = timestamps[i]
            
            elif not is_auto and auto_start_time is not None:
                # 退出自动驾驶 (发生接管)
                duration = timestamps[i] - auto_start_time
                
                # 判断是否为有效接管
                is_valid = duration >= self.min_auto_duration
                
                events.append(TakeoverEvent(
                    timestamp=timestamps[i],
                    duration_before_takeover=duration,
                    is_valid=is_valid
                ))
                
                auto_start_time = None
        
        return events
    
    def _compute_auto_time(self, states: List[bool], timestamps: List[float]) -> float:
        """计算自动驾驶总时间，排除 bag 间隔"""
        auto_time = 0.0
        max_gap = 1.0  # 最大允许时间间隔（秒）
        
        for i in range(1, len(states)):
            if states[i - 1]:  # 上一帧是自动驾驶
                dt = timestamps[i] - timestamps[i - 1]
                # 过滤 bag 间隔
                if 0 < dt <= max_gap:
                    auto_time += dt
        
        return auto_time
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="总接管次数",
            value=0,
            unit="次",
            description="数据不足，无法统计接管次数"
        ))
        self.add_result(KPIResult(
            name="每百公里接管次数",
            value=0.0,
            unit="次/百公里",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="平均接管里程",
            value=0.0,
            unit="km/次",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集驾驶状态数据（流式模式）
        
        Args:
            synced_frames: 同步后的帧列表
            streaming_data: 中间数据容器
        """
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            if fm_msg is None:
                continue
            
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            if operator_type is None:
                continue
            
            is_auto = operator_type == self.auto_operator_type
            # 存储: (is_auto, timestamp)
            streaming_data.auto_states.append((is_auto, frame.timestamp))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算接管统计（流式模式）
        
        Args:
            streaming_data: 汇总后的中间数据
            
        Returns:
            KPI结果列表
        """
        self.clear_results()
        
        if len(streaming_data.auto_states) < 2:
            self._add_empty_results()
            return self.get_results()
        
        # 提取数据
        states = [s[0] for s in streaming_data.auto_states]
        timestamps = [s[1] for s in streaming_data.auto_states]
        
        # 检测接管事件
        takeover_events = self._detect_takeovers(states, timestamps)
        
        # 区分有效和无效接管
        valid_events = [e for e in takeover_events if e.is_valid]
        invalid_events = [e for e in takeover_events if not e.is_valid]
        
        total_valid_takeovers = len(valid_events)
        total_all_takeovers = len(takeover_events)
        
        # 计算自动驾驶总时间
        auto_time = self._compute_auto_time(states, timestamps)
        
        # 获取自动驾驶里程（从 kwargs 或通过 streaming_data 计算）
        auto_mileage_km = kwargs.get('auto_mileage_km', 0)
        
        # 获取 bag 时间映射器
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 计算比率指标
        takeover_per_100km = (total_valid_takeovers / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        avg_takeover_mileage = (auto_mileage_km / total_valid_takeovers) if total_valid_takeovers > 0 else float('inf')
        
        # 计算平均接管间隔
        if len(valid_events) > 1:
            intervals = []
            for i in range(1, len(valid_events)):
                intervals.append(valid_events[i].timestamp - valid_events[i-1].timestamp)
            avg_interval = np.mean(intervals)
        else:
            avg_interval = auto_time if total_valid_takeovers <= 1 else 0
        
        # 添加结果
        takeover_result = KPIResult(
            name="总接管次数",
            value=total_valid_takeovers,
            unit="次",
            description=f"有效接管次数（自动驾驶持续≥{self.min_auto_duration}s后被接管）",
            details={
                'valid_count': total_valid_takeovers,
                'invalid_count': len(invalid_events),
                'total_count': total_all_takeovers,
                'min_auto_duration': self.min_auto_duration
            }
        )
        
        # 添加溯源记录
        for i, event in enumerate(valid_events):
            takeover_result.add_anomaly(
                timestamp=event.timestamp,
                bag_name=bag_mapper.get_bag_name(event.timestamp),
                description=f"有效接管 #{i+1}：自动驾驶持续 {event.duration_before_takeover:.1f}s 后被接管",
                value=event.duration_before_takeover
            )
        
        self.add_result(takeover_result)
        
        self.add_result(KPIResult(
            name="每百公里接管次数",
            value=round(takeover_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的接管次数",
            details={'auto_mileage_km': round(auto_mileage_km, 3)}
        ))
        
        self.add_result(KPIResult(
            name="平均接管里程",
            value="∞" if total_valid_takeovers == 0 else round(avg_takeover_mileage, 3),
            unit="km/次",
            description="平均每多少公里自动驾驶里程接管一次"
        ))
        
        self.add_result(KPIResult(
            name="平均接管间隔",
            value=round(avg_interval / 60, 2),
            unit="min",
            description="两次接管之间的平均时间间隔"
        ))
        
        self.add_result(KPIResult(
            name="自动驾驶总时间",
            value=round(auto_time / 60, 2),
            unit="min",
            description="自动驾驶模式的累计时间"
        ))
        
        return self.get_results()
