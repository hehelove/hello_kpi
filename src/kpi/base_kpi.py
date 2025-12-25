"""
KPI计算基类
定义KPI计算的通用接口和数据结构
支持普通模式和流式模式（收集中间数据后统一计算）
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
import numpy as np
import yaml


class BagTimeMapper:
    """
    根据时间戳查找对应的 bag 文件名
    用于异常事件溯源
    """
    
    def __init__(self, bag_infos: List = None):
        """
        初始化
        
        Args:
            bag_infos: BagInfo 列表，每个包含 path, start_time, end_time
        """
        self.bag_infos = bag_infos or []
    
    def get_bag_name(self, timestamp: float) -> str:
        """
        根据时间戳获取 bag 文件名
        
        Args:
            timestamp: 时间戳
            
        Returns:
            bag 文件名，找不到则返回空字符串
        """
        if not self.bag_infos:
            return ""
        
        for info in self.bag_infos:
            if info.start_time <= timestamp <= info.end_time:
                return info.path.name if isinstance(info.path, Path) else str(info.path)
        
        # 如果不在任何 bag 范围内，返回最近的
        min_dist = float('inf')
        closest_name = ""
        for info in self.bag_infos:
            dist = min(abs(timestamp - info.start_time), abs(timestamp - info.end_time))
            if dist < min_dist:
                min_dist = dist
                closest_name = info.path.name if isinstance(info.path, Path) else str(info.path)
        
        return closest_name


class MergeStrategy(Enum):
    """KPI 结果合并策略"""
    SUM = "sum"           # 求和（如：里程、次数）
    AVERAGE = "average"   # 平均（如：舒适度评分）
    WEIGHTED_AVG = "weighted_avg"  # 加权平均（按时长加权）
    MAX = "max"           # 取最大值
    MIN = "min"           # 取最小值
    LAST = "last"         # 取最后一个
    CONCAT = "concat"     # 连接列表
    CUSTOM = "custom"     # 自定义合并


@dataclass
class AnomalyRecord:
    """异常数据记录（用于溯源）"""
    timestamp: float          # 发生时间戳
    frame_idx: int = -1       # 帧索引（-1 表示未知）
    bag_name: str = ""        # 来源 bag 文件名
    description: str = ""     # 异常描述
    value: Any = None         # 异常值
    threshold: Any = None     # 阈值（用于对比）
    end_timestamp: float = 0  # 事件结束时间戳（用于连续帧事件）
    frame_count: int = 1      # 连续帧数量
    peak_value: Any = None    # 峰值（连续帧中的最大/最小值）
    
    def to_dict(self) -> Dict:
        result = {
            'timestamp': self.timestamp,
            'frame_idx': self.frame_idx,
            'bag_name': self.bag_name,
            'description': self.description,
            'value': self.value,
            'threshold': self.threshold
        }
        if self.end_timestamp > 0:
            result['end_timestamp'] = self.end_timestamp
            result['duration'] = round(self.end_timestamp - self.timestamp, 3)
        if self.frame_count > 1:
            result['frame_count'] = self.frame_count
        if self.peak_value is not None:
            result['peak_value'] = self.peak_value
        return result


def merge_consecutive_events(
    indices: List[int],
    timestamps: List[float],
    values: List[float],
    bag_mapper: 'BagTimeMapper',
    description_template: str,
    threshold: float,
    max_gap_frames: int = 3,
    max_gap_sec: float = 0.5,
    max_events: int = 50
) -> List[AnomalyRecord]:
    """
    合并连续帧的事件为单个事件
    
    Args:
        indices: 超限帧的索引列表
        timestamps: 所有帧的时间戳列表
        values: 所有帧的值列表
        bag_mapper: bag 时间映射器
        description_template: 描述模板，使用 {value}, {peak}, {count}, {duration} 占位符
        threshold: 阈值
        max_gap_frames: 允许的最大帧间隔（超过则认为是新事件）
        max_gap_sec: 允许的最大时间间隔（超过则认为是新事件，如接管导致的中断）
        max_events: 最大返回事件数
        
    Returns:
        合并后的 AnomalyRecord 列表
    """
    if len(indices) == 0:
        return []
    
    events = []
    
    # 将连续的索引分组（同时检查帧间隔和时间间隔）
    groups = []
    current_group = [indices[0]]
    
    for i in range(1, len(indices)):
        prev_idx = indices[i-1]
        curr_idx = indices[i]
        # 检查帧间隔
        frame_gap_ok = (curr_idx - prev_idx) <= max_gap_frames
        # 检查时间间隔（防止接管等导致的时间跳变）
        time_gap_ok = (timestamps[curr_idx] - timestamps[prev_idx]) <= max_gap_sec
        
        if frame_gap_ok and time_gap_ok:
            current_group.append(curr_idx)
        else:
            groups.append(current_group)
            current_group = [curr_idx]
    groups.append(current_group)
    
    # 为每组创建一个事件
    for group in groups[:max_events]:
        start_idx = group[0]
        end_idx = group[-1]
        
        # 计算这组帧中的峰值（绝对值最大）
        group_values = [abs(values[idx]) for idx in group]
        peak_idx = group[group_values.index(max(group_values))]
        peak_value = values[peak_idx]
        
        # 平均值
        avg_value = sum(values[idx] for idx in group) / len(group)
        
        start_ts = timestamps[start_idx]
        end_ts = timestamps[end_idx]
        
        # 计算实际有效持续时间（排除大间隔）
        if len(group) > 1:
            group_timestamps = [timestamps[idx] for idx in group]
            ts_diffs = [group_timestamps[j+1] - group_timestamps[j] for j in range(len(group_timestamps)-1)]
            # 只累加正常间隔
            valid_diffs = [d for d in ts_diffs if d <= max_gap_sec]
            duration = sum(valid_diffs)
        else:
            duration = 0.0
        
        # 格式化描述
        description = description_template.format(
            value=f"{avg_value:.2f}",
            peak=f"{peak_value:.2f}",
            count=len(group),
            duration=f"{duration:.2f}"
        )
        
        events.append(AnomalyRecord(
            timestamp=start_ts,
            bag_name=bag_mapper.get_bag_name(start_ts),
            description=description,
            value=float(avg_value),
            peak_value=float(peak_value),
            threshold=threshold,
            end_timestamp=end_ts if duration > 0 else 0,
            frame_count=len(group)
        ))
    
    return events


@dataclass
class KPIResult:
    """KPI计算结果"""
    name: str
    value: Any
    unit: str = ""
    description: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    merge_strategy: MergeStrategy = MergeStrategy.SUM  # 合并策略
    weight: float = 1.0  # 权重（用于加权平均）
    anomalies: List['AnomalyRecord'] = field(default_factory=list)  # 异常记录（用于溯源）
    
    def add_anomaly(self, timestamp: float, description: str, 
                    value: Any = None, threshold: Any = None,
                    frame_idx: int = -1, bag_name: str = ""):
        """添加异常记录"""
        self.anomalies.append(AnomalyRecord(
            timestamp=timestamp,
            frame_idx=frame_idx,
            bag_name=bag_name,
            description=description,
            value=value,
            threshold=threshold
        ))
    
    def to_dict(self) -> Dict:
        result = {
            'name': self.name,
            'value': self.value,
            'unit': self.unit,
            'description': self.description,
            'details': self.details
        }
        if self.anomalies:
            result['anomalies'] = [a.to_dict() for a in self.anomalies]
        return result
    
    def __str__(self) -> str:
        if self.unit:
            return f"{self.name}: {self.value} {self.unit}"
        return f"{self.name}: {self.value}"


def merge_kpi_results(results_list: List[List[KPIResult]]) -> List[KPIResult]:
    """
    合并多个 bag 的 KPI 结果
    
    Args:
        results_list: [[bag1_results], [bag2_results], ...]
        
    Returns:
        合并后的结果列表
    """
    if not results_list:
        return []
    
    if len(results_list) == 1:
        return results_list[0]
    
    # 按 name 分组
    grouped: Dict[str, List[KPIResult]] = {}
    for results in results_list:
        for r in results:
            if r.name not in grouped:
                grouped[r.name] = []
            grouped[r.name].append(r)
    
    # 合并每个指标
    merged = []
    for name, items in grouped.items():
        if len(items) == 1:
            merged.append(items[0])
            continue
        
        strategy = items[0].merge_strategy
        unit = items[0].unit
        description = items[0].description
        
        if strategy == MergeStrategy.SUM:
            # 求和
            total = sum(r.value for r in items if isinstance(r.value, (int, float)))
            merged.append(KPIResult(
                name=name, value=round(total, 4), unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'sum'}
            ))
        
        elif strategy == MergeStrategy.AVERAGE:
            # 简单平均
            values = [r.value for r in items if isinstance(r.value, (int, float))]
            avg = sum(values) / len(values) if values else 0
            merged.append(KPIResult(
                name=name, value=round(avg, 4), unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'average'}
            ))
        
        elif strategy == MergeStrategy.WEIGHTED_AVG:
            # 加权平均（按 weight 加权，通常是时长）
            total_weight = sum(r.weight for r in items)
            if total_weight > 0:
                weighted_sum = sum(r.value * r.weight for r in items 
                                 if isinstance(r.value, (int, float)))
                avg = weighted_sum / total_weight
            else:
                avg = 0
            merged.append(KPIResult(
                name=name, value=round(avg, 4), unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'weighted_avg',
                        'total_weight': total_weight}
            ))
        
        elif strategy == MergeStrategy.MAX:
            max_val = max(r.value for r in items if isinstance(r.value, (int, float)))
            merged.append(KPIResult(
                name=name, value=max_val, unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'max'}
            ))
        
        elif strategy == MergeStrategy.MIN:
            min_val = min(r.value for r in items if isinstance(r.value, (int, float)))
            merged.append(KPIResult(
                name=name, value=min_val, unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'min'}
            ))
        
        elif strategy == MergeStrategy.LAST:
            merged.append(items[-1])
        
        elif strategy == MergeStrategy.CONCAT:
            # 连接列表类型的值
            all_values = []
            for r in items:
                if isinstance(r.value, list):
                    all_values.extend(r.value)
                else:
                    all_values.append(r.value)
            merged.append(KPIResult(
                name=name, value=all_values, unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'concat'}
            ))
        
        else:
            # CUSTOM 或未知策略，使用求和
            total = sum(r.value for r in items if isinstance(r.value, (int, float)))
            merged.append(KPIResult(
                name=name, value=round(total, 4), unit=unit,
                description=description,
                details={'merged_from': len(items), 'strategy': 'sum_default'}
            ))
    
    return merged


@dataclass
class StreamingData:
    """
    流式处理的中间数据容器
    
    用于收集多个 bag 的原始数据，最后统一计算 KPI。
    这样可以保证流式模式与普通模式计算结果一致。
    """
    # ========== 里程统计 ==========
    # 位置序列 [(lat, lon, is_auto, timestamp), ...]
    positions: List[tuple] = field(default_factory=list)
    
    # ========== 接管统计 ==========
    # 驾驶状态序列 [(is_auto, timestamp), ...]
    auto_states: List[tuple] = field(default_factory=list)
    
    # ========== 舒适性 ==========
    # 加速度数据 [(lon_acc, lat_acc, speed, timestamp), ...]
    accelerations: List[tuple] = field(default_factory=list)
    
    # ========== 车道保持 ==========
    # 横向偏差数据 [(lateral_error, kappa, timestamp), ...]
    lateral_errors: List[tuple] = field(default_factory=list)
    
    # ========== 转向平滑度 ==========
    # 转向角数据 [(steering_angle, speed, timestamp), ...]
    steering_data: List[tuple] = field(default_factory=list)
    
    # ========== 曲率 ==========
    # 曲率数据 [(kappa, distance, timestamp), ...]
    curvature_data: List[tuple] = field(default_factory=list)
    
    # ========== 定位 ==========
    # 定位数据 [(status, stddev_east, stddev_north, timestamp), ...]
    localization_data: List[tuple] = field(default_factory=list)
    
    # ========== 画龙检测 ==========
    # 画龙数据 [(lateral_error, timestamp), ...]
    weaving_data: List[tuple] = field(default_factory=list)
    
    # ========== ROI障碍物 ==========
    # ROI数据 [(obstacle_info_dict, timestamp), ...]
    roi_data: List[tuple] = field(default_factory=list)
    # ROI可视化数据 (采样帧，用于生成示例图)
    roi_viz_data: List = field(default_factory=list)
    # TTC危险事件完整数据 (用于生成危险场景图)
    ttc_danger_points: List = field(default_factory=list)
    # 危险距离帧数据 (距离 < 3m 的帧，用于生成危险距离场景图)
    danger_distance_frames: List = field(default_factory=list)
    
    # ========== 紧急事件 ==========
    # 紧急事件数据 [(event_type, value, timestamp), ...]
    emergency_data: List[tuple] = field(default_factory=list)
    
    # ========== 超速检测 ==========
    # 超速数据 [(speed, speed_limit, lat, lon, is_auto, timestamp), ...]
    speeding_data: List[tuple] = field(default_factory=list)
    
    # ========== bag 信息 ==========
    bag_infos: List = field(default_factory=list)
    
    # ========== 控制调试数据 ==========
    # {timestamp: {key: value}}
    parsed_debug_data: Dict = field(default_factory=dict)
    
    def merge(self, other: 'StreamingData'):
        """合并另一个 StreamingData 的数据"""
        self.positions.extend(other.positions)
        self.auto_states.extend(other.auto_states)
        self.accelerations.extend(other.accelerations)
        self.lateral_errors.extend(other.lateral_errors)
        self.steering_data.extend(other.steering_data)
        self.curvature_data.extend(other.curvature_data)
        self.localization_data.extend(other.localization_data)
        self.weaving_data.extend(other.weaving_data)
        self.roi_data.extend(other.roi_data)
        self.roi_viz_data.extend(other.roi_viz_data)
        self.ttc_danger_points.extend(other.ttc_danger_points)
        self.danger_distance_frames.extend(other.danger_distance_frames)
        self.emergency_data.extend(other.emergency_data)
        self.speeding_data.extend(other.speeding_data)
        self.bag_infos.extend(other.bag_infos)
        self.parsed_debug_data.update(other.parsed_debug_data)
    
    def sort_by_timestamp(self):
        """按时间戳排序所有数据"""
        # 各数据列表的时间戳位置不同，需要分别排序
        # positions: (lat, lon, is_auto, timestamp) - 索引 3
        if self.positions:
            self.positions.sort(key=lambda x: x[3])
        # auto_states: (is_auto, timestamp) - 索引 1
        if self.auto_states:
            self.auto_states.sort(key=lambda x: x[1])
        # accelerations: (lon_acc, lat_acc, speed, timestamp) - 索引 3
        if self.accelerations:
            self.accelerations.sort(key=lambda x: x[3])
        # lateral_errors: (lateral_error, kappa, timestamp) - 索引 2
        if self.lateral_errors:
            self.lateral_errors.sort(key=lambda x: x[2])
        # steering_data: (steering_angle, steering_velocity, speed, timestamp) - 索引 3
        if self.steering_data:
            self.steering_data.sort(key=lambda x: x[3])
        # curvature_data: (kappa, distance, timestamp) - 索引 2
        if self.curvature_data:
            self.curvature_data.sort(key=lambda x: x[2])
        # localization_data: (status, stddev_east, stddev_north, timestamp) - 索引 3
        if self.localization_data:
            self.localization_data.sort(key=lambda x: x[3])
        # weaving_data: (steering_vel, lat_acc, lat_error, loc_reliable, speed, timestamp) - 索引 5
        if self.weaving_data:
            self.weaving_data.sort(key=lambda x: x[5])
        # roi_data: (near_total, near_static, near_moving, ..., timestamp) - 索引 13
        if self.roi_data:
            self.roi_data.sort(key=lambda x: x[13])
        # emergency_data: (lon_acc, steering_vel, speed, timestamp) - 索引 3
        if self.emergency_data:
            self.emergency_data.sort(key=lambda x: x[3])
        # speeding_data: (speed, speed_limit, lat, lon, is_auto, timestamp) - 索引 5
        if self.speeding_data:
            self.speeding_data.sort(key=lambda x: x[5])
    
    def clear(self):
        """清空所有数据"""
        self.positions.clear()
        self.auto_states.clear()
        self.accelerations.clear()
        self.lateral_errors.clear()
        self.steering_data.clear()
        self.curvature_data.clear()
        self.localization_data.clear()
        self.weaving_data.clear()
        self.roi_data.clear()
        self.roi_viz_data.clear()
        self.ttc_danger_points.clear()
        self.danger_distance_frames.clear()
        self.emergency_data.clear()
        self.speeding_data.clear()
        self.bag_infos.clear()
        self.parsed_debug_data.clear()
    
    def get_stats(self) -> Dict:
        """获取数据统计信息"""
        return {
            'positions': len(self.positions),
            'auto_states': len(self.auto_states),
            'accelerations': len(self.accelerations),
            'lateral_errors': len(self.lateral_errors),
            'steering_data': len(self.steering_data),
            'curvature_data': len(self.curvature_data),
            'localization_data': len(self.localization_data),
            'weaving_data': len(self.weaving_data),
            'roi_data': len(self.roi_data),
            'emergency_data': len(self.emergency_data),
            'bags': len(self.bag_infos),
            'debug_timestamps': len(self.parsed_debug_data)
        }


class BaseKPI(ABC):
    """
    KPI计算基类
    
    支持两种计算模式：
    1. 普通模式：直接调用 compute(synced_frames)
    2. 流式模式：先调用 collect(synced_frames) 收集数据，最后调用 compute_from_collected(streaming_data)
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        初始化KPI计算器
        
        Args:
            config: 配置字典
        """
        self.config = config or {}
        self._results: List[KPIResult] = []
        # 通用配置：自动驾驶状态值
        self.auto_operator_type = self.config.get('kpi', {}).get(
            'auto_driving', {}).get('operator_type_value', 2)
    
    @property
    @abstractmethod
    def name(self) -> str:
        """KPI名称"""
        pass
    
    @property
    def required_topics(self) -> List[str]:
        """需要的topic列表"""
        return []
    
    @property
    def dependencies(self) -> List[str]:
        """
        依赖的其他 KPI 名称列表
        声明此 KPI 需要其他 KPI 的计算结果
        """
        return []
    
    @property
    def provides(self) -> List[str]:
        """
        此 KPI 提供的结果名称列表
        用于依赖检查
        """
        return []
    
    @property
    def supports_streaming(self) -> bool:
        """
        是否支持流式收集模式
        如果为 True，则可以使用 collect() + compute_from_collected() 流程
        """
        return False  # 默认不支持，子类需要显式启用
    
    @abstractmethod
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """
        计算KPI（普通模式）
        
        Args:
            synced_frames: 同步后的帧列表
            **kwargs: 额外参数
            
        Returns:
            KPI结果列表
        """
        pass
    
    def _compute_via_streaming(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """
        通过流式模式计算 KPI（模板方法）
        
        支持流式模式的 KPI 可以在 compute() 中调用此方法，
        从而复用 collect() + compute_from_collected() 的逻辑，
        避免 compute() 和 collect() 之间的代码重复。
        
        Args:
            synced_frames: 同步后的帧列表
            **kwargs: 额外参数
            
        Returns:
            KPI结果列表
        """
        # 创建临时的 StreamingData
        streaming_data = StreamingData()
        # 如果有 bag_infos，传递给 streaming_data
        bag_infos = kwargs.get('bag_infos', [])
        if bag_infos:
            streaming_data.bag_infos = list(bag_infos)
        # 收集数据
        self.collect(synced_frames, streaming_data, **kwargs)
        # 从收集的数据计算
        return self.compute_from_collected(streaming_data, **kwargs)
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集中间数据（流式模式）
        
        子类需要重写此方法来收集原始数据到 streaming_data 中。
        
        Args:
            synced_frames: 同步后的帧列表
            streaming_data: 中间数据容器
            **kwargs: 额外参数
        """
        # 默认实现：不收集任何数据
        pass
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的中间数据计算KPI（流式模式）
        
        子类需要重写此方法来从汇总的数据计算最终指标。
        
        Args:
            streaming_data: 汇总后的中间数据
            **kwargs: 额外参数
            
        Returns:
            KPI结果列表
        """
        # 默认实现：返回空结果
        return []
    
    def get_results(self) -> List[KPIResult]:
        """获取所有计算结果"""
        return self._results
    
    def add_result(self, result: KPIResult):
        """添加一个结果"""
        self._results.append(result)
    
    def clear_results(self):
        """清除结果"""
        self._results.clear()
    
    @staticmethod
    def load_config(config_path: str) -> Dict:
        """加载配置文件"""
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)


class KPIRegistry:
    """KPI注册表"""
    
    _registry: Dict[str, type] = {}
    
    @classmethod
    def register(cls, kpi_class: type):
        """注册一个KPI类"""
        cls._registry[kpi_class.__name__] = kpi_class
        return kpi_class
    
    @classmethod
    def get(cls, name: str) -> Optional[type]:
        """获取KPI类"""
        return cls._registry.get(name)
    
    @classmethod
    def list_all(cls) -> List[str]:
        """列出所有已注册的KPI"""
        return list(cls._registry.keys())
    
    @classmethod
    def create_all(cls, config: Optional[Dict] = None) -> List[BaseKPI]:
        """创建所有已注册的KPI实例"""
        return [kpi_cls(config) for kpi_cls in cls._registry.values()]

