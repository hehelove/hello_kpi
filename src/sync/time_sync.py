"""
时间同步模块
基于global_timestamp进行多topic时间对齐
优化版：使用预计算索引和二分查找
"""
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
import numpy as np
from bisect import bisect_left, bisect_right
from concurrent.futures import ThreadPoolExecutor
import time

from ..data_loader.bag_reader import TopicData, MessageAccessor


@dataclass
class SyncedFrame:
    """同步后的一帧数据"""
    timestamp: float  # 基准时间戳
    messages: Dict[str, Any] = field(default_factory=dict)  # topic -> message
    valid: bool = True  # 是否所有必需的topic都找到了数据


@dataclass
class TopicIndex:
    """Topic 索引数据（用于快速查找）"""
    topic_name: str
    global_timestamps: np.ndarray  # 预计算的 global_timestamp 数组
    bag_timestamps: np.ndarray     # 原始 bag 时间戳
    messages: List[Any]            # 消息列表引用
    sorted_indices: np.ndarray     # global_timestamp 排序后的原始索引


class TimeSynchronizer:
    """
    时间同步器（优化版）
    使用预计算索引和二分查找加速同步
    """
    
    # 特殊topic的时间戳字段路径
    SPECIAL_TIMESTAMP_FIELDS = {
        "/planning/trajectory": "planning_header.global_timestamp",
    }
    
    def __init__(self, 
                 base_topic: str = "/function/function_manager",
                 tolerance: float = 0.3,  # 300ms
                 base_frequency: float = 10.0):  # 10Hz
        """
        初始化时间同步器
        
        Args:
            base_topic: 基准topic
            tolerance: 时间容差(秒)
            base_frequency: 基准频率(Hz)
        """
        self.base_topic = base_topic
        self.tolerance = tolerance
        self.base_frequency = base_frequency
        self.base_period = 1.0 / base_frequency
        
        self._topic_data: Dict[str, TopicData] = {}
        self._topic_indices: Dict[str, TopicIndex] = {}  # 预计算索引
        self._index_built = False
    
    def add_topic_data(self, topic_data: Dict[str, TopicData]):
        """添加topic数据用于同步"""
        self._topic_data.update(topic_data)
        self._index_built = False
    
    def _build_indices(self):
        """预计算所有 topic 的 global_timestamp 索引（并行化）"""
        if self._index_built:
            return
        
        start_time = time.time()
        
        def build_single_index(topic: str, data: TopicData) -> Tuple[str, TopicIndex]:
            """构建单个 topic 的索引"""
            n_msgs = len(data.messages)
            global_ts = np.zeros(n_msgs, dtype=np.float64)
            
            # 批量获取 global_timestamp
            ts_field = self.SPECIAL_TIMESTAMP_FIELDS.get(topic, None)
            
            for i, msg in enumerate(data.messages):
                if ts_field:
                    ts = MessageAccessor.get_field(msg, ts_field)
                else:
                    ts = MessageAccessor.get_timestamp(msg)
                global_ts[i] = ts if ts is not None else data.timestamps[i]
            
            # 获取排序索引（按 global_timestamp 排序）
            sorted_indices = np.argsort(global_ts)
            
            return topic, TopicIndex(
                topic_name=topic,
                global_timestamps=global_ts[sorted_indices],  # 已排序
                bag_timestamps=np.array(data.timestamps),
                messages=data.messages,
                sorted_indices=sorted_indices
            )
        
        # 并行构建索引
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [
                executor.submit(build_single_index, topic, data)
                for topic, data in self._topic_data.items()
            ]
            for future in futures:
                topic, index = future.result()
                self._topic_indices[topic] = index
        
        self._index_built = True
        elapsed = time.time() - start_time
        print(f"    [索引] 预计算完成，耗时 {elapsed:.2f}s")
    
    def _get_global_timestamp(self, msg: Any, topic: str = None) -> Optional[float]:
        """获取消息的global_timestamp"""
        if topic and topic in self.SPECIAL_TIMESTAMP_FIELDS:
            field_path = self.SPECIAL_TIMESTAMP_FIELDS[topic]
            return MessageAccessor.get_field(msg, field_path)
        return MessageAccessor.get_timestamp(msg)
    
    def _find_nearest_by_index(self, 
                               topic: str, 
                                target_time: float) -> Optional[Tuple[Any, float]]:
        """
        使用预计算索引进行二分查找
        
        Returns:
            (message, time_diff) 或 None
        """
        if topic not in self._topic_indices:
            return None
        
        index = self._topic_indices[topic]
        timestamps = index.global_timestamps
        n = len(timestamps)
        
        if n == 0:
            return None
        
        # 二分查找
        pos = bisect_left(timestamps, target_time)
        
        # 检查前后两个位置
        candidates = []
        if pos > 0:
            candidates.append(pos - 1)
        if pos < n:
            candidates.append(pos)
        
        if not candidates:
            return None
        
        # 找最接近的
        best_pos = min(candidates, key=lambda p: abs(timestamps[p] - target_time))
        time_diff = abs(timestamps[best_pos] - target_time)
        
        if time_diff <= self.tolerance:
            # 获取原始消息索引
            original_idx = index.sorted_indices[best_pos]
            return index.messages[original_idx], time_diff
        
        return None
    
    def _find_nearest_message(self, 
                               topic: str, 
                               target_time: float) -> Optional[tuple]:
        """查找最接近目标时间的消息（使用 bag 时间戳）"""
        if topic not in self._topic_data:
            return None
        
        data = self._topic_data[topic]
        if topic not in self._topic_indices:
            return None
        
        index = self._topic_indices[topic]
        timestamps = index.bag_timestamps
        
        if len(timestamps) == 0:
            return None
        
        idx = bisect_left(timestamps, target_time)
        
        candidates = []
        if idx > 0:
            candidates.append(idx - 1)
        if idx < len(timestamps):
            candidates.append(idx)
        
        if not candidates:
            return None
        
        best_idx = min(candidates, key=lambda i: abs(timestamps[i] - target_time))
        time_diff = abs(timestamps[best_idx] - target_time)
        
        if time_diff <= self.tolerance:
            return data.messages[best_idx], time_diff
        
        return None
    
    def synchronize(self, 
                    required_topics: Optional[List[str]] = None,
                    use_global_timestamp: bool = True) -> List[SyncedFrame]:
        """
        执行时间同步（优化版）
        
        Args:
            required_topics: 必需的topic列表
            use_global_timestamp: 是否使用global_timestamp进行同步
            
        Returns:
            同步后的帧列表
        """
        if self.base_topic not in self._topic_data:
            raise ValueError(f"Base topic {self.base_topic} not found in data")
        
        # 预计算索引
        self._build_indices()
        
        start_time = time.time()
        
        base_data = self._topic_data[self.base_topic]
        base_index = self._topic_indices[self.base_topic]
        other_topics = [t for t in self._topic_data.keys() if t != self.base_topic]
        required_topics = required_topics or []
        
        n_base = len(base_data.messages)
        synced_frames = []
        
        # 预分配结果列表
        synced_frames = [None] * n_base
        
        # 获取基准时间戳数组
        if use_global_timestamp:
            base_times = base_index.global_timestamps[np.argsort(base_index.sorted_indices)]
        else:
            base_times = base_index.bag_timestamps
        
        # 批量同步
        for i in range(n_base):
            base_time = base_times[i]
            if base_time is None or np.isnan(base_time):
                    continue
            
            frame = SyncedFrame(timestamp=float(base_time))
            frame.messages[self.base_topic] = base_data.messages[i]
            
            # 对齐其他 topic
            for topic in other_topics:
                if use_global_timestamp:
                    result = self._find_nearest_by_index(topic, base_time)
                else:
                    result = self._find_nearest_message(topic, base_time)
                
                if result is not None:
                    msg, _ = result
                    frame.messages[topic] = msg
            
            # 检查必需 topic
            for req_topic in required_topics:
                if req_topic not in frame.messages:
                    frame.valid = False
                    break
            
            synced_frames[i] = frame
        
        # 过滤掉 None
        synced_frames = [f for f in synced_frames if f is not None]
        
        elapsed = time.time() - start_time
        print(f"    [同步] 完成 {len(synced_frames)} 帧，耗时 {elapsed:.2f}s")
        
        return synced_frames
    
    def synchronize_by_timestamps(self, 
                                   target_timestamps: List[float],
                                   topics: List[str]) -> List[SyncedFrame]:
        """根据指定的时间戳列表进行同步"""
        # 确保索引已构建
        self._build_indices()
        
        synced_frames = []
        
        for ts in target_timestamps:
            frame = SyncedFrame(timestamp=ts)
            
            for topic in topics:
                result = self._find_nearest_by_index(topic, ts)
                if result is not None:
                    msg, _ = result
                    frame.messages[topic] = msg
            
            synced_frames.append(frame)
        
        return synced_frames


class HighFreqDataAligner:
    """
    高频数据对齐器
    用于50Hz数据的处理和降采样
    """
    
    def __init__(self, target_frequency: float = 10.0):
        self.target_frequency = target_frequency
        self.target_period = 1.0 / target_frequency
    
    def downsample(self, 
                   topic_data: TopicData,
                   method: str = 'nearest') -> TopicData:
        """降采样到目标频率"""
        if len(topic_data.timestamps) == 0:
            return topic_data
        
        timestamps = np.array(topic_data.timestamps)
        start_time = timestamps[0]
        end_time = timestamps[-1]
        
        target_times = np.arange(start_time, end_time, self.target_period)
        
        result = TopicData(topic_name=topic_data.topic_name)
        
        # 使用 numpy 向量化加速
        indices = np.searchsorted(timestamps, target_times)
        indices = np.clip(indices, 0, len(timestamps) - 1)
        
        # 检查是否需要使用前一个索引
        prev_indices = np.maximum(indices - 1, 0)
        use_prev = np.abs(timestamps[prev_indices] - target_times) < \
                   np.abs(timestamps[indices] - target_times)
        final_indices = np.where(use_prev, prev_indices, indices)
        
        for i, idx in enumerate(final_indices):
            result.append(target_times[i], topic_data.messages[idx])
        
        return result
    
    def interpolate_numeric(self,
                            timestamps: np.ndarray,
                            values: np.ndarray,
                            target_times: np.ndarray) -> np.ndarray:
        """对数值数据进行线性插值"""
        return np.interp(target_times, timestamps, values)
