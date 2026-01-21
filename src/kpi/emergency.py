"""
紧急事件检测KPI
包括急减速、横向猛打、顿挫检测
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import numpy as np
from scipy import signal

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor, EventDetection


@dataclass
class EmergencyEvent:
    """紧急事件"""
    event_type: str  # 'hard_braking', 'lateral_jerk', 'jerk_event'
    start_time: float
    end_time: float
    peak_value: float
    speed_at_event: float = 0.0
    # 事件窗口内的连续帧数据 [(relative_time_ms, value), ...]
    series: List[tuple] = field(default_factory=list)


class EmergencyEventsKPI(BaseKPI):
    """紧急事件检测KPI"""
    
    @property
    def name(self) -> str:
        return "紧急事件检测"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/vehicle/chassis_domain_report",
            "/control/control",  # 纵向加速度来源
            "/localization/localization"  # 用于检查定位可信度
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]
    
    @property
    def provides(self) -> List[str]:
        return ["急减速次数", "横向猛打次数", "顿挫次数"]
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 急减速配置（使用速度相关阈值）
        hard_braking_config = self.config.get('kpi', {}).get('hard_braking', {})
        self.hard_braking_min_duration = hard_braking_config.get('min_duration', 0.1)
        self.hard_braking_reset_duration = hard_braking_config.get('reset_duration', 0.2)
        
        # 顿挫配置（使用速度相关阈值）
        jerk_config = self.config.get('kpi', {}).get('jerk_event', {})
        self.jerk_min_duration = jerk_config.get('min_duration', 0.2)
        # 事件合并间隔：两个事件间隔小于此值则合并为一个
        # 原因：车辆颠簸通常是"负向冲击→过渡→正向反弹"的模式，人体感知为同一次颠簸
        self.jerk_merge_gap = jerk_config.get('merge_gap', 0.4)
        
        # 加载舒适性配置（用于获取速度相关阈值）
        self.comfort_config = self.config.get('kpi', {}).get('comfort', {})
        
        # 转向配置（横向猛打阈值）
        steering_config = self.config.get('kpi', {}).get('steering', {})
        # 最小持续时间，过滤短暂噪声（50Hz下100ms=5帧）
        self.lateral_min_duration = steering_config.get('min_duration', 0.1)
        # 事件合并间隔，相邻事件间隔小于此值则合并
        self.lateral_merge_gap = steering_config.get('merge_gap', 0.3)
        # 转角速度阈值 (°/s)，按速度分段
        self.lateral_speed_thresholds = steering_config.get('speed_thresholds', {
            0: 200, 10: 180, 20: 160, 30: 140, 40: 120,
            50: 100, 60: 80, 70: 60
        })
        # 转角加速度阈值 (°/s²)，按速度分段
        self.steering_acc_thresholds = steering_config.get('steering_acc_thresholds', {
            100: 120, 80: 150, 60: 200, 30: 250, 10: 400, 0: 600
        })
        
        # 滤波配置（与 weaving.py / steering.py 保持一致）
        # 转角滤波截止频率 (Hz) - 保留 0~10Hz 有效信号
        self.angle_filter_cutoff = steering_config.get('angle_filter_cutoff', 10.0)
        # 转角速度滤波截止频率 (Hz) - 保留 0~15Hz 有效信号
        self.rate_filter_cutoff = steering_config.get('rate_filter_cutoff', 15.0)
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]  # 有效的定位状态
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算紧急事件检测KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _apply_lowpass_filter(self, data: np.ndarray, cutoff_hz: float, fs: float, order: int = 2) -> np.ndarray:
        """
        应用 Butterworth 低通滤波
        
        Args:
            data: 输入信号
            cutoff_hz: 截止频率 (Hz)
            fs: 采样率 (Hz)
            order: 滤波器阶数
            
        Returns:
            滤波后的信号
        """
        if len(data) < 3:
            return data
        
        nyquist = fs / 2.0
        normal_cutoff = cutoff_hz / nyquist
        if normal_cutoff >= 1.0:
            return data
        
        b, a = signal.butter(order, normal_cutoff, btype='low', analog=False)
        filtered = signal.filtfilt(b, a, data)
        return filtered
    
    def _get_deceleration_threshold(self, speed_kph: float) -> float:
        """根据速度获取减速度阈值（fail级别）"""
        try:
            ranges = self.comfort_config.get('longitudinal_deceleration', {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                if speed_range[0] <= speed_kph < speed_range[1]:
                    return -r.get('fail', 3.0)  # 返回负值（减速度）
        except Exception:
            pass
        return -3.0  # 默认阈值
    
    def _get_deceleration_thresholds_vectorized(self, speeds: np.ndarray) -> np.ndarray:
        """向量化获取减速度阈值（性能优化）"""
        thresholds = np.full(len(speeds), -3.0, dtype=np.float64)  # 默认值
        try:
            ranges = self.comfort_config.get('longitudinal_deceleration', {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                fail_val = -r.get('fail', 3.0)
                mask = (speeds >= speed_range[0]) & (speeds < speed_range[1])
                thresholds[mask] = fail_val
        except Exception:
            pass
        return thresholds
    
    def _detect_hard_braking(self, 
                              timestamps: np.ndarray,
                              accelerations: np.ndarray,
                              speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测急减速事件（使用速度相关阈值）
        
        对每个数据点根据实时速度判断是否超过 fail 阈值
        
        注意：时间戳可能因为自动驾驶状态过滤而不连续（接管段被过滤）。
        当相邻时间戳差距超过 max_gap 时，强制结束当前事件。
        """
        events = []
        
        # 时间跳跃阈值：超过此值认为是不同的时间段
        max_gap = 0.5  # 0.5 秒
        
        # 向量化判断是否超过速度相关阈值（性能优化）
        thresholds_used = self._get_deceleration_thresholds_vectorized(speeds)
        over_threshold_mask = accelerations < thresholds_used  # 减速度为负值，小于阈值表示更急
        
        # 检测连续超阈值事件（考虑时间跳跃）
        in_event = False
        event_start_idx = None
        
        for i in range(len(over_threshold_mask)):
            # 检查时间跳跃：如果与上一个时间点间隔过大，强制结束当前事件
            if i > 0 and (timestamps[i] - timestamps[i-1]) > max_gap:
                if in_event:
                    # 强制结束事件（不跨越时间跳跃）
                    in_event = False
                    duration = timestamps[i-1] - timestamps[event_start_idx]
                    if duration >= self.hard_braking_min_duration:
                        event_mask = (timestamps >= timestamps[event_start_idx]) & (timestamps <= timestamps[i-1])
                        speed_at_event = np.mean(speeds[event_mask]) if np.any(event_mask) else 0.0
                        peak_value = np.min(accelerations[event_mask]) if np.any(event_mask) else accelerations[event_start_idx]
                        
                        events.append(EmergencyEvent(
                            event_type='hard_braking',
                            start_time=timestamps[event_start_idx],
                            end_time=timestamps[i-1],
                            peak_value=peak_value,
                            speed_at_event=speed_at_event
                        ))
            
            if over_threshold_mask[i] and not in_event:
                in_event = True
                event_start_idx = i
            elif not over_threshold_mask[i] and in_event:
                in_event = False
                # 检查持续时间
                duration = timestamps[i-1] - timestamps[event_start_idx]
                if duration >= self.hard_braking_min_duration:
                    # 计算事件期间的速度和峰值
                    event_mask = (timestamps >= timestamps[event_start_idx]) & (timestamps <= timestamps[i-1])
                    speed_at_event = np.mean(speeds[event_mask]) if np.any(event_mask) else 0.0
                    peak_value = np.min(accelerations[event_mask]) if np.any(event_mask) else accelerations[event_start_idx]
                    threshold_used = thresholds_used[event_start_idx]
                    
                    events.append(EmergencyEvent(
                        event_type='hard_braking',
                        start_time=timestamps[event_start_idx],
                        end_time=timestamps[i-1],
                        peak_value=peak_value,
                        speed_at_event=speed_at_event
                    ))
        
        # 处理最后一个事件
        if in_event:
            duration = timestamps[-1] - timestamps[event_start_idx]
            if duration >= self.hard_braking_min_duration:
                event_mask = (timestamps >= timestamps[event_start_idx])
                speed_at_event = np.mean(speeds[event_mask]) if np.any(event_mask) else 0.0
                peak_value = np.min(accelerations[event_mask]) if np.any(event_mask) else accelerations[event_start_idx]
                
                events.append(EmergencyEvent(
                    event_type='hard_braking',
                    start_time=timestamps[event_start_idx],
                    end_time=timestamps[-1],
                    peak_value=peak_value,
                    speed_at_event=speed_at_event
                ))
        
        return events
    
    def _get_jerk_threshold(self, speed_kph: float) -> float:
        """根据速度获取 jerk 阈值（peak级别）"""
        try:
            ranges = self.comfort_config.get('longitudinal_jerk', {}).get('peak', {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                if speed_range[0] <= speed_kph < speed_range[1]:
                    return r.get('threshold', 2.5)
        except Exception:
            pass
        return 2.5  # 默认阈值
    
    def _get_jerk_thresholds_vectorized(self, speeds: np.ndarray) -> np.ndarray:
        """向量化获取 jerk 阈值（性能优化）"""
        thresholds = np.full(len(speeds), 2.5, dtype=np.float64)  # 默认值
        try:
            ranges = self.comfort_config.get('longitudinal_jerk', {}).get('peak', {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                thresh_val = r.get('threshold', 2.5)
                mask = (speeds >= speed_range[0]) & (speeds < speed_range[1])
                thresholds[mask] = thresh_val
        except Exception:
            pass
        return thresholds
    
    def _detect_jerk_events(self,
                             timestamps: np.ndarray,
                             accelerations: np.ndarray,
                             speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测顿挫事件（使用速度相关阈值）
        
        对每个 jerk 点根据实时速度判断是否超过 peak 阈值
        
        注意：时间戳可能因为自动驾驶状态过滤而不连续（接管段被过滤）。
        当相邻时间戳差距超过 max_gap 时，强制结束当前事件。
        """
        events = []
        
        # 时间跳跃阈值：超过此值认为是不同的时间段
        max_gap = 0.5  # 0.5 秒
        
        # 计算jerk
        ts_jerk, jerks = SignalProcessor.compute_jerk(timestamps, accelerations)
        
        if len(jerks) == 0:
            return events
        
        # 将 speeds 对齐到 jerk 时间戳（使用插值，O(n) 复杂度）
        jerk_speeds = np.interp(ts_jerk, timestamps, speeds)
        
        # 向量化判断是否超过速度相关阈值（性能优化）
        jerk_thresholds = self._get_jerk_thresholds_vectorized(jerk_speeds)
        over_threshold_mask = np.abs(jerks) > jerk_thresholds
        
        # 检测连续超阈值事件（考虑时间跳跃）
        in_event = False
        event_start_idx = None
        
        for i in range(len(over_threshold_mask)):
            # 检查时间跳跃：如果与上一个时间点间隔过大，强制结束当前事件
            if i > 0 and (ts_jerk[i] - ts_jerk[i-1]) > max_gap:
                if in_event:
                    # 强制结束事件（不跨越时间跳跃）
                    in_event = False
                    duration = ts_jerk[i-1] - ts_jerk[event_start_idx]
                    if duration >= self.jerk_min_duration:
                        event_mask = (ts_jerk >= ts_jerk[event_start_idx]) & (ts_jerk <= ts_jerk[i-1])
                        peak_value = np.max(np.abs(jerks[event_mask])) if np.any(event_mask) else abs(jerks[event_start_idx])
                        
                        event_series = []
                        event_start = ts_jerk[event_start_idx]
                        for idx in range(event_start_idx, i):
                            rel_time_ms = (ts_jerk[idx] - event_start) * 1000
                            event_series.append((round(rel_time_ms, 1), round(jerks[idx], 2)))
                        
                        events.append(EmergencyEvent(
                            event_type='jerk_event',
                            start_time=ts_jerk[event_start_idx],
                            end_time=ts_jerk[i-1],
                            peak_value=peak_value,
                            series=event_series
                        ))
            
            if over_threshold_mask[i] and not in_event:
                in_event = True
                event_start_idx = i
            elif not over_threshold_mask[i] and in_event:
                in_event = False
                # 检查持续时间
                duration = ts_jerk[i-1] - ts_jerk[event_start_idx]
                if duration >= self.jerk_min_duration:
                    # 计算峰值和序列
                    event_mask = (ts_jerk >= ts_jerk[event_start_idx]) & (ts_jerk <= ts_jerk[i-1])
                    peak_value = np.max(np.abs(jerks[event_mask])) if np.any(event_mask) else abs(jerks[event_start_idx])
                    
                    # 保存事件窗口内的 jerk 序列 [(relative_time_ms, jerk_value), ...]
                    event_series = []
                    event_start = ts_jerk[event_start_idx]
                    for idx in range(event_start_idx, i):
                        rel_time_ms = (ts_jerk[idx] - event_start) * 1000
                        event_series.append((round(rel_time_ms, 1), round(jerks[idx], 2)))
                    
                    events.append(EmergencyEvent(
                        event_type='jerk_event',
                        start_time=ts_jerk[event_start_idx],
                        end_time=ts_jerk[i-1],
                        peak_value=peak_value,
                        series=event_series
                    ))
        
        # 处理最后一个事件
        if in_event:
            duration = ts_jerk[-1] - ts_jerk[event_start_idx]
            if duration >= self.jerk_min_duration:
                event_mask = (ts_jerk >= ts_jerk[event_start_idx])
                peak_value = np.max(np.abs(jerks[event_mask])) if np.any(event_mask) else abs(jerks[event_start_idx])
                
                # 保存事件窗口内的 jerk 序列
                event_series = []
                event_start = ts_jerk[event_start_idx]
                for idx in range(event_start_idx, len(ts_jerk)):
                    rel_time_ms = (ts_jerk[idx] - event_start) * 1000
                    event_series.append((round(rel_time_ms, 1), round(jerks[idx], 2)))
                
                events.append(EmergencyEvent(
                    event_type='jerk_event',
                    start_time=ts_jerk[event_start_idx],
                    end_time=ts_jerk[-1],
                    peak_value=peak_value,
                    series=event_series
                ))
        
        # 合并间隔小于 merge_gap 的相邻事件
        # 原因：车辆颠簸通常是"负向冲击→短暂过渡→正向反弹"的模式
        # 间隔小于 400ms 时人体通常感知为同一次颠簸
        if len(events) <= 1 or self.jerk_merge_gap <= 0:
            return events
        
        merged_events = []
        current = events[0]
        
        for next_event in events[1:]:
            gap = next_event.start_time - current.end_time
            
            if gap <= self.jerk_merge_gap:
                # 合并事件：扩展时间范围，更新峰值，合并序列
                # 合并 series（调整第二个事件的相对时间）
                merged_series = list(current.series)
                time_offset = (next_event.start_time - current.start_time) * 1000
                for rel_t, val in next_event.series:
                    merged_series.append((round(rel_t + time_offset, 1), val))
                
                current = EmergencyEvent(
                    event_type='jerk_event',
                    start_time=current.start_time,
                    end_time=next_event.end_time,
                    peak_value=max(current.peak_value, next_event.peak_value),
                    series=merged_series
                )
            else:
                # 间隔超过阈值，保存当前事件，开始新事件
                merged_events.append(current)
                current = next_event
        
        # 添加最后一个事件
        merged_events.append(current)
        
        return merged_events
    
    def _detect_lateral_jerk(self,
                              timestamps: np.ndarray,
                              steering_velocities: np.ndarray,
                              speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测横向猛打事件
        同时检测：转角速度超限 + 转角加速度超限
        
        注意：时间戳可能因为自动驾驶状态过滤而不连续（接管段被过滤）。
        当相邻时间戳差距超过 max_gap 时，强制结束当前事件，避免跨越接管段。
        """
        events = []
        
        if len(steering_velocities) == 0:
            return events
        
        # 时间跳跃阈值：超过此值认为是不同的时间段（接管或bag切换）
        max_gap = 0.5  # 0.5 秒
        
        # ========== 1. 转角速度超限检测 ==========
        vel_thresholds = self._get_lateral_thresholds_vectorized(speeds)
        vel_over_threshold = np.abs(steering_velocities) > vel_thresholds
        
        # 找连续区间（考虑时间跳跃）
        in_event = False
        event_start_idx = 0
        
        for i in range(len(vel_over_threshold)):
            # 检查时间跳跃：如果与上一个时间点间隔过大，强制结束当前事件
            if i > 0 and (timestamps[i] - timestamps[i-1]) > max_gap:
                if in_event:
                    # 强制结束事件（不跨越时间跳跃）
                    in_event = False
                    duration = timestamps[i - 1] - timestamps[event_start_idx]
                    if duration >= self.lateral_min_duration:
                        events.append(EmergencyEvent(
                            event_type='lateral_jerk_vel',
                            start_time=timestamps[event_start_idx],
                            end_time=timestamps[i - 1],
                            peak_value=float(np.max(np.abs(steering_velocities[event_start_idx:i]))),
                            speed_at_event=float(np.mean(speeds[event_start_idx:i]))
                        ))
            
            if vel_over_threshold[i] and not in_event:
                in_event = True
                event_start_idx = i
            elif not vel_over_threshold[i] and in_event:
                in_event = False
                # 检查持续时间，过滤单帧噪声
                duration = timestamps[i - 1] - timestamps[event_start_idx]
                if duration >= self.lateral_min_duration:
                    events.append(EmergencyEvent(
                        event_type='lateral_jerk_vel',  # 转角速度超限
                        start_time=timestamps[event_start_idx],
                        end_time=timestamps[i - 1],
                        peak_value=float(np.max(np.abs(steering_velocities[event_start_idx:i]))),
                        speed_at_event=float(np.mean(speeds[event_start_idx:i]))
                    ))
        
        # 检查是否在事件中结束
        if in_event:
            duration = timestamps[-1] - timestamps[event_start_idx]
            if duration >= self.lateral_min_duration:
                events.append(EmergencyEvent(
                    event_type='lateral_jerk_vel',
                    start_time=timestamps[event_start_idx],
                    end_time=timestamps[-1],
                    peak_value=float(np.max(np.abs(steering_velocities[event_start_idx:]))),
                    speed_at_event=float(np.mean(speeds[event_start_idx:]))
                ))
        
        # ========== 2. 转角加速度超限检测 ==========
        ts_acc, steering_acc = SignalProcessor.compute_derivative(timestamps, steering_velocities)
        
        if len(steering_acc) > 0:
            # 对应的速度（插值）
            speeds_at_acc = np.interp(ts_acc, timestamps, speeds)
            acc_thresholds = self._get_steering_acc_thresholds_vectorized(speeds_at_acc)
            acc_over_threshold = np.abs(steering_acc) > acc_thresholds
            
            in_event = False
            event_start_idx = 0
            
            for i in range(len(acc_over_threshold)):
                # 检查时间跳跃
                if i > 0 and (ts_acc[i] - ts_acc[i-1]) > max_gap:
                    if in_event:
                        in_event = False
                        duration = ts_acc[i - 1] - ts_acc[event_start_idx]
                        if duration >= self.lateral_min_duration:
                            events.append(EmergencyEvent(
                                event_type='lateral_jerk_acc',
                                start_time=ts_acc[event_start_idx],
                                end_time=ts_acc[i - 1],
                                peak_value=float(np.max(np.abs(steering_acc[event_start_idx:i]))),
                                speed_at_event=float(np.mean(speeds_at_acc[event_start_idx:i]))
                            ))
                
                if acc_over_threshold[i] and not in_event:
                    in_event = True
                    event_start_idx = i
                elif not acc_over_threshold[i] and in_event:
                    in_event = False
                    # 检查持续时间，过滤单帧噪声
                    duration = ts_acc[i - 1] - ts_acc[event_start_idx]
                    if duration >= self.lateral_min_duration:
                        events.append(EmergencyEvent(
                            event_type='lateral_jerk_acc',  # 转角加速度超限
                            start_time=ts_acc[event_start_idx],
                            end_time=ts_acc[i - 1],
                            peak_value=float(np.max(np.abs(steering_acc[event_start_idx:i]))),
                            speed_at_event=float(np.mean(speeds_at_acc[event_start_idx:i]))
                        ))
            
            if in_event:
                duration = ts_acc[-1] - ts_acc[event_start_idx]
                if duration >= self.lateral_min_duration:
                    events.append(EmergencyEvent(
                        event_type='lateral_jerk_acc',
                        start_time=ts_acc[event_start_idx],
                        end_time=ts_acc[-1],
                        peak_value=float(np.max(np.abs(steering_acc[event_start_idx:]))),
                        speed_at_event=float(np.mean(speeds_at_acc[event_start_idx:]))
                    ))
        
        # 按时间排序
        events.sort(key=lambda e: e.start_time)
        
        # 合并相邻事件
        events = self._merge_lateral_jerk_events(events)
        
        return events
    
    def _merge_lateral_jerk_events(self, events: List[EmergencyEvent]) -> List[EmergencyEvent]:
        """
        合并相邻的横向猛打事件
        
        当两个事件间隔小于 merge_gap 时，合并为一个事件。
        这避免了一次大幅转向被拆分成多个短事件。
        """
        if len(events) <= 1:
            return events
        
        merged = []
        current = events[0]
        
        for i in range(1, len(events)):
            next_event = events[i]
            gap = next_event.start_time - current.end_time
            
            if gap <= self.lateral_merge_gap:
                # 合并事件：取更大的峰值，平均速度，扩展时间范围
                current = EmergencyEvent(
                    event_type=current.event_type if current.event_type == next_event.event_type else 'lateral_jerk_both',
                    start_time=current.start_time,
                    end_time=next_event.end_time,
                    peak_value=max(current.peak_value, next_event.peak_value),
                    speed_at_event=(current.speed_at_event + next_event.speed_at_event) / 2
                )
            else:
                merged.append(current)
                current = next_event
        
        merged.append(current)
        return merged
    
    def _get_lateral_threshold(self, speed_kmh: float) -> float:
        """根据速度获取转角速度阈值"""
        sorted_speeds = sorted(self.lateral_speed_thresholds.keys(), reverse=True)
        
        for threshold_speed in sorted_speeds:
            if speed_kmh >= threshold_speed:
                return self.lateral_speed_thresholds[threshold_speed]
        
        return self.lateral_speed_thresholds.get(0, 200)
    
    def _get_lateral_thresholds_vectorized(self, speeds: np.ndarray) -> np.ndarray:
        """向量化获取转角速度阈值（性能优化）"""
        thresholds = np.full(len(speeds), self.lateral_speed_thresholds.get(0, 200), dtype=np.float64)
        sorted_speeds = sorted(self.lateral_speed_thresholds.keys())  # 升序
        
        for thresh_speed in sorted_speeds:
            mask = speeds >= thresh_speed
            thresholds[mask] = self.lateral_speed_thresholds[thresh_speed]
        
        return thresholds
    
    def _get_steering_acc_threshold(self, speed_kmh: float) -> float:
        """根据速度获取转角加速度阈值"""
        sorted_speeds = sorted(self.steering_acc_thresholds.keys(), reverse=True)
        
        for threshold_speed in sorted_speeds:
            if speed_kmh > threshold_speed:
                return self.steering_acc_thresholds[threshold_speed]
        
        return self.steering_acc_thresholds.get(0, 600)
    
    def _get_steering_acc_thresholds_vectorized(self, speeds: np.ndarray) -> np.ndarray:
        """向量化获取转角加速度阈值（性能优化）"""
        thresholds = np.full(len(speeds), self.steering_acc_thresholds.get(0, 600), dtype=np.float64)
        sorted_speeds = sorted(self.steering_acc_thresholds.keys())  # 升序
        
        for thresh_speed in sorted_speeds:
            mask = speeds > thresh_speed
            thresholds[mask] = self.steering_acc_thresholds[thresh_speed]
        
        return thresholds
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="急减速次数",
            value=0,
            unit="次",
            description="数据不足，无法检测紧急事件"
        ))
        self.add_result(KPIResult(
            name="顿挫次数",
            value=0,
            unit="次",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="横向猛打次数",
            value=0,
            unit="次",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集紧急事件相关数据（流式模式）
        
        Note:
            此方法现在不再从 synced_frames 收集数据，
            而是依赖 main.py 预先收集的高频数据:
            - control_highfreq (~100Hz): 纵向加速度，用于急减速和顿挫检测
            - chassis_highfreq (~50Hz): 转角速度，用于横向猛打检测
        """
        # 高频数据已在 main.py 的 _collect_highfreq_data 中收集
        pass
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算紧急事件KPI（流式模式）
        
        Note:
            数据来源优先级：
            1. 高频数据 (control_highfreq ~100Hz, chassis_highfreq ~50Hz) - 优先使用
            2. 低频数据 (emergency_data ~10Hz) - 兼容旧模式/缓存模式
        """
        self.clear_results()
        
        # 检查是否有高频数据可用
        use_control_highfreq = len(streaming_data.control_highfreq) > 0 and len(streaming_data.control_auto_states) > 0
        use_chassis_highfreq = len(streaming_data.chassis_highfreq) > 0 and len(streaming_data.chassis_auto_states) > 0
        
        # ========== 纵向加速度数据（用于急减速和顿挫检测）==========
        if use_control_highfreq:
            # 高频控制数据: (lon_acc, timestamp)
            auto_indices = [i for i, (is_auto, _) in enumerate(streaming_data.control_auto_states) 
                           if is_auto and i < len(streaming_data.control_highfreq)]
            
            if len(auto_indices) >= 10:
                lon_accelerations = np.array([streaming_data.control_highfreq[i][0] for i in auto_indices])
                control_timestamps = np.array([streaming_data.control_highfreq[i][1] for i in auto_indices])
                
                # 从底盘高频数据获取速度（插值对齐）
                if use_chassis_highfreq:
                    chassis_ts = np.array([d[4] for d in streaming_data.chassis_highfreq])
                    chassis_speeds = np.array([d[2] for d in streaming_data.chassis_highfreq])
                    control_speeds = np.interp(control_timestamps, chassis_ts, chassis_speeds)
                else:
                    # 回退到低频数据
                    if len(streaming_data.emergency_data) > 0:
                        emerg_ts = np.array([d[3] for d in streaming_data.emergency_data])
                        emerg_speeds = np.array([d[2] for d in streaming_data.emergency_data])
                        control_speeds = np.interp(control_timestamps, emerg_ts, emerg_speeds)
                    else:
                        control_speeds = np.zeros_like(control_timestamps)
            else:
                # 高频数据不足，回退
                use_control_highfreq = False
        
        if not use_control_highfreq:
            # 回退到低频数据
            if len(streaming_data.emergency_data) < 10:
                self._add_empty_results()
                return self.get_results()
            
            lon_accelerations = np.array([d[0] for d in streaming_data.emergency_data])
            control_speeds = np.array([d[2] for d in streaming_data.emergency_data])
            control_timestamps = np.array([d[3] for d in streaming_data.emergency_data])
        
        # ========== 转角速度数据（用于横向猛打检测）==========
        # 不直接使用底盘的 steering_vel（原始信号噪声大），
        # 而是从 steering_angle 差分计算，与低频模式保持一致
        if use_chassis_highfreq:
            # 高频底盘数据: (steering_angle, steering_vel, speed, lat_acc, timestamp)
            auto_indices = [i for i, (is_auto, _) in enumerate(streaming_data.chassis_auto_states) 
                           if is_auto and i < len(streaming_data.chassis_highfreq)]
            
            if len(auto_indices) >= 10:
                # 提取转角、速度、时间戳
                steering_angles = np.array([streaming_data.chassis_highfreq[i][0] for i in auto_indices])
                chassis_speeds = np.array([streaming_data.chassis_highfreq[i][2] for i in auto_indices])
                chassis_timestamps = np.array([streaming_data.chassis_highfreq[i][4] for i in auto_indices])
                
                # 估计实际采样率
                if len(chassis_timestamps) >= 2:
                    actual_fs = 1.0 / np.median(np.diff(chassis_timestamps))
                    actual_fs = max(1.0, min(actual_fs, 100.0))
                else:
                    actual_fs = 50.0  # 默认 50Hz
                
                # 1. 对转角进行低通滤波（去噪）
                angle_cutoff = min(self.angle_filter_cutoff, actual_fs / 2 - 0.1)
                if angle_cutoff > 0:
                    steering_angles_filtered = self._apply_lowpass_filter(
                        steering_angles, angle_cutoff, actual_fs, order=2)
                else:
                    steering_angles_filtered = steering_angles
                
                # 2. 差分计算转角速度
                ts_rate, steering_velocities = SignalProcessor.compute_derivative(
                    chassis_timestamps, steering_angles_filtered)
                
                # 3. 对转角速度进行低通滤波
                rate_cutoff = min(self.rate_filter_cutoff, actual_fs / 2 - 0.1)
                if rate_cutoff > 0 and len(steering_velocities) >= 3:
                    steering_velocities = self._apply_lowpass_filter(
                        steering_velocities, rate_cutoff, actual_fs, order=2)
                
                # 4. 将速度插值到转角速度的时间戳
                chassis_speeds = np.interp(ts_rate, chassis_timestamps, chassis_speeds)
                chassis_timestamps = ts_rate
            else:
                use_chassis_highfreq = False
        
        if not use_chassis_highfreq:
            # 回退到低频数据
            if len(streaming_data.emergency_data) >= 10:
                steering_velocities = np.array([d[1] for d in streaming_data.emergency_data])
                chassis_speeds = np.array([d[2] for d in streaming_data.emergency_data])
                chassis_timestamps = np.array([d[3] for d in streaming_data.emergency_data])
            else:
                steering_velocities = np.array([])
                chassis_speeds = np.array([])
                chassis_timestamps = np.array([])
        
        # 1. 检测急减速
        hard_braking_events = self._detect_hard_braking(
            control_timestamps, lon_accelerations, control_speeds)
        
        # 2. 检测顿挫（需要速度信息）
        jerk_events = self._detect_jerk_events(control_timestamps, lon_accelerations, control_speeds)
        
        # 3. 检测横向猛打（转角速度超限 + 转角加速度超限）
        if len(steering_velocities) >= 10:
            lateral_jerk_events = self._detect_lateral_jerk(
                chassis_timestamps, steering_velocities, chassis_speeds)
        else:
            lateral_jerk_events = []
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 添加结果
        # 获取典型阈值范围用于描述
        low_speed_thresh = abs(self._get_deceleration_threshold(10))
        high_speed_thresh = abs(self._get_deceleration_threshold(60))
        
        hard_braking_result = KPIResult(
            name="急减速次数",
            value=len(hard_braking_events),
            unit="次",
            description=f"减速度超过速度相关阈值（{high_speed_thresh}~{low_speed_thresh}m/s²）且持续{self.hard_braking_min_duration*1000}ms以上",
            details={
                'threshold_at_10kph': low_speed_thresh,
                'threshold_at_60kph': high_speed_thresh,
                'min_duration_ms': self.hard_braking_min_duration * 1000,
                'sample_count': len(control_timestamps)
            }
        )
        for i, e in enumerate(hard_braking_events):
            threshold_used = abs(self._get_deceleration_threshold(e.speed_at_event))
            duration_ms = (e.end_time - e.start_time) * 1000
            hard_braking_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=f"急减速 #{i+1}@{e.speed_at_event:.0f}km/h：{e.peak_value:.2f}<{threshold_used:.2f}m/s²，持续{duration_ms:.0f}ms",
                value=e.peak_value,
                threshold=-threshold_used
            )
        self.add_result(hard_braking_result)
        
        # 获取典型阈值范围用于描述
        jerk_low_thresh = self._get_jerk_threshold(10)
        jerk_high_thresh = self._get_jerk_threshold(60)
        
        jerk_result = KPIResult(
            name="顿挫次数",
            value=len(jerk_events),
            unit="次",
            description=f"|纵向jerk|超过速度相关阈值（{jerk_high_thresh}~{jerk_low_thresh}m/s³）且持续{self.jerk_min_duration*1000}ms以上"
        )
        for i, e in enumerate(jerk_events):
            # 估算事件时的速度（使用事件开始时间）
            event_start_idx = np.argmin(np.abs(control_timestamps - e.start_time))
            event_speed = control_speeds[event_start_idx] if event_start_idx < len(control_speeds) else 30.0
            threshold_used = self._get_jerk_threshold(event_speed)
            duration_ms = (e.end_time - e.start_time) * 1000
            
            # 格式化 jerk 序列（只显示值，省略相对时间）
            jerk_values = [v for _, v in e.series] if e.series else []
            series_str = ",".join(f"{v:.1f}" for v in jerk_values[:20])  # 最多显示20个
            if len(jerk_values) > 20:
                series_str += f"...共{len(jerk_values)}帧"
            
            jerk_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=f"顿挫 #{i+1}@{event_speed:.0f}km/h：|{e.peak_value:.2f}|>{threshold_used:.2f}m/s³，持续{duration_ms:.0f}ms，jerk序列[{series_str}]",
                value={'peak': round(e.peak_value, 2), 'series': e.series},
                threshold=threshold_used
            )
        self.add_result(jerk_result)
        
        # 分别统计转角速度超限和转角加速度超限
        vel_events = [e for e in lateral_jerk_events if e.event_type == 'lateral_jerk_vel']
        acc_events = [e for e in lateral_jerk_events if e.event_type == 'lateral_jerk_acc']
        
        lateral_result = KPIResult(
            name="横向猛打次数",
            value=len(lateral_jerk_events),
            unit="次",
            description="转角速度或转角加速度超过速度相关阈值的次数",
            details={
                'vel_over_count': len(vel_events),
                'acc_over_count': len(acc_events)
            }
        )
        for i, e in enumerate(lateral_jerk_events):
            duration_ms = (e.end_time - e.start_time) * 1000
            if e.event_type == 'lateral_jerk_vel':
                threshold = self._get_lateral_threshold(e.speed_at_event)
                lateral_result.add_anomaly(
                    timestamp=e.start_time,
                    bag_name=bag_mapper.get_bag_name(e.start_time),
                    description=f"横向猛打 #{i+1}(转角速度)：{e.peak_value:.1f}°/s，车速 {e.speed_at_event:.1f}km/h，持续{duration_ms:.0f}ms",
                    value=e.peak_value,
                    threshold=threshold
                )
            else:  # lateral_jerk_acc
                threshold = self._get_steering_acc_threshold(e.speed_at_event)
                lateral_result.add_anomaly(
                    timestamp=e.start_time,
                    bag_name=bag_mapper.get_bag_name(e.start_time),
                    description=f"横向猛打 #{i+1}(转角加速度)：{e.peak_value:.1f}°/s²，车速 {e.speed_at_event:.1f}km/h，持续{duration_ms:.0f}ms",
                    value=e.peak_value,
                    threshold=threshold
                )
        self.add_result(lateral_result)
        
        # 获取自动驾驶里程
        auto_mileage_km = kwargs.get('auto_mileage_km', 0)
        
        # 急减速频率
        hard_braking_count = len(hard_braking_events)
        hard_braking_per_100km = (hard_braking_count / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        avg_hard_braking_mileage = (auto_mileage_km / hard_braking_count) if hard_braking_count > 0 else float('inf')
        
        self.add_result(KPIResult(
            name="急减速频率",
            value=round(hard_braking_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的急减速次数"
        ))
        
        self.add_result(KPIResult(
            name="平均急减速里程",
            value="∞" if hard_braking_count == 0 else round(avg_hard_braking_mileage, 3),
            unit="km/次",
            description="平均每多少公里自动驾驶里程急减速一次"
        ))
        
        # 顿挫频率
        jerk_count = len(jerk_events)
        jerk_per_100km = (jerk_count / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        avg_jerk_mileage = (auto_mileage_km / jerk_count) if jerk_count > 0 else float('inf')
        
        self.add_result(KPIResult(
            name="顿挫频率",
            value=round(jerk_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的顿挫次数"
        ))
        
        self.add_result(KPIResult(
            name="平均顿挫里程",
            value="∞" if jerk_count == 0 else round(avg_jerk_mileage, 3),
            unit="km/次",
            description="平均每多少公里自动驾驶里程顿挫一次"
        ))
        
        return self.get_results()

