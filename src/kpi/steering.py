"""
转向平滑度KPI
基于滑动窗口的转向平滑度评估
"""
from typing import List, Dict, Any, Optional
import numpy as np
from scipy import signal

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor


class SteeringSmoothnessKPI(BaseKPI):
    """转向平滑度KPI - 基于滑动窗口的平滑度评估"""
    
    @property
    def name(self) -> str:
        return "转向平滑度"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/vehicle/chassis_domain_report",
            "/localization/localization"  # 用于定位可信度检查
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]
    
    @property
    def provides(self) -> List[str]:
        return ["转角速度RMS", "转角速度P95", "转角速度符号翻转率"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 加载转向平滑度配置
        smoothness_config = self.config.get('kpi', {}).get('steering_smoothness', {})
        
        # 基本参数
        self.sampling_rate_hz = smoothness_config.get('sampling_rate_hz', 50)
        self.min_speed_kmh = smoothness_config.get('valid_condition', {}).get('min_speed_kmh', 1.0)
        
        # 预处理配置
        preprocess_config = smoothness_config.get('preprocessing', {})
        angle_config = preprocess_config.get('steering_angle', {})
        rate_config = preprocess_config.get('steering_angle_rate', {})
        
        self.angle_filter_cutoff = angle_config.get('lowpass_filter', {}).get('cutoff_hz', 3.0)
        self.rate_filter_cutoff = rate_config.get('lowpass_filter', {}).get('cutoff_hz', 5.0)
        
        # 滑动窗口配置
        window_config = smoothness_config.get('window', {})
        self.window_duration_sec = window_config.get('duration_sec', 1.0)
        self.window_step_sec = window_config.get('step_sec', 0.1)
        
        # 指标配置
        metrics_config = smoothness_config.get('metrics', {})
        self.rms_enabled = metrics_config.get('steering_rate_rms', {}).get('enabled', True)
        self.p95_enabled = metrics_config.get('steering_rate_p95', {}).get('enabled', True)
        self.flip_enabled = metrics_config.get('steering_rate_flip', {}).get('enabled', True)
        self.flip_min_rate = metrics_config.get('steering_rate_flip', {}).get('min_effective_rate_deg_s', 5.0)
        
        # 速度区间和阈值
        self.speed_bins = smoothness_config.get('speed_bins_kmh', [
            [0, 10], [10, 20], [20, 30], [30, 40],
            [40, 50], [50, 60], [60, 80], [80, 120]
        ])
        self.thresholds = smoothness_config.get('thresholds', {})
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
    
    def _apply_lowpass_filter(self, data: np.ndarray, cutoff_hz: float, fs: float, order: int = 2) -> np.ndarray:
        """应用 Butterworth 低通滤波"""
        if len(data) < 3:
            return data
        
        nyquist = fs / 2.0
        normal_cutoff = cutoff_hz / nyquist
        if normal_cutoff >= 1.0:
            return data
        
        b, a = signal.butter(order, normal_cutoff, btype='low', analog=False)
        filtered = signal.filtfilt(b, a, data)
        return filtered
    
    def _get_threshold(self, metric_name: str, speed_kph: float, level: str = 'warn') -> float:
        """根据速度和指标名称获取阈值"""
        try:
            metric_thresholds = self.thresholds.get(metric_name, [])
            for thresh_config in metric_thresholds:
                speed_range = thresh_config.get('speed_range', [0, 999])
                if speed_range[0] <= speed_kph < speed_range[1]:
                    return thresh_config.get(level, 0.0)
        except Exception:
            pass
        return 0.0
    
    def _compute_sliding_window_rms(self, values: np.ndarray, timestamps: np.ndarray, 
                                     window_duration: float, step: float) -> tuple:
        """计算滑动窗口 RMS"""
        if len(values) == 0:
            return np.array([]), np.array([])
        
        window_size = int(window_duration * self.sampling_rate_hz)
        step_size = int(step * self.sampling_rate_hz)
        
        rms_values = []
        window_centers = []
        
        for start_idx in range(0, len(values) - window_size + 1, step_size):
            end_idx = start_idx + window_size
            window_data = values[start_idx:end_idx]
            rms = np.sqrt(np.mean(window_data ** 2))
            rms_values.append(rms)
            window_centers.append(timestamps[start_idx + window_size // 2])
        
        return np.array(window_centers), np.array(rms_values)
    
    def _compute_sign_flip_rate(self, values: np.ndarray, timestamps: np.ndarray,
                                 min_effective_rate: float) -> float:
        """计算符号翻转率（次/秒）"""
        if len(values) < 2:
            return 0.0
        
        # 只考虑绝对值大于阈值的点
        effective_mask = np.abs(values) >= min_effective_rate
        if np.sum(effective_mask) < 2:
            return 0.0
        
        effective_values = values[effective_mask]
        effective_timestamps = timestamps[effective_mask]
        
        # 计算符号变化次数
        signs = np.sign(effective_values)
        sign_changes = np.sum(np.diff(signs) != 0)
        
        # 计算时间跨度
        if len(effective_timestamps) > 1:
            time_span = effective_timestamps[-1] - effective_timestamps[0]
            if time_span > 0:
                return sign_changes / time_span
        
        return 0.0
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算转向平滑度KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="转角速度RMS",
            value=0.0,
            unit="°/s",
            description="数据不足，无法计算转向平滑度"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集转向数据（流式模式）
        """
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            chassis_msg = frame.messages.get("/vehicle/chassis_domain_report")
            loc_msg = frame.messages.get("/localization/localization")
            
            if fm_msg is None or chassis_msg is None:
                continue
            
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            is_auto = operator_type == self.auto_operator_type
            
            if not is_auto:
                continue
            
            # 定位可信度检查
            if loc_msg is not None:
                loc_status = MessageAccessor.get_field(loc_msg, "status.common", None)
                pos_stddev_east = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.east", None)
                pos_stddev_north = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.north", None)
                
                if loc_status is not None and loc_status not in self.loc_valid_status:
                    continue
                
                if pos_stddev_east is not None and pos_stddev_north is not None:
                    if max(pos_stddev_east, pos_stddev_north) > self.loc_max_stddev:
                        continue
            
            # 获取转向数据
            steering_angle = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle", None)
            speed = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_speed", None)
            
            if steering_angle is None or speed is None:
                continue
            
            # 速度过滤
            if speed < self.min_speed_kmh:
                continue
            
            # 存储: (steering_angle, speed, timestamp)
            streaming_data.steering_data.append((
                steering_angle,
                speed,
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算转向平滑度KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.steering_data) < int(self.window_duration_sec * self.sampling_rate_hz):
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据
        steering_angles = np.array([d[0] for d in streaming_data.steering_data])
        speeds = np.array([d[1] for d in streaming_data.steering_data])
        timestamps = np.array([d[2] for d in streaming_data.steering_data])
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 1. 信号预处理：对转角进行低通滤波
        steering_angles_filtered = self._apply_lowpass_filter(
            steering_angles, self.angle_filter_cutoff, self.sampling_rate_hz, order=2)
        
        # 2. 计算转角速度（从滤波后的转角计算）
        ts_rate, steering_rates = SignalProcessor.compute_derivative(
            timestamps, steering_angles_filtered)
        
        if len(steering_rates) == 0:
            self._add_empty_results()
            return self.get_results()
        
        # 3. 对转角速度进行低通滤波
        steering_rates_filtered = self._apply_lowpass_filter(
            steering_rates, self.rate_filter_cutoff, self.sampling_rate_hz, order=2)
        
        # 4. 计算滑动窗口 RMS 并检测超阈值事件
        if self.rms_enabled:
            window_centers, rms_values = self._compute_sliding_window_rms(
                np.abs(steering_rates_filtered), ts_rate,
                self.window_duration_sec, self.window_step_sec)
            
            if len(rms_values) > 0:
                # 统计 RMS 指标
                rms_mean = np.mean(rms_values)
                rms_max = np.max(rms_values)
                rms_p95 = np.percentile(rms_values, 95)
                
                # 将速度对齐到窗口中心时间戳
                speeds_at_windows = np.interp(window_centers, timestamps, speeds)
                
                # 检测超阈值事件（fail级别）
                rms_events = []
                for i, (rms_val, ts, spd) in enumerate(zip(rms_values, window_centers, speeds_at_windows)):
                    fail_threshold = self._get_threshold('steering_rate_rms', spd, 'fail')
                    if fail_threshold > 0 and rms_val > fail_threshold:
                        rms_events.append({
                            'timestamp': ts,
                            'value': rms_val,
                            'threshold': fail_threshold,
                            'speed': spd
                        })
                
                self.add_result(KPIResult(
                    name="转角速度RMS均值",
                    value=round(rms_mean, 2),
                    unit="°/s",
                    description="滑动窗口内转角速度RMS的平均值（1秒窗口，100ms步进）"
                ))
                
                rms_max_result = KPIResult(
                    name="转角速度RMS最大",
                    value=round(rms_max, 2),
                    unit="°/s",
                    description="滑动窗口内转角速度RMS的最大值"
                )
                
                # 添加 RMS 超阈值事件
                for event in rms_events[:50]:  # 最多50个事件
                    rms_max_result.add_anomaly(
                        timestamp=event['timestamp'],
                        bag_name=bag_mapper.get_bag_name(event['timestamp']),
                        description=f"转角速度RMS超阈值@{event['speed']:.0f}km/h: {event['value']:.1f}>{event['threshold']:.0f}°/s",
                        value=event['value'],
                        threshold=event['threshold']
                    )
                
                self.add_result(rms_max_result)
                
                self.add_result(KPIResult(
                    name="转角速度RMS P95",
                    value=round(rms_p95, 2),
                    unit="°/s",
                    description="滑动窗口内转角速度RMS的95分位数"
                ))
        
        # 5. 计算转角速度 P95（全局统计）并检测超阈值事件
        if self.p95_enabled:
            rate_p95 = np.percentile(np.abs(steering_rates_filtered), 95)
            
            # 将速度对齐到转角速度时间戳
            speeds_at_rate = np.interp(ts_rate, timestamps, speeds)
            
            # 检测超阈值事件（fail级别）- 找出超过阈值的连续段
            p95_events = []
            in_event = False
            event_start_idx = 0
            
            for i, (rate_val, ts, spd) in enumerate(zip(np.abs(steering_rates_filtered), ts_rate, speeds_at_rate)):
                fail_threshold = self._get_threshold('steering_rate_p95', spd, 'fail')
                exceeds = fail_threshold > 0 and rate_val > fail_threshold
                
                if exceeds and not in_event:
                    # 事件开始
                    in_event = True
                    event_start_idx = i
                elif not exceeds and in_event:
                    # 事件结束
                    in_event = False
                    event_end_idx = i - 1
                    # 记录事件
                    event_rates = np.abs(steering_rates_filtered[event_start_idx:event_end_idx+1])
                    event_speeds = speeds_at_rate[event_start_idx:event_end_idx+1]
                    p95_events.append({
                        'start_ts': ts_rate[event_start_idx],
                        'end_ts': ts_rate[event_end_idx],
                        'peak_value': np.max(event_rates),
                        'avg_speed': np.mean(event_speeds),
                        'threshold': self._get_threshold('steering_rate_p95', np.mean(event_speeds), 'fail'),
                        'frame_count': event_end_idx - event_start_idx + 1
                    })
            
            # 处理最后一个事件（如果还在进行中）
            if in_event:
                event_end_idx = len(steering_rates_filtered) - 1
                event_rates = np.abs(steering_rates_filtered[event_start_idx:event_end_idx+1])
                event_speeds = speeds_at_rate[event_start_idx:event_end_idx+1]
                p95_events.append({
                    'start_ts': ts_rate[event_start_idx],
                    'end_ts': ts_rate[event_end_idx],
                    'peak_value': np.max(event_rates),
                    'avg_speed': np.mean(event_speeds),
                    'threshold': self._get_threshold('steering_rate_p95', np.mean(event_speeds), 'fail'),
                    'frame_count': event_end_idx - event_start_idx + 1
                })
            
            p95_result = KPIResult(
                name="转角速度P95",
                value=round(rate_p95, 2),
                unit="°/s",
                description="转角速度绝对值的95分位数"
            )
            
            # 添加 P95 超阈值事件
            for event in p95_events[:50]:  # 最多50个事件
                duration_ms = (event['end_ts'] - event['start_ts']) * 1000
                p95_result.add_anomaly(
                    timestamp=event['start_ts'],
                    bag_name=bag_mapper.get_bag_name(event['start_ts']),
                    description=f"转角速度超阈值@{event['avg_speed']:.0f}km/h: 峰值{event['peak_value']:.1f}>{event['threshold']:.0f}°/s, 持续{duration_ms:.0f}ms/{event['frame_count']}帧",
                    value=event['peak_value'],
                    threshold=event['threshold']
                )
            
            self.add_result(p95_result)
        
        # 6. 计算符号翻转率并检测超阈值事件
        if self.flip_enabled:
            flip_rate = self._compute_sign_flip_rate(
                steering_rates_filtered, ts_rate, self.flip_min_rate)
            
            # 获取平均速度用于阈值判断
            avg_speed = np.mean(speeds) if len(speeds) > 0 else 30.0
            fail_threshold = self._get_threshold('steering_rate_flip', avg_speed, 'fail')
            
            flip_result = KPIResult(
                name="转角速度符号翻转率",
                value=round(flip_rate, 2),
                unit="次/秒",
                description=f"转角速度符号变化频率（仅统计|速率|>={self.flip_min_rate}°/s的点）"
            )
            
            # 如果全局翻转率超阈值，找出翻转率最高的时间段
            if fail_threshold > 0 and flip_rate > fail_threshold:
                window_size = int(self.window_duration_sec * self.sampling_rate_hz)
                step_size = int(self.window_step_sec * self.sampling_rate_hz)
                
                # 计算每个窗口的翻转率
                flip_events = []
                for start_idx in range(0, len(steering_rates_filtered) - window_size + 1, step_size):
                    end_idx = start_idx + window_size
                    window_rates = steering_rates_filtered[start_idx:end_idx]
                    window_ts = ts_rate[start_idx:end_idx]
                    window_flip = self._compute_sign_flip_rate(window_rates, window_ts, self.flip_min_rate)
                    
                    # 获取该窗口的平均速度
                    window_speeds = np.interp(window_ts, timestamps, speeds)
                    window_avg_speed = np.mean(window_speeds)
                    window_threshold = self._get_threshold('steering_rate_flip', window_avg_speed, 'fail')
                    
                    if window_threshold > 0 and window_flip > window_threshold:
                        flip_events.append({
                            'timestamp': window_ts[len(window_ts) // 2],
                            'value': window_flip,
                            'threshold': window_threshold,
                            'speed': window_avg_speed
                        })
                
                # 合并相邻的超阈值窗口
                merged_events = []
                if len(flip_events) > 0:
                    current_event = flip_events[0].copy()
                    for i in range(1, len(flip_events)):
                        time_gap = flip_events[i]['timestamp'] - flip_events[i-1]['timestamp']
                        if time_gap <= self.window_step_sec * 2:  # 相邻窗口
                            # 更新为峰值
                            if flip_events[i]['value'] > current_event['value']:
                                current_event = flip_events[i].copy()
                        else:
                            merged_events.append(current_event)
                            current_event = flip_events[i].copy()
                    merged_events.append(current_event)
                
                # 添加翻转率超阈值事件
                for event in merged_events[:20]:  # 最多20个事件
                    flip_result.add_anomaly(
                        timestamp=event['timestamp'],
                        bag_name=bag_mapper.get_bag_name(event['timestamp']),
                        description=f"转角速度翻转率超阈值@{event['speed']:.0f}km/h: {event['value']:.2f}>{event['threshold']:.1f}次/秒",
                        value=event['value'],
                        threshold=event['threshold']
                    )
            
            self.add_result(flip_result)
        
        # 7. 按速度区间分组统计（如果启用）
        output_config = self.config.get('kpi', {}).get('steering_smoothness', {}).get('output', {})
        if output_config.get('group_by_speed_bin', True):
            # 将速度对齐到转角速度时间戳
            speeds_at_rate = np.interp(ts_rate, timestamps, speeds)
            
            for speed_bin in self.speed_bins:
                bin_min, bin_max = speed_bin
                bin_mask = (speeds_at_rate >= bin_min) & (speeds_at_rate < bin_max)
                bin_rates = steering_rates_filtered[bin_mask]
                
                if len(bin_rates) > 0:
                    bin_rms_mean = np.sqrt(np.mean(bin_rates ** 2))
                    bin_p95 = np.percentile(np.abs(bin_rates), 95)
                    
                    self.add_result(KPIResult(
                        name=f"转角速度RMS({bin_min}-{bin_max}km/h)",
                        value=round(bin_rms_mean, 2),
                        unit="°/s",
                        description=f"速度区间[{bin_min}-{bin_max}km/h]的转角速度RMS"
                    ))
                    
                    self.add_result(KPIResult(
                        name=f"转角速度P95({bin_min}-{bin_max}km/h)",
                        value=round(bin_p95, 2),
                        unit="°/s",
                        description=f"速度区间[{bin_min}-{bin_max}km/h]的转角速度P95"
                    ))
        
        return self.get_results()
