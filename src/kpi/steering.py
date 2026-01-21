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
        # 异常事件报告的最低速度（低于此速度的异常不报告）
        self.min_anomaly_speed_kmh = smoothness_config.get('valid_condition', {}).get('min_anomaly_speed_kmh', 10.0)
        
        # 预处理配置（滤波截止频率）
        preprocess_config = smoothness_config.get('preprocessing', {})
        angle_config = preprocess_config.get('steering_angle', {})
        rate_config = preprocess_config.get('steering_angle_rate', {})
        
        # 默认截止频率：10Hz/15Hz（保留更多有效信号，同时滤除高频噪声）
        self.angle_filter_cutoff = angle_config.get('lowpass_filter', {}).get('cutoff_hz', 10.0)
        self.rate_filter_cutoff = rate_config.get('lowpass_filter', {}).get('cutoff_hz', 15.0)
        
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
                                     window_duration: float, step: float,
                                     fs: float = None) -> tuple:
        """计算滑动窗口 RMS
        
        Args:
            fs: 实际采样率(Hz)，若为 None 则使用配置值
        """
        if len(values) == 0:
            return np.array([]), np.array([])
        
        actual_fs = fs if fs is not None else self.sampling_rate_hz
        window_size = int(window_duration * actual_fs)
        step_size = int(step * actual_fs)
        
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
        
        Note:
            此方法现在不再从 synced_frames 收集数据，
            而是依赖 main.py 预先收集的高频数据 (chassis_highfreq)。
            保留此方法是为了兼容性，实际数据收集在 main.py 中完成。
        """
        # 高频数据已在 main.py 的 _collect_highfreq_data 中收集到 streaming_data.chassis_highfreq
        # 此处不再需要从 synced_frames 收集，避免重复和下采样
        pass
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算转向平滑度KPI（流式模式）
        
        Note:
            数据来源优先级：
            1. 高频数据 (chassis_highfreq, ~50Hz) - 优先使用
            2. 低频数据 (steering_data, ~10Hz) - 兼容旧模式/缓存模式
        """
        self.clear_results()
        
        # 优先使用高频数据
        if len(streaming_data.chassis_highfreq) > 0:
            # 高频数据: (steering_angle, steering_vel, speed, lat_acc, timestamp)
            # 过滤自动驾驶状态
            auto_indices = [i for i, (is_auto, _) in enumerate(streaming_data.chassis_auto_states) 
                           if is_auto and i < len(streaming_data.chassis_highfreq)]
            
            if len(auto_indices) > 0:
                steering_angles = np.array([streaming_data.chassis_highfreq[i][0] for i in auto_indices])
                speeds = np.array([streaming_data.chassis_highfreq[i][2] for i in auto_indices])
                timestamps = np.array([streaming_data.chassis_highfreq[i][4] for i in auto_indices])
            else:
                # 没有自动驾驶数据
                self._add_empty_results()
                return self.get_results()
        elif len(streaming_data.steering_data) > 0:
            # 回退到低频数据（兼容缓存模式）
            steering_angles = np.array([d[0] for d in streaming_data.steering_data])
            speeds = np.array([d[1] for d in streaming_data.steering_data])
            timestamps = np.array([d[2] for d in streaming_data.steering_data])
        else:
            self._add_empty_results()
            return self.get_results()
        
        # 速度过滤
        speed_mask = speeds >= self.min_speed_kmh
        steering_angles = steering_angles[speed_mask]
        speeds = speeds[speed_mask]
        timestamps = timestamps[speed_mask]
        
        # 动态估计实际采样率
        if len(timestamps) >= 2:
            actual_fs = 1.0 / np.median(np.diff(timestamps))
            actual_fs = max(1.0, min(actual_fs, 100.0))  # 限制在合理范围
        else:
            actual_fs = self.sampling_rate_hz
        
        if len(timestamps) < int(self.window_duration_sec * actual_fs):
            self._add_empty_results()
            return self.get_results()
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 1. 信号预处理：对转角进行低通滤波
        # 注意：滤波截止频率需 < Nyquist (actual_fs/2)，否则跳过滤波
        angle_cutoff = min(self.angle_filter_cutoff, actual_fs / 2 - 0.1)
        if angle_cutoff > 0:
            steering_angles_filtered = self._apply_lowpass_filter(
                steering_angles, angle_cutoff, actual_fs, order=2)
        else:
            steering_angles_filtered = steering_angles
        
        # 2. 计算转角速度（从滤波后的转角计算）
        ts_rate, steering_rates = SignalProcessor.compute_derivative(
            timestamps, steering_angles_filtered)
        
        if len(steering_rates) == 0:
            self._add_empty_results()
            return self.get_results()
        
        # 3. 对转角速度进行低通滤波
        rate_cutoff = min(self.rate_filter_cutoff, actual_fs / 2 - 0.1)
        if rate_cutoff > 0:
            steering_rates_filtered = self._apply_lowpass_filter(
                steering_rates, rate_cutoff, actual_fs, order=2)
        else:
            steering_rates_filtered = steering_rates
        
        # 4. 计算滑动窗口 RMS 并检测超阈值事件
        if self.rms_enabled:
            window_centers, rms_values = self._compute_sliding_window_rms(
                np.abs(steering_rates_filtered), ts_rate,
                self.window_duration_sec, self.window_step_sec, fs=actual_fs)
            
            if len(rms_values) > 0:
                # 统计 RMS 指标
                rms_mean = np.mean(rms_values)
                rms_max = np.max(rms_values)
                rms_p95 = np.percentile(rms_values, 95)
                
                # 将速度对齐到窗口中心时间戳
                speeds_at_windows = np.interp(window_centers, timestamps, speeds)
                
                # 检测超阈值事件（fail级别）- 合并连续的超阈值窗口
                # 过滤低速事件（< min_anomaly_speed_kmh）
                rms_events_raw = []
                for i, (rms_val, ts, spd) in enumerate(zip(rms_values, window_centers, speeds_at_windows)):
                    # 低速过滤
                    if spd < self.min_anomaly_speed_kmh:
                        continue
                    fail_threshold = self._get_threshold('steering_rate_rms', spd, 'fail')
                    if fail_threshold > 0 and rms_val > fail_threshold:
                        rms_events_raw.append({
                            'timestamp': ts,
                            'value': rms_val,
                            'threshold': fail_threshold,
                            'speed': spd
                        })
                
                # 合并相邻的超阈值事件（时间间隔 < 1秒则合并为同一事件）
                merge_gap = 1.0
                rms_events = []
                if len(rms_events_raw) > 0:
                    current_event = {
                        'start_ts': rms_events_raw[0]['timestamp'],
                        'end_ts': rms_events_raw[0]['timestamp'],
                        'peak_value': rms_events_raw[0]['value'],
                        'peak_ts': rms_events_raw[0]['timestamp'],
                        'threshold': rms_events_raw[0]['threshold'],
                        'speed': rms_events_raw[0]['speed'],
                        'count': 1
                    }
                    
                    for i in range(1, len(rms_events_raw)):
                        time_gap = rms_events_raw[i]['timestamp'] - rms_events_raw[i-1]['timestamp']
                        
                        if time_gap <= merge_gap:
                            # 合并到当前事件
                            current_event['end_ts'] = rms_events_raw[i]['timestamp']
                            current_event['count'] += 1
                            # 更新峰值
                            if rms_events_raw[i]['value'] > current_event['peak_value']:
                                current_event['peak_value'] = rms_events_raw[i]['value']
                                current_event['peak_ts'] = rms_events_raw[i]['timestamp']
                                current_event['threshold'] = rms_events_raw[i]['threshold']
                                current_event['speed'] = rms_events_raw[i]['speed']
                        else:
                            # 保存当前事件，开始新事件
                            rms_events.append(current_event)
                            current_event = {
                                'start_ts': rms_events_raw[i]['timestamp'],
                                'end_ts': rms_events_raw[i]['timestamp'],
                                'peak_value': rms_events_raw[i]['value'],
                                'peak_ts': rms_events_raw[i]['timestamp'],
                                'threshold': rms_events_raw[i]['threshold'],
                                'speed': rms_events_raw[i]['speed'],
                                'count': 1
                            }
                    
                    # 保存最后一个事件
                    rms_events.append(current_event)
                
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
                
                # 添加 RMS 超阈值事件（已合并）
                for event in rms_events[:50]:  # 最多50个事件
                    duration_sec = event['end_ts'] - event['start_ts']
                    rms_max_result.add_anomaly(
                        timestamp=event['peak_ts'],
                        bag_name=bag_mapper.get_bag_name(event['peak_ts']),
                        description=f"转角速度RMS超阈值@{event['speed']:.0f}km/h: 峰值{event['peak_value']:.1f}>{event['threshold']:.0f}°/s, 持续{duration_sec:.1f}s",
                        value=event['peak_value'],
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
            
            # 添加 P95 超阈值事件（过滤低速）
            added_count = 0
            for event in p95_events:
                # 低速过滤
                if event['avg_speed'] < self.min_anomaly_speed_kmh:
                    continue
                if added_count >= 50:  # 最多50个事件
                    break
                duration_ms = (event['end_ts'] - event['start_ts']) * 1000
                p95_result.add_anomaly(
                    timestamp=event['start_ts'],
                    bag_name=bag_mapper.get_bag_name(event['start_ts']),
                    description=f"转角速度超阈值@{event['avg_speed']:.0f}km/h: 峰值{event['peak_value']:.1f}>{event['threshold']:.0f}°/s, 持续{duration_ms:.0f}ms/{event['frame_count']}帧",
                    value=event['peak_value'],
                    threshold=event['threshold']
                )
                added_count += 1
            
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
                window_size = int(self.window_duration_sec * actual_fs)
                step_size = int(self.window_step_sec * actual_fs)
                
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
