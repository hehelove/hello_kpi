"""
信号处理工具
用于计算导数、滤波、统计等

行业标准 Jerk 计算方法：
- 支持重采样到均匀时间网格
- Butterworth / Savitzky-Golay 滤波
- 中心差分求导
- 多尺度统计（瞬时、窗口RMS、P95）
"""
import numpy as np
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass
from scipy.signal import butter, filtfilt, savgol_filter


@dataclass
class EventDetection:
    """事件检测结果"""
    start_time: float
    end_time: float
    duration: float
    peak_value: float
    average_value: float


@dataclass
class JerkComputeResult:
    """Jerk 计算结果（包含元数据）"""
    timestamps: np.ndarray      # 时间戳
    values: np.ndarray          # Jerk 值
    filtered_acc: np.ndarray    # 滤波后的加速度
    stats: Dict                 # 统计信息
    params: Dict                # 计算参数（用于可比性）


class SignalProcessor:
    """信号处理器"""
    
    # ==================== 重采样与滤波 ====================
    
    @staticmethod
    def resample_uniform(timestamps: np.ndarray, 
                         values: np.ndarray, 
                         target_dt: float) -> Tuple[np.ndarray, np.ndarray]:
        """
        重采样到均匀时间网格
        
        Args:
            timestamps: 原始时间戳
            values: 原始值
            target_dt: 目标采样间隔 (秒)
            
        Returns:
            (new_timestamps, new_values)
        """
        if len(timestamps) < 2:
            return timestamps, values
        
        t0, t1 = timestamps[0], timestamps[-1]
        t_new = np.arange(t0, t1, target_dt)
        v_new = np.interp(t_new, timestamps, values)
        return t_new, v_new
    
    @staticmethod
    def butter_lowpass_filter(data: np.ndarray, 
                               fs: float, 
                               cutoff: float = 10.0, 
                               order: int = 2) -> np.ndarray:
        """
        Butterworth 低通滤波
        
        Args:
            data: 输入数据
            fs: 采样频率 (Hz)
            cutoff: 截止频率 (Hz)
            order: 滤波器阶数
            
        Returns:
            滤波后的数据
        """
        if len(data) < 10:
            return data
        
        nyq = 0.5 * fs
        # 确保 cutoff 不超过奈奎斯特频率
        normal_cutoff = min(cutoff / nyq, 0.99)
        
        try:
            b, a = butter(order, normal_cutoff, btype='low', analog=False)
            y = filtfilt(b, a, data)
            return y
        except Exception:
            # 如果滤波失败，返回原数据
            return data
    
    @staticmethod
    def savgol_smooth(data: np.ndarray, 
                      fs: float, 
                      window_sec: float = 0.1, 
                      polyorder: int = 3) -> np.ndarray:
        """
        Savitzky-Golay 平滑滤波
        
        Args:
            data: 输入数据
            fs: 采样频率 (Hz)
            window_sec: 窗口时长 (秒)
            polyorder: 多项式阶数
            
        Returns:
            滤波后的数据
        """
        if len(data) < 10:
            return data
        
        # 计算窗口大小（必须为奇数）
        window_len = int(max(5, round(window_sec * fs)))
        if window_len % 2 == 0:
            window_len += 1
        
        # 确保窗口不超过数据长度
        window_len = min(window_len, len(data) - 1)
        if window_len % 2 == 0:
            window_len -= 1
        
        if window_len < 5:
            return data
        
        try:
            return savgol_filter(data, window_len, polyorder)
        except Exception:
            return data
    
    # ==================== Jerk 计算（行业标准） ====================
    
    @staticmethod
    def compute_jerk_robust(timestamps: np.ndarray,
                            accelerations: np.ndarray,
                            target_fs: float = None,
                            filter_method: str = 'butter',
                            cutoff: float = None,
                            max_dt: float = 1.0) -> JerkComputeResult:
        """
        行业标准 Jerk 计算方法
        
        流程：
        1. 重采样到均匀网格（如果需要）
        2. 低通滤波（Butterworth 或 Savitzky-Golay）
        3. 中心差分求导
        4. 计算多尺度统计量
        
        Args:
            timestamps: 时间戳数组
            accelerations: 加速度数组 (m/s²)
            target_fs: 目标采样频率 (Hz)，None 则使用原始采样率
            filter_method: 滤波方法 ('butter', 'savgol', 'none')
            cutoff: 截止频率 (Hz)，None 则自动选择
            max_dt: 最大有效时间间隔 (秒)
            
        Returns:
            JerkComputeResult 包含 jerk 值、滤波后加速度、统计信息和计算参数
        """
        if len(timestamps) < 5:
            return JerkComputeResult(
                timestamps=np.array([]),
                values=np.array([]),
                filtered_acc=np.array([]),
                stats={'error': 'insufficient_data'},
                params={}
            )
        
        timestamps = np.asarray(timestamps, dtype=float)
        accelerations = np.asarray(accelerations, dtype=float)
        
        # 计算原始采样率
        dt_raw = np.median(np.diff(timestamps))
        fs_raw = 1.0 / dt_raw if dt_raw > 0 else 10.0
        
        # 确定目标采样率
        if target_fs is None:
            fs = fs_raw
            dt = dt_raw
            t_rs, a_rs = timestamps, accelerations
        else:
            fs = target_fs
            dt = 1.0 / fs
            t_rs, a_rs = SignalProcessor.resample_uniform(timestamps, accelerations, dt)
        
        # 自动选择截止频率
        if cutoff is None:
            # 推荐：cutoff = min(0.2 * fs, 20 Hz)
            cutoff = min(0.2 * fs, 20.0)
        
        # 滤波
        if filter_method == 'butter':
            a_filt = SignalProcessor.butter_lowpass_filter(a_rs, fs, cutoff=cutoff, order=2)
        elif filter_method == 'savgol':
            a_filt = SignalProcessor.savgol_smooth(a_rs, fs, window_sec=0.1)
        else:
            a_filt = a_rs
        
        # 中心差分求导（更稳定）
        n = len(a_filt)
        jerk = np.zeros(n)
        
        # 中心差分：j[i] = (a[i+1] - a[i-1]) / (2*dt)
        if n > 2:
            jerk[1:-1] = (a_filt[2:] - a_filt[:-2]) / (2.0 * dt)
            # 边界用单边差分
            jerk[0] = (a_filt[1] - a_filt[0]) / dt
            jerk[-1] = (a_filt[-1] - a_filt[-2]) / dt
        
        # 过滤异常值（由于 bag 间隔导致的尖峰）
        # 检测原始时间间隔，标记大间隔位置
        if len(timestamps) > 1:
            dt_orig = np.diff(timestamps)
            gap_indices = np.where(dt_orig > max_dt)[0]
            
            # 在重采样后的时间轴上找到对应位置并置零
            for gap_idx in gap_indices:
                gap_time = timestamps[gap_idx]
                # 找到重采样后最接近的索引
                rs_idx = np.searchsorted(t_rs, gap_time)
                # 清除间隔附近的 jerk 值
                start_clear = max(0, rs_idx - 2)
                end_clear = min(n, rs_idx + 3)
                jerk[start_clear:end_clear] = 0
        
        # 过滤物理不合理的极端值（|jerk| > 15 m/s³ 极少是真实数据）
        # 参考：紧急制动 jerk 约 5-10 m/s³，15 m/s³ 已是极端情况
        MAX_PHYSICAL_JERK = 15.0  # m/s³，物理上限阈值
        outlier_mask = np.abs(jerk) > MAX_PHYSICAL_JERK
        outlier_count = np.sum(outlier_mask)
        jerk_cleaned = np.where(outlier_mask, np.nan, jerk)  # 用 NaN 标记异常点
        
        # 计算统计量（排除 NaN 和零值异常点）
        valid_jerk = jerk_cleaned[np.isfinite(jerk_cleaned)]
        if len(valid_jerk) == 0:
            valid_jerk = np.array([0.0])
        
        stats = {
            'fs': round(fs, 1),
            'dt_ms': round(dt * 1000, 1),
            'cutoff_hz': round(cutoff, 1) if filter_method == 'butter' else None,
            'filter_method': filter_method,
            'sample_count': len(valid_jerk),
            'outlier_count': int(outlier_count),  # 被过滤的极端值数量
            # 瞬时统计
            'j_max': float(np.max(np.abs(valid_jerk))),
            'j_min': float(np.min(valid_jerk)),
            'j_mean': float(np.mean(valid_jerk)),
            'j_abs_mean': float(np.mean(np.abs(valid_jerk))),
            'j_std': float(np.std(valid_jerk)),
            # RMS（整体）
            'j_rms': float(np.sqrt(np.mean(valid_jerk ** 2))),
            # P95
            'j_p95': float(np.percentile(np.abs(valid_jerk), 95)),
            'j_p99': float(np.percentile(np.abs(valid_jerk), 99)),
        }
        
        # 计算零交叉率（用于检测高频振荡/画龙）
        sign_changes = np.sum(np.diff(np.sign(valid_jerk)) != 0)
        duration = (t_rs[-1] - t_rs[0]) if len(t_rs) > 1 else 1.0
        zero_crossing_rate = float(sign_changes / duration) if duration > 0 else 0.0
        stats['zero_crossing_rate'] = round(zero_crossing_rate, 2)  # 次/秒
        
        # 计算 1 秒窗口 RMS（使用清理后的数据，跳过包含 NaN 的窗口）
        window_1s = max(1, round(fs * 1.0))  # 使用 round 确保精确的 1 秒窗口
        # 使用清理后的 jerk 数组（NaN 标记异常点）
        jerk_for_rms = jerk_cleaned.copy()
        
        if len(jerk_for_rms) >= window_1s:
            step = max(1, window_1s // 2)  # 50% 重叠
            rms_1s_values = []
            rms_1s_timestamps = []  # 记录每个窗口的中心时间戳
            
            for start in range(0, len(jerk_for_rms) - window_1s + 1, step):
                window = jerk_for_rms[start:start + window_1s]
                # 跳过包含 NaN 的窗口（这些窗口包含异常数据）
                if np.any(np.isnan(window)):
                    continue
                rms_val = np.sqrt(np.mean(window ** 2))
                rms_1s_values.append(rms_val)
                # 记录窗口中心时间戳
                center_idx = start + window_1s // 2
                rms_1s_timestamps.append(t_rs[center_idx])
            
            if rms_1s_values:
                max_idx = int(np.argmax(rms_1s_values))
                stats['j_rms_1s_max'] = float(rms_1s_values[max_idx])
                stats['j_rms_1s_max_ts'] = float(rms_1s_timestamps[max_idx])  # 新增：最大值时间戳
                stats['j_rms_1s_mean'] = float(np.mean(rms_1s_values))
                stats['j_rms_1s_p95'] = float(np.percentile(rms_1s_values, 95))  # P95
            else:
                # 所有窗口都包含异常，使用整体 RMS
                stats['j_rms_1s_max'] = stats['j_rms']
                stats['j_rms_1s_max_ts'] = float(t_rs[len(t_rs)//2]) if len(t_rs) > 0 else 0.0
                stats['j_rms_1s_mean'] = stats['j_rms']
                stats['j_rms_1s_p95'] = stats['j_rms']
        else:
            stats['j_rms_1s_max'] = stats['j_rms']
            stats['j_rms_1s_max_ts'] = float(t_rs[len(t_rs)//2]) if len(t_rs) > 0 else 0.0
            stats['j_rms_1s_mean'] = stats['j_rms']
            stats['j_rms_1s_p95'] = stats['j_rms']
        
        params = {
            'target_fs': target_fs,
            'actual_fs': round(fs, 1),
            'effective_fs': round(fs, 1),  # 实际使用的采样率（与 actual_fs 相同，显式记录）
            'filter_method': filter_method,
            'cutoff': cutoff,
            'raw_fs': round(fs_raw, 1),
            'window_1s_samples': window_1s  # 1秒窗口的实际样本数
        }
        
        # 返回时用 0 替换 NaN（便于后续处理）
        jerk_output = np.where(np.isnan(jerk_cleaned), 0.0, jerk_cleaned)
        
        return JerkComputeResult(
            timestamps=t_rs,
            values=jerk_output,
            filtered_acc=a_filt,
            stats=stats,
            params=params
        )
    
    @staticmethod
    def compute_derivative(timestamps: np.ndarray, 
                           values: np.ndarray,
                           max_dt: float = 1.0) -> Tuple[np.ndarray, np.ndarray]:
        """
        计算一阶导数 (微分) - 简单版本，向后兼容
        
        Args:
            timestamps: 时间戳数组
            values: 值数组
            max_dt: 最大允许的时间间隔（秒），超过此值的点会被过滤
            
        Returns:
            (timestamps, derivatives) - 导数对应的时间戳和导数值
        """
        if len(timestamps) < 2:
            return np.array([]), np.array([])
        
        dt = np.diff(timestamps)
        dv = np.diff(values)
        
        # 过滤时间间隔过大的点（bag 间隔）
        valid_mask = (dt > 0) & (dt <= max_dt)
        
        if not np.any(valid_mask):
            return np.array([]), np.array([])
        
        dt = dt[valid_mask]
        dv = dv[valid_mask]
        
        # 避免除以零
        dt[dt == 0] = 1e-10
        
        derivatives = dv / dt
        
        # 导数对应的时间戳取中点
        mid_timestamps = (timestamps[:-1] + timestamps[1:]) / 2
        mid_timestamps = mid_timestamps[valid_mask]
        
        return mid_timestamps, derivatives
    
    @staticmethod
    def compute_jerk(timestamps: np.ndarray, 
                     accelerations: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        计算jerk (加速度的导数) - 简单版本，向后兼容
        
        Args:
            timestamps: 时间戳数组
            accelerations: 加速度数组
            
        Returns:
            (timestamps, jerks) - jerk对应的时间戳和jerk值
        """
        return SignalProcessor.compute_derivative(timestamps, accelerations)
    
    # ==================== 其他工具方法 ====================
    
    @staticmethod
    def moving_average(values: np.ndarray, window_size: int) -> np.ndarray:
        """
        移动平均滤波
        
        Args:
            values: 输入值数组
            window_size: 窗口大小
            
        Returns:
            滤波后的值
        """
        if len(values) < window_size:
            return values
        
        kernel = np.ones(window_size) / window_size
        return np.convolve(values, kernel, mode='same')
    
    @staticmethod
    def low_pass_filter(values: np.ndarray, 
                        cutoff_ratio: float = 0.1) -> np.ndarray:
        """
        简单低通滤波 (指数平滑) - 向后兼容
        
        Args:
            values: 输入值数组
            cutoff_ratio: 截止比率 (0-1)
            
        Returns:
            滤波后的值
        """
        alpha = cutoff_ratio
        filtered = np.zeros_like(values)
        filtered[0] = values[0]
        
        for i in range(1, len(values)):
            filtered[i] = alpha * values[i] + (1 - alpha) * filtered[i-1]
        
        return filtered
    
    @staticmethod
    def detect_threshold_events(timestamps: np.ndarray,
                                 values: np.ndarray,
                                 threshold: float,
                                 min_duration: float,
                                 compare: str = 'greater') -> List[EventDetection]:
        """
        检测超过阈值的事件
        
        Args:
            timestamps: 时间戳数组
            values: 值数组
            threshold: 阈值
            min_duration: 最小持续时间 (秒)
            compare: 比较方式 ('greater', 'less', 'abs_greater')
            
        Returns:
            事件列表
        """
        if len(timestamps) == 0:
            return []
        
        # 确定是否超过阈值
        if compare == 'greater':
            over_threshold = values > threshold
        elif compare == 'less':
            over_threshold = values < threshold
        elif compare == 'abs_greater':
            over_threshold = np.abs(values) > threshold
        else:
            raise ValueError(f"Unknown compare mode: {compare}")
        
        events = []
        in_event = False
        event_start_idx = 0
        
        for i in range(len(over_threshold)):
            if over_threshold[i] and not in_event:
                # 事件开始
                in_event = True
                event_start_idx = i
            elif not over_threshold[i] and in_event:
                # 事件结束
                in_event = False
                event_end_idx = i - 1
                
                start_time = timestamps[event_start_idx]
                end_time = timestamps[event_end_idx]
                duration = end_time - start_time
                
                if duration >= min_duration:
                    event_values = values[event_start_idx:event_end_idx+1]
                    events.append(EventDetection(
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration,
                        peak_value=float(np.max(np.abs(event_values))),
                        average_value=float(np.mean(np.abs(event_values)))
                    ))
        
        # 检查是否在事件中结束
        if in_event:
            start_time = timestamps[event_start_idx]
            end_time = timestamps[-1]
            duration = end_time - start_time
            
            if duration >= min_duration:
                event_values = values[event_start_idx:]
                events.append(EventDetection(
                    start_time=start_time,
                    end_time=end_time,
                    duration=duration,
                    peak_value=float(np.max(np.abs(event_values))),
                    average_value=float(np.mean(np.abs(event_values)))
                ))
        
        return events
    
    @staticmethod
    def detect_oscillation(timestamps: np.ndarray,
                           values: np.ndarray,
                           amplitude_threshold: float,
                           window_size: int = 10) -> List[EventDetection]:
        """
        检测振荡/波动事件 (画龙检测)
        
        Args:
            timestamps: 时间戳数组
            values: 值数组 (如横向偏移)
            amplitude_threshold: 振幅阈值
            window_size: 检测窗口大小
            
        Returns:
            振荡事件列表
        """
        if len(values) < window_size:
            return []
        
        events = []
        
        # 使用滑动窗口检测
        for i in range(len(values) - window_size):
            window_values = values[i:i+window_size]
            
            # 计算窗口内的振幅 (峰峰值)
            amplitude = np.max(window_values) - np.min(window_values)
            
            if amplitude > amplitude_threshold:
                # 检测到振荡
                start_time = timestamps[i]
                end_time = timestamps[i + window_size - 1]
                
                events.append(EventDetection(
                    start_time=start_time,
                    end_time=end_time,
                    duration=end_time - start_time,
                    peak_value=amplitude,
                    average_value=np.mean(np.abs(window_values))
                ))
        
        # 合并相邻的事件
        return SignalProcessor._merge_adjacent_events(events, min_gap=0.5)
    
    @staticmethod
    def _merge_adjacent_events(events: List[EventDetection], 
                                min_gap: float) -> List[EventDetection]:
        """合并相邻的事件"""
        if len(events) <= 1:
            return events
        
        merged = [events[0]]
        
        for event in events[1:]:
            if event.start_time - merged[-1].end_time < min_gap:
                # 合并事件
                merged[-1] = EventDetection(
                    start_time=merged[-1].start_time,
                    end_time=event.end_time,
                    duration=event.end_time - merged[-1].start_time,
                    peak_value=max(merged[-1].peak_value, event.peak_value),
                    average_value=(merged[-1].average_value + event.average_value) / 2
                )
            else:
                merged.append(event)
        
        return merged
    
    @staticmethod
    def compute_statistics(values: np.ndarray) -> dict:
        """
        计算统计量
        
        Args:
            values: 值数组
            
        Returns:
            统计量字典
        """
        if len(values) == 0:
            return {
                'mean': 0.0,
                'std': 0.0,
                'min': 0.0,
                'max': 0.0,
                'median': 0.0,
                'abs_mean': 0.0,
                'rms': 0.0,
                'p95': 0.0,
                'p99': 0.0
            }
        
        return {
            'mean': float(np.mean(values)),
            'std': float(np.std(values)),
            'min': float(np.min(values)),
            'max': float(np.max(values)),
            'median': float(np.median(values)),
            'abs_mean': float(np.mean(np.abs(values))),
            'rms': float(np.sqrt(np.mean(values ** 2))),
            'p95': float(np.percentile(np.abs(values), 95)),
            'p99': float(np.percentile(np.abs(values), 99))
        }


class CurvatureEstimator:
    """曲率估计器"""
    
    @staticmethod
    def estimate_road_curvature(steering_angle: float,
                                  wheelbase: float = 2.85) -> float:
        """
        从方向盘转角估计道路曲率
        
        Args:
            steering_angle: 方向盘转角 (度)
            wheelbase: 轴距 (米)
            
        Returns:
            曲率 (1/m)
        """
        # 假设转向比为16:1
        steering_ratio = 16.0
        wheel_angle = np.radians(steering_angle / steering_ratio)
        
        if abs(wheel_angle) < 1e-6:
            return 0.0
        
        # 曲率 = tan(转向角) / 轴距
        curvature = np.tan(wheel_angle) / wheelbase
        
        return float(curvature)
    
    @staticmethod
    def classify_road_type(curvature: float,
                           threshold: float = 0.002) -> str:
        """
        根据曲率分类道路类型
        
        Args:
            curvature: 曲率 (1/m)
            threshold: 直道阈值
            
        Returns:
            'straight' 或 'curve'
        """
        if abs(curvature) < threshold:
            return 'straight'
        else:
            return 'curve'
