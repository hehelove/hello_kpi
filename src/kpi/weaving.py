"""
画龙检测KPI（L4 级别）
检测自动驾驶横向控制异常（lateral oscillation）

核心思想：画龙 = 行驶时方向盘频繁左右来回打（持续振荡且幅度明显）

判定方法（滑动5s窗口，所有条件同时满足）：
┌─────────────────────────────────────────────────────────────────┐
│  ① 场景过滤                                                     │
│     • 窗口内 90% 以上的点速度 >= 5 km/h                         │
│     • 不过滤曲率（弯道中也可能发生画龙）                        │
│                                                                 │
│  ② 振荡检测（有效过零点）                                        │
│     • 窗口内有效过零点 >= 4 次（2个完整振荡周期）               │
│     • 有效过零定义：过零前后 0.5s 内转角速度峰值都超阈值        │
│       - 低速(<30km/h): >= 20°/s                                 │
│       - 中速(30-60km/h): >= 15°/s                               │
│       - 高速(>60km/h): >= 10°/s                                 │
│     • 过滤微小波动（如 ±2°/s），只保留明显的方向切换            │
│                                                                 │
│  ③ 幅度要求                                                     │
│     • 转角峰谷差 >= 40°                                        │
│     • 转角速度 RMS >= 阈值×0.8（动态阈值）                      │
│                                                                 │
│  ④ 排除正常转弯                                                 │
│     • 转角变化 >80% 同向 → 认为是转弯/变道，排除                │
│     • 原理：正常转弯是持续单向转动，画龙是来回振荡              │
└─────────────────────────────────────────────────────────────────┘

事件提取：
  • 连续满足条件的窗口合并为一个事件
  • 事件合并 hysteresis：允许短暂 gap（0.5s），避免边界抖动
  • 最终事件需满足：
    - 持续时间 >= 2s
    - 振荡周期 >= 2（有效过零点/2）
    - 振荡密度 >= 0.6 * (min_zero_crossings / window_duration)

数据来源：
  • 优先使用高频底盘数据（~50Hz chassis_highfreq）
  • 回退到同步帧数据（~10Hz weaving_data）
  • 位置/曲率从低频数据插值到高频时间戳
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np
import os
import json

from scipy import signal

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor


@dataclass
class WeavingEvent:
    """
    画龙事件（L4标准：横向控制振荡）
    
    核心判定字段（参与检测逻辑）：
      - oscillation_count: 有效过零点次数
      - max_rms: 转角速度 RMS (°/s)
      - max_steering_rate: 最大转角速度 (°/s)
    
    记录字段（仅用于事件记录/调试，不参与判定）：
      - max_steering_acc: 角加速度（备用）
      - max_lateral_error_rate: 横向误差变化率（备用，可用于轨迹级判定）
      - max_lateral_acc: 横向加速度（备用）
    """
    start_time: float
    end_time: float
    # ===== 核心字段（参与判定）=====
    max_steering_rate: float      # 最大方向盘角速度 (deg/s) - 核心
    oscillation_count: int        # 有效过零点次数 - 核心
    max_rms: float = 0.0          # 最大 RMS(|δ̇|) (deg/s) - 核心，L4标准
    # ===== 记录字段（不参与判定，仅记录）=====
    max_steering_acc: float = 0.0       # 最大方向盘角加速度 (deg/s²) - 备用
    max_lateral_error_rate: float = 0.0 # 最大横向误差变化率 (m/s) - 备用，可用于轨迹级判定
    max_lateral_acc: float = 0.0        # 最大横向加速度 (m/s²) - 备用
    rms_threshold: float = 0.0          # RMS 阈值（用于报告对齐）
    trajectory: List[Dict] = None       # 轨迹点列表 [{lat, lon, timestamp, steering_vel}]
    
    @property
    def duration(self) -> float:
        return self.end_time - self.start_time
    
    def __post_init__(self):
        if self.trajectory is None:
            self.trajectory = []


class WeavingKPI(BaseKPI):
    """
    画龙检测KPI
    
    基于行业标准的横向控制异常检测
    """
    
    @property
    def name(self) -> str:
        return "画龙检测"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/vehicle/chassis_domain_report",
            "/control/debug",  # 用于获取横向误差
            "/localization/localization"  # 用于检查定位可信度
        ]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]
    
    @property
    def provides(self) -> List[str]:
        return ["画龙次数", "画龙频率"]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        weaving_config = self.config.get('kpi', {}).get('weaving', {})
        
        # ========== 场景过滤参数 ==========
        # 最小速度：低于此速度不检测画龙（排除低速调头、泊车等）
        self.min_speed = weaving_config.get('min_speed', 5.0)  # km/h（原10，放宽到5）
        
        # ========== 振荡检测参数 ==========
        # 检测窗口：在多长时间内统计过零点次数
        self.window_duration = weaving_config.get('window_duration', 5.0)  # 秒
        
        # 最小过零点次数：窗口内至少要有这么多次过零点才算振荡
        # 4次过零 = 2个完整周期
        self.min_zero_crossings = weaving_config.get('min_zero_crossings', 4)
        
        # ========== 幅度阈值（与速度相关）==========
        # 低速阈值（< 30 km/h）
        self.steering_rate_low_speed = weaving_config.get('steering_rate_low_speed', 20.0)  # deg/s
        # 中速阈值（30-60 km/h）
        self.steering_rate_mid_speed = weaving_config.get('steering_rate_mid_speed', 15.0)  # deg/s
        # 高速阈值（> 60 km/h）
        self.steering_rate_high_speed = weaving_config.get('steering_rate_high_speed', 10.0)  # deg/s
        # 兼容旧配置
        self.steering_rate_threshold = weaving_config.get('steering_rate_threshold', 15.0)  # deg/s
        
        # 有效过零点阈值：过零前后峰值需 >= 此值才算有效过零
        self.effective_zero_threshold = weaving_config.get('effective_zero_threshold', 10.0)  # deg/s
        
        # ========== 幅度要求 ==========
        # 转角峰谷差阈值：振荡幅度需 >= 此值才算画龙（振荡足够明显）
        # L4 标准：40° 是用户体验可接受的下限
        self.min_steering_amplitude = weaving_config.get('min_steering_amplitude', 40.0)  # deg
        
        # 事件合并 hysteresis：允许短暂 gap，避免边界抖动导致事件碎片化
        self.event_merge_gap = weaving_config.get('event_merge_gap', 0.5)  # 秒
        
        # ========== 持续性要求 ==========
        # 最小持续时间
        self.min_duration = weaving_config.get('min_duration', 2.0)  # 秒
        
        # 最大事件持续时间：超过此时长的事件会被自动分割
        # 真实的画龙事件通常不会持续超过 10 秒
        self.max_event_duration = weaving_config.get('max_event_duration', 10.0)  # 秒
        
        # 最小振荡周期数
        self.min_oscillation_cycles = weaving_config.get('min_oscillation_cycles', 2)
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]  # 有效的定位状态
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
        
        # 转角速度滤波配置（画龙检测用更高截止频率，保留更多有效信号）
        weaving_filter_config = weaving_config.get('filter', {})
        self.sampling_rate_hz = weaving_filter_config.get('sampling_rate_hz', 50)
        self.angle_filter_cutoff = weaving_filter_config.get('angle_cutoff_hz', 10.0)  # 转角滤波 10Hz
        self.rate_filter_cutoff = weaving_filter_config.get('rate_cutoff_hz', 15.0)    # 转角速度滤波 15Hz
        
        # 数据缓存
        self._debug_timestamps = None
    
    def _apply_lowpass_filter(self, data: np.ndarray, cutoff_hz: float, fs: float, order: int = 2) -> np.ndarray:
        """
        应用 Butterworth 低通滤波（与 steering.py 一致）
        
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
    
    def _find_nearest_debug_data(self, parsed_data: Dict, target_ts: float, 
                                   key: str, tolerance: float = 0.05) -> Optional[float]:
        """根据时间戳查找最近的调试数据"""
        if not parsed_data:
            return None
        
        if self._debug_timestamps is None:
            self._debug_timestamps = sorted(parsed_data.keys())
        
        timestamps = self._debug_timestamps
        if not timestamps:
            return None
            
        left, right = 0, len(timestamps) - 1
        
        while left < right:
            mid = (left + right) // 2
            if timestamps[mid] < target_ts:
                left = mid + 1
            else:
                right = mid
        
        best_ts = None
        best_diff = float('inf')
        
        for idx in [left - 1, left, left + 1]:
            if 0 <= idx < len(timestamps):
                diff = abs(timestamps[idx] - target_ts)
                if diff < best_diff:
                    best_diff = diff
                    best_ts = timestamps[idx]
        
        if best_ts is not None and best_diff <= tolerance:
            return parsed_data.get(best_ts, {}).get(key)
        
        return None
    
    def compute(self, synced_frames: List, 
                parsed_debug_data: Optional[Dict] = None,
                **kwargs) -> List[KPIResult]:
        """计算画龙检测KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, 
                                           parsed_debug_data=parsed_debug_data,
                                           **kwargs)
    
    def _get_steering_rate_threshold(self, speed_kmh: float) -> float:
        """根据速度获取转角速度阈值"""
        if speed_kmh < 30:
            return self.steering_rate_low_speed
        elif speed_kmh < 60:
            return self.steering_rate_mid_speed
        else:
            return self.steering_rate_high_speed
    
    def _detect_weaving_events(self,
                                timestamps: np.ndarray,
                                steering_angles: np.ndarray,
                                steering_velocities: np.ndarray,
                                ts_acc: np.ndarray,
                                steering_accs: np.ndarray,
                                lateral_errors: np.ndarray,
                                ts_err_rate: np.ndarray,
                                lateral_error_rates: np.ndarray,
                                lateral_accs: np.ndarray,
                                speeds: np.ndarray,
                                latitudes: np.ndarray = None,
                                longitudes: np.ndarray = None,
                                curvatures: np.ndarray = None) -> List[WeavingEvent]:
        """
        画龙检测核心算法
        
        检测条件（滑动窗口内同时满足）：
          ① 场景：90% 以上的点速度 >= min_speed
          ② 振荡：有效过零点 >= min_zero_crossings
          ③ 幅度：转角峰谷差 >= min_steering_amplitude，RMS >= 动态阈值×0.8
          ④ 排除转弯：转角单向变化比例 <= 80%
        
        事件提取：连续满足窗口合并，允许 0.5s gap (hysteresis)
        """
        events = []
        
        if len(timestamps) < 10:
            return events
        
        # 计算采样间隔
        dt = np.median(np.diff(timestamps))
        if dt <= 0:
            dt = 0.1
        
        # ========== 参数 ==========
        window_samples = max(int(self.window_duration / dt), 10)  # 窗口大小（采样点数）
        max_gap_sec = 0.5  # 最大允许时间间隔（超过则认为数据中断）
        
        # ========== 第一步：场景过滤 ==========
        # 注意：不过滤曲率，弯道中也可能发生画龙
        # 通过转角振幅阈值（40°）来排除正常转弯操作
        
        # ========== 第二步：检测有效过零点 ==========
        # 有效过零：过零前后的转角速度峰值都必须超过阈值
        # 过滤微小波动（如 +2 → -2），只保留明显的方向切换（如 +10 → -10）
        
        signs = np.sign(steering_velocities)
        # 处理零值：继承前一个符号
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        
        # 标记过零点位置（符号发生变化的点）
        zero_crossings_raw = np.zeros(len(signs), dtype=bool)
        zero_crossings_raw[1:] = (signs[1:] != signs[:-1])
        
        # 过滤微小过零：检查过零前后是否有足够大的幅度
        # 在每个过零点前后各 0.5 秒内，检查是否有超过阈值的峰值
        half_window = max(int(0.5 / dt), 5)  # 0.5秒或至少5个点
        min_absolute_rate = 5.0  # 绝对最小阈值，过滤极小切换（±2~5°/s）
        
        # 动态阈值：按速度分级判断有效过零
        effective_zero_crossings = []
        for i in range(1, len(zero_crossings_raw)):
            if not zero_crossings_raw[i]:
                continue
            
            before_start = max(0, i - half_window)
            after_end = min(len(steering_velocities), i + half_window)
            
            # 计算过零前后0.5s内的平均速度
            window_speeds = speeds[before_start:after_end]
            avg_speed = float(np.mean(window_speeds))
            
            # 动态阈值（按速度分级）
            threshold = self._get_steering_rate_threshold(avg_speed)
            
            before_peak = np.max(np.abs(steering_velocities[before_start:i]))
            after_peak = np.max(np.abs(steering_velocities[i:after_end]))
            
            # 两侧峰值都需超过动态阈值，且超过绝对最小阈值
            if (before_peak >= threshold and after_peak >= threshold and
                before_peak >= min_absolute_rate and after_peak >= min_absolute_rate):
                effective_zero_crossings.append(i)
        
        # 构建有效过零点布尔数组（供窗口检测使用）
        zero_crossings = np.zeros(len(signs), dtype=bool)
        for idx in effective_zero_crossings:
            zero_crossings[idx] = True
        
        # ========== 第三步：滑动窗口检测 ==========
        is_weaving = np.zeros(len(timestamps), dtype=bool)
        
        for i in range(window_samples, len(timestamps)):
            window_start = i - window_samples
            window_end = i
            
            # 检查窗口内是否有数据中断
            window_ts = timestamps[window_start:window_end]
            ts_gaps = np.diff(window_ts)
            if np.any(ts_gaps > max_gap_sec):
                continue  # 有中断，跳过
            
            # ===== 条件①：场景过滤 =====
            # 窗口内速度检查：平均速度 >= 8km/h 且 80% 以上的点速度 >= min_speed
            # 避免低速边缘误触发
            window_speeds = speeds[window_start:window_end]
            avg_window_speed = np.mean(window_speeds)
            if avg_window_speed < 8.0:
                continue  # 平均速度太低，跳过
            speed_ok_ratio = np.mean(window_speeds >= self.min_speed)
            if speed_ok_ratio < 0.8:
                continue  # 超过20%的时间速度太低，跳过
            
            # 注意：不检查曲率，弯道中也可能发生画龙
            
            # ===== 条件②：振荡检测 =====
            window_zero_crossings = np.sum(zero_crossings[window_start:window_end])
            if window_zero_crossings < self.min_zero_crossings:
                continue  # 过零点不够，不是振荡
            
            # ===== 条件③：幅度要求 =====
            window_angles = steering_angles[window_start:window_end]
            steering_amplitude = np.max(window_angles) - np.min(window_angles)
            
            # 条件③-a：转角峰谷差 >= min_steering_amplitude（振荡幅度足够明显）
            if steering_amplitude < self.min_steering_amplitude:
                continue  # 振荡幅度太小（< 40°），不是明显的画龙
            
            # 条件③-b：转角速度 RMS 超过阈值（过滤小幅修正方向）
            # 真正的画龙方向盘打得较快，RMS 需满足动态阈值
            window_steering_vel = steering_velocities[window_start:window_end]
            rms_steering_vel = np.sqrt(np.mean(window_steering_vel ** 2))
            avg_speed = float(np.mean(window_speeds))
            rms_threshold = self._get_steering_rate_threshold(avg_speed) * 0.8  # 稍微放宽
            if rms_steering_vel < rms_threshold:
                continue  # 转角速度太低，只是小幅修正不是画龙
            
            # ===== 条件④：排除正常转弯（单向持续转动）=====
            # 计算转角的单调性：如果转角持续单向变化，可能是正常转弯
            angle_diff = np.diff(window_angles)
            positive_ratio = np.mean(angle_diff > 0)
            # 如果超过 70% 的点是单向变化，认为是转弯（放宽到70%以捕获急转弯）
            if positive_ratio > 0.7 or positive_ratio < 0.3:
                continue
            
            # ===== 条件⑤：排除急转弯（净转角变化大）=====
            # 画龙的特点是来回振荡，净转角变化接近0
            # 急转弯（转过去→回正）会有明显的净转角变化
            net_angle_change = abs(window_angles[-1] - window_angles[0])
            if net_angle_change > 15.0:  # 净转角变化 > 15° 认为是转弯
                continue
            
            # 所有条件都满足
            is_weaving[i] = True
        
        # ========== 第四步：提取连续的画龙区间（带 hysteresis + 最大时长限制） ==========
        # hysteresis：允许短暂 gap，避免边界抖动导致事件碎片化
        # max_event_duration：防止事件过长，超过时自动分割
        in_event = False
        event_start_idx = 0
        gap_start_idx = 0  # gap 开始位置
        gap_duration = 0.0  # 当前 gap 持续时间
        
        for i in range(len(is_weaving)):
            # 检查时间连续性
            if i > 0:
                time_gap = timestamps[i] - timestamps[i-1]
                if time_gap > max_gap_sec:
                    # 数据中断，结束当前事件
                    if in_event:
                        self._finalize_simple_event(
                            events, timestamps, steering_velocities, lateral_accs,
                            event_start_idx, gap_start_idx - 1 if gap_start_idx > event_start_idx else i - 1, 
                            latitudes, longitudes, speeds, effective_zero_crossings
                        )
                        in_event = False
                        gap_duration = 0.0
                    continue
            
            if is_weaving[i]:
                if not in_event:
                    in_event = True
                    event_start_idx = i
                else:
                    # 检查事件是否超过最大持续时间（提前分割，优化性能）
                    event_duration = timestamps[i] - timestamps[event_start_idx]
                    if event_duration > self.max_event_duration:
                        # 超过最大时长，结束当前事件并开始新事件
                        self._finalize_simple_event(
                            events, timestamps, steering_velocities, lateral_accs,
                            event_start_idx, i - 1, 
                            latitudes, longitudes, speeds, effective_zero_crossings
                        )
                        # 开始新事件
                        event_start_idx = i
                # 重置 gap 计数（回到事件中）
                gap_duration = 0.0
            else:
                if in_event:
                    # 进入 gap 状态
                    if gap_duration == 0.0:
                        gap_start_idx = i  # 记录 gap 开始位置
                    
                    gap_duration = timestamps[i] - timestamps[gap_start_idx]
                    
                    # 检查 gap 是否超过 hysteresis 阈值
                    if gap_duration > self.event_merge_gap:
                        # gap 太长，结束事件（事件结束于 gap 开始前）
                        self._finalize_simple_event(
                            events, timestamps, steering_velocities, lateral_accs,
                            event_start_idx, gap_start_idx - 1, 
                            latitudes, longitudes, speeds, effective_zero_crossings
                        )
                        in_event = False
                        gap_duration = 0.0
        
        # 处理最后一个事件
        if in_event:
            end_idx = len(timestamps) - 1
            # 如果结束在 gap 中，回退到 gap 开始前
            if gap_duration > 0 and gap_start_idx > event_start_idx:
                end_idx = gap_start_idx - 1
            self._finalize_simple_event(
                events, timestamps, steering_velocities, lateral_accs,
                event_start_idx, end_idx, 
                latitudes, longitudes, speeds, effective_zero_crossings
            )
        
        return events
    
    def _finalize_simple_event(self, events: List[WeavingEvent],
                                timestamps: np.ndarray,
                                steering_velocities: np.ndarray,
                                lateral_accs: np.ndarray,
                                start_idx: int,
                                end_idx: int,
                                latitudes: np.ndarray = None,
                                longitudes: np.ndarray = None,
                                speeds: np.ndarray = None,
                                effective_zero_crossings: List[int] = None):
        """
        完成画龙事件的记录
        
        Args:
            effective_zero_crossings: 有效过零点索引列表（复用检测阶段的结果）
        """
        if end_idx <= start_idx:
            return
        
        start_time = timestamps[start_idx]
        end_time = timestamps[end_idx]
        duration = end_time - start_time
        
        # 检查持续时间
        if duration < self.min_duration:
            return
        
        # 如果事件超过最大持续时间，分割成多个子事件
        if duration > self.max_event_duration:
            # 找到分割点（约 max_event_duration 处）
            split_time = start_time + self.max_event_duration
            split_idx = start_idx
            for i in range(start_idx, end_idx + 1):
                if timestamps[i] >= split_time:
                    split_idx = i
                    break
            
            if split_idx > start_idx and split_idx < end_idx:
                # 递归处理前半部分
                self._finalize_simple_event(
                    events, timestamps, steering_velocities, lateral_accs,
                    start_idx, split_idx - 1,
                    latitudes, longitudes, speeds, effective_zero_crossings
                )
                # 递归处理后半部分
                self._finalize_simple_event(
                    events, timestamps, steering_velocities, lateral_accs,
                    split_idx, end_idx,
                    latitudes, longitudes, speeds, effective_zero_crossings
                )
                return
        
        # 计算统计信息
        segment_vel = steering_velocities[start_idx:end_idx+1]
        segment_lat_acc = lateral_accs[start_idx:end_idx+1]
        
        max_steering_rate = float(np.max(np.abs(segment_vel)))
        max_lateral_acc = float(np.max(np.abs(segment_lat_acc)))
        
        # 计算平均速度
        avg_speed = 0.0
        if speeds is not None:
            segment_speeds = speeds[start_idx:end_idx+1]
            avg_speed = float(np.mean(segment_speeds))
        
        # 计算 RMS（取事件内窗口 RMS 最大值，用于与检测阈值对齐）
        if len(segment_vel) >= 2:
            seg_dt = np.median(np.diff(timestamps[start_idx:end_idx+1]))
            if seg_dt <= 0:
                seg_dt = 0.1
            window_samples = max(int(self.window_duration / seg_dt), 2)
            if len(segment_vel) >= window_samples:
                rms_values = []
                for s in range(0, len(segment_vel) - window_samples + 1):
                    window = segment_vel[s:s + window_samples]
                    rms_values.append(np.sqrt(np.mean(window ** 2)))
                rms = float(np.max(rms_values)) if rms_values else float(np.sqrt(np.mean(segment_vel ** 2)))
            else:
                rms = float(np.sqrt(np.mean(segment_vel ** 2)))
        else:
            rms = 0.0

        # RMS 阈值（用于事件过滤与报告）
        rms_threshold = self._get_steering_rate_threshold(avg_speed) * 0.8  # 稍微放宽
        if rms < rms_threshold:
            return  # RMS太低，不是真正的画龙
        
        # 计算有效过零点次数（复用检测阶段的结果，避免把边缘抖动算进去）
        if effective_zero_crossings is not None:
            # 统计落在事件区间内的有效过零点
            oscillation_count = sum(1 for idx in effective_zero_crossings 
                                   if start_idx <= idx <= end_idx)
        else:
            # 兜底：使用符号翻转（不推荐）
            signs = np.sign(segment_vel)
            for i in range(1, len(signs)):
                if signs[i] == 0:
                    signs[i] = signs[i-1]
            oscillation_count = int(np.sum(signs[1:] != signs[:-1]))
        
        # 检查振荡周期数（有效过零点/2 = 周期数）
        if oscillation_count < self.min_oscillation_cycles * 2:
            return  # 振荡周期不够

        # 检查振荡密度（避免长时段低频翻转被误判）
        # 期望密度约等于 (min_zero_crossings / window_duration)
        # 提高到0.8，更严格过滤稀疏振荡
        if duration > 0:
            oscillation_rate = oscillation_count / duration  # 次/秒
            required_rate = self.min_zero_crossings / self.window_duration
            if oscillation_rate < required_rate * 0.8:
                return
        
        # 横向加速度辅助过滤：真正的画龙会产生明显的横向加速度
        if max_lateral_acc < 0.3:  # 极小横向加速度，可能是假阳性
            return
        
        # 提取轨迹数据（用于可视化）
        trajectory = []
        if latitudes is not None and longitudes is not None:
            segment_ts = timestamps[start_idx:end_idx+1]
            segment_lat = latitudes[start_idx:end_idx+1]
            segment_lon = longitudes[start_idx:end_idx+1]
            
            for i in range(len(segment_ts)):
                # 只保留有效的位置点
                if segment_lat[i] != 0 and segment_lon[i] != 0:
                    trajectory.append({
                        'lat': float(segment_lat[i]),
                        'lon': float(segment_lon[i]),
                        'timestamp': float(segment_ts[i]),
                        'steering_vel': float(segment_vel[i]),
                        'speed': float(segment_speeds[i]) if speeds is not None else 0.0
                    })
        
        events.append(WeavingEvent(
            start_time=start_time,
            end_time=end_time,
            max_steering_rate=max_steering_rate,
            max_steering_acc=0.0,
            max_lateral_error_rate=0.0,
            max_lateral_acc=max_lateral_acc,
            oscillation_count=oscillation_count,
            max_rms=rms,
            rms_threshold=rms_threshold,
            trajectory=trajectory
        ))
    
    def _generate_weaving_map(self, weaving_events: List[WeavingEvent], output_dir: str):
        """
        生成画龙事件的轨迹地图可视化
        
        为每个画龙事件生成一个 HTML 地图文件，显示事件期间的车辆轨迹
        轨迹颜色根据转角速度大小渐变
        """
        # 创建输出目录
        weaving_viz_dir = os.path.join(output_dir, 'weaving_viz')
        os.makedirs(weaving_viz_dir, exist_ok=True)
        
        # 检查是否有有效轨迹的事件
        events_with_trajectory = [e for e in weaving_events if e.trajectory and len(e.trajectory) >= 2]
        
        if not events_with_trajectory:
            print(f"    [画龙可视化] 没有包含有效轨迹的画龙事件")
            return
        
        # 为每个事件生成地图
        for i, event in enumerate(events_with_trajectory):
            output_path = os.path.join(weaving_viz_dir, f'weaving_event_{i+1:02d}.html')
            self._generate_single_weaving_map(event, i + 1, output_path)
        
        # 生成汇总地图（所有画龙事件）
        all_trajectory_map_path = os.path.join(weaving_viz_dir, 'weaving_all_events.html')
        self._generate_all_weaving_map(events_with_trajectory, all_trajectory_map_path)
        
        print(f"    [画龙可视化] 已生成 {len(events_with_trajectory)} 个画龙事件地图到 {weaving_viz_dir}")
    
    def _generate_single_weaving_map(self, event: WeavingEvent, event_idx: int, output_path: str):
        """生成单个画龙事件的地图"""
        trajectory = event.trajectory
        if not trajectory or len(trajectory) < 2:
            return
        
        # 中心点
        center_lat = trajectory[len(trajectory) // 2]['lat']
        center_lon = trajectory[len(trajectory) // 2]['lon']
        
        # 转换为 GeoJSON
        # 轨迹线
        line_coords = [[p['lon'], p['lat']] for p in trajectory]
        line_feature = {
            "type": "Feature",
            "properties": {},
            "geometry": {
                "type": "LineString",
                "coordinates": line_coords
            }
        }
        
        # 轨迹点（带转角速度信息）
        point_features = []
        max_steering_vel = max(abs(p['steering_vel']) for p in trajectory)
        
        for j, p in enumerate(trajectory):
            # 归一化转角速度用于颜色渐变 (0-1)
            normalized_vel = abs(p['steering_vel']) / max_steering_vel if max_steering_vel > 0 else 0
            
            point_features.append({
                "type": "Feature",
                "properties": {
                    "index": j,
                    "steering_vel": round(p['steering_vel'], 1),
                    "normalized": round(normalized_vel, 3),
                    "timestamp": p['timestamp']
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [p['lon'], p['lat']]
                }
            })
        
        line_collection = {"type": "FeatureCollection", "features": [line_feature]}
        points_collection = {"type": "FeatureCollection", "features": point_features}
        
        # 获取 Mapbox token
        mapbox_token = self.config.get('kpi', {}).get('curvature', {}).get(
            'map_viz', {}).get('mapbox_token', 'YOUR_MAPBOX_TOKEN_HERE')
        
        html_content = self._generate_weaving_html(
            center_lat=center_lat,
            center_lon=center_lon,
            line_json=json.dumps(line_collection),
            points_json=json.dumps(points_collection),
            event_idx=event_idx,
            event=event,
            mapbox_token=mapbox_token
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_all_weaving_map(self, events: List[WeavingEvent], output_path: str):
        """生成所有画龙事件的汇总地图"""
        if not events:
            return
        
        # 收集所有轨迹点
        all_features = []
        all_line_features = []
        
        # 颜色列表（为不同事件分配不同颜色）
        colors = ['#e74c3c', '#3498db', '#2ecc71', '#f39c12', '#9b59b6', 
                  '#1abc9c', '#e67e22', '#34495e', '#16a085', '#d35400']
        
        for i, event in enumerate(events):
            if not event.trajectory or len(event.trajectory) < 2:
                continue
            
            color = colors[i % len(colors)]
            
            # 轨迹线
            line_coords = [[p['lon'], p['lat']] for p in event.trajectory]
            all_line_features.append({
                "type": "Feature",
                "properties": {
                    "event_idx": i + 1,
                    "color": color,
                    "duration": round(event.duration, 2),
                    "max_steering_rate": round(event.max_steering_rate, 1)
                },
                "geometry": {
                    "type": "LineString",
                    "coordinates": line_coords
                }
            })
            
            # 起点和终点标记
            all_features.append({
                "type": "Feature",
                "properties": {
                    "type": "start",
                    "event_idx": i + 1,
                    "color": color
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [event.trajectory[0]['lon'], event.trajectory[0]['lat']]
                }
            })
            all_features.append({
                "type": "Feature",
                "properties": {
                    "type": "end",
                    "event_idx": i + 1,
                    "color": color
                },
                "geometry": {
                    "type": "Point",
                    "coordinates": [event.trajectory[-1]['lon'], event.trajectory[-1]['lat']]
                }
            })
        
        if not all_line_features:
            return
        
        # 计算中心点（所有轨迹的中心）
        all_coords = []
        for event in events:
            for p in event.trajectory:
                all_coords.append((p['lat'], p['lon']))
        
        center_lat = sum(c[0] for c in all_coords) / len(all_coords)
        center_lon = sum(c[1] for c in all_coords) / len(all_coords)
        
        lines_collection = {"type": "FeatureCollection", "features": all_line_features}
        points_collection = {"type": "FeatureCollection", "features": all_features}
        
        mapbox_token = self.config.get('kpi', {}).get('curvature', {}).get(
            'map_viz', {}).get('mapbox_token', 'YOUR_MAPBOX_TOKEN_HERE')
        
        html_content = self._generate_all_weaving_html(
            center_lat=center_lat,
            center_lon=center_lon,
            lines_json=json.dumps(lines_collection),
            points_json=json.dumps(points_collection),
            event_count=len(events),
            mapbox_token=mapbox_token
        )
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)
    
    def _generate_weaving_html(self, center_lat: float, center_lon: float,
                                line_json: str, points_json: str,
                                event_idx: int, event: WeavingEvent,
                                mapbox_token: str) -> str:
        """生成单个画龙事件的 HTML 页面"""
        
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>画龙事件 #{event_idx} 轨迹</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 13px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
            max-width: 280px;
        }}
        .info-title {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #e74c3c;
        }}
        .info-row {{
            display: flex;
            justify-content: space-between;
            margin: 6px 0;
        }}
        .info-label {{ color: #666; }}
        .info-value {{ font-weight: 500; }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .legend-title {{ font-weight: 600; margin-bottom: 8px; }}
        .gradient-bar {{
            width: 150px;
            height: 15px;
            background: linear-gradient(to right, #3498db, #f39c12, #e74c3c);
            border-radius: 3px;
            margin: 5px 0;
        }}
        .gradient-labels {{
            display: flex;
            justify-content: space-between;
            font-size: 11px;
            color: #666;
        }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <div class="info-title">🐉 画龙事件 #{event_idx}</div>
    <div class="info-row">
        <span class="info-label">持续时间:</span>
        <span class="info-value">{event.duration:.2f} 秒</span>
    </div>
    <div class="info-row">
        <span class="info-label">峰值转角速度:</span>
        <span class="info-value">{event.max_steering_rate:.1f} °/s</span>
    </div>
    <div class="info-row">
        <span class="info-label">RMS转角速度:</span>
        <span class="info-value">{event.max_rms:.1f} °/s</span>
    </div>
    <div class="info-row">
        <span class="info-label">振荡次数:</span>
        <span class="info-value">{event.oscillation_count} 次</span>
    </div>
    <div class="info-row">
        <span class="info-label">轨迹点数:</span>
        <span class="info-value">{len(event.trajectory)}</span>
    </div>
</div>

<div class="legend">
    <div class="legend-title">转角速度幅度</div>
    <div class="gradient-bar"></div>
    <div class="gradient-labels">
        <span>0</span>
        <span>{event.max_steering_rate:.0f}°/s</span>
    </div>
</div>

<script>
    mapboxgl.accessToken = '{mapbox_token}';
    
    const lineData = {line_json};
    const pointsData = {points_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [{center_lon}, {center_lat}],
        zoom: 18
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加轨迹线
        map.addSource('line', {{ 'type': 'geojson', 'data': lineData }});
        map.addLayer({{
            'id': 'trajectory-line',
            'type': 'line',
            'source': 'line',
            'paint': {{
                'line-color': '#e74c3c',
                'line-width': 4,
                'line-opacity': 0.7
            }}
        }});
        
        // 添加轨迹点（颜色根据转角速度渐变）
        map.addSource('points', {{ 'type': 'geojson', 'data': pointsData }});
        map.addLayer({{
            'id': 'trajectory-points',
            'type': 'circle',
            'source': 'points',
            'paint': {{
                'circle-radius': 6,
                'circle-color': [
                    'interpolate',
                    ['linear'],
                    ['get', 'normalized'],
                    0, '#3498db',
                    0.5, '#f39c12',
                    1, '#e74c3c'
                ],
                'circle-opacity': 0.9,
                'circle-stroke-width': 1,
                'circle-stroke-color': '#fff'
            }}
        }});
        
        // 自动适配视野
        const bounds = new mapboxgl.LngLatBounds();
        pointsData.features.forEach(f => bounds.extend(f.geometry.coordinates));
        map.fitBounds(bounds, {{ padding: 60 }});
        
        // 添加起点终点标记
        if (pointsData.features.length > 0) {{
            const startCoord = pointsData.features[0].geometry.coordinates;
            const endCoord = pointsData.features[pointsData.features.length - 1].geometry.coordinates;
            
            new mapboxgl.Marker({{ color: '#2ecc71' }})
                .setLngLat(startCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>起点</b>'))
                .addTo(map);
            
            new mapboxgl.Marker({{ color: '#e74c3c' }})
                .setLngLat(endCoord)
                .setPopup(new mapboxgl.Popup().setHTML('<b>终点</b>'))
                .addTo(map);
        }}
        
        // 点击弹窗
        map.on('click', 'trajectory-points', (e) => {{
            const props = e.features[0].properties;
            new mapboxgl.Popup()
                .setLngLat(e.features[0].geometry.coordinates)
                .setHTML(`<b>轨迹点 #${{props.index}}</b><br>转角速度: ${{props.steering_vel}}°/s`)
                .addTo(map);
        }});
        
        map.on('mouseenter', 'trajectory-points', () => {{ map.getCanvas().style.cursor = 'pointer'; }});
        map.on('mouseleave', 'trajectory-points', () => {{ map.getCanvas().style.cursor = ''; }});
    }});
</script>
</body>
</html>
'''
    
    def _generate_all_weaving_html(self, center_lat: float, center_lon: float,
                                    lines_json: str, points_json: str,
                                    event_count: int, mapbox_token: str) -> str:
        """生成所有画龙事件汇总的 HTML 页面"""
        
        return f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>画龙事件汇总 ({event_count}个事件)</title>
    <meta name="viewport" content="initial-scale=1,maximum-scale=1,user-scalable=no">
    <link href="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.css" rel="stylesheet">
    <script src="https://api.mapbox.com/mapbox-gl-js/v3.0.1/mapbox-gl.js"></script>
    <style>
        body {{ margin: 0; padding: 0; }}
        #map {{ position: absolute; top: 0; bottom: 0; width: 100%; }}
        .info-panel {{
            position: absolute;
            top: 10px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 14px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .info-title {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 10px;
            color: #e74c3c;
        }}
        .legend {{
            position: absolute;
            bottom: 30px;
            left: 10px;
            background: rgba(255, 255, 255, 0.95);
            padding: 12px 15px;
            border-radius: 8px;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            font-size: 12px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.2);
        }}
        .legend-title {{ font-weight: 600; margin-bottom: 8px; }}
        .legend-item {{
            display: flex;
            align-items: center;
            margin: 5px 0;
        }}
        .legend-line {{
            width: 25px;
            height: 4px;
            margin-right: 8px;
            border-radius: 2px;
        }}
    </style>
</head>
<body>
<div id="map"></div>

<div class="info-panel">
    <div class="info-title">🐉 画龙事件汇总</div>
    <div>共检测到 <b>{event_count}</b> 个画龙事件</div>
    <div style="margin-top: 8px; font-size: 12px; color: #666;">
        点击轨迹线查看详情<br>
        🟢 起点 &nbsp; 🔴 终点
    </div>
</div>

<div class="legend">
    <div class="legend-title">事件图例</div>
    <div id="legend-items"></div>
</div>

<script>
    mapboxgl.accessToken = '{mapbox_token}';
    
    const linesData = {lines_json};
    const pointsData = {points_json};
    
    const map = new mapboxgl.Map({{
        container: 'map',
        style: 'mapbox://styles/mapbox/dark-v11',
        center: [{center_lon}, {center_lat}],
        zoom: 15
    }});
    
    map.addControl(new mapboxgl.NavigationControl());
    map.addControl(new mapboxgl.ScaleControl());
    
    map.on('load', () => {{
        // 添加所有轨迹线
        map.addSource('lines', {{ 'type': 'geojson', 'data': linesData }});
        map.addLayer({{
            'id': 'trajectory-lines',
            'type': 'line',
            'source': 'lines',
            'paint': {{
                'line-color': ['get', 'color'],
                'line-width': 4,
                'line-opacity': 0.8
            }}
        }});
        
        // 自动适配视野
        const bounds = new mapboxgl.LngLatBounds();
        linesData.features.forEach(f => {{
            f.geometry.coordinates.forEach(coord => bounds.extend(coord));
        }});
        map.fitBounds(bounds, {{ padding: 60 }});
        
        // 为每个事件添加起点终点标记
        pointsData.features.forEach(f => {{
            const props = f.properties;
            const color = props.type === 'start' ? '#2ecc71' : '#e74c3c';
            const label = props.type === 'start' ? `事件#${{props.event_idx}} 起点` : `事件#${{props.event_idx}} 终点`;
            
            new mapboxgl.Marker({{ color: color, scale: 0.7 }})
                .setLngLat(f.geometry.coordinates)
                .setPopup(new mapboxgl.Popup().setHTML(`<b>${{label}}</b>`))
                .addTo(map);
        }});
        
        // 生成图例
        const legendItems = document.getElementById('legend-items');
        const seenEvents = new Set();
        linesData.features.forEach(f => {{
            const idx = f.properties.event_idx;
            if (!seenEvents.has(idx)) {{
                seenEvents.add(idx);
                const item = document.createElement('div');
                item.className = 'legend-item';
                item.innerHTML = `<div class="legend-line" style="background: ${{f.properties.color}};"></div>` +
                                 `<span>事件 #${{idx}} (${{f.properties.duration}}s)</span>`;
                legendItems.appendChild(item);
            }}
        }});
        
        // 点击弹窗
        map.on('click', 'trajectory-lines', (e) => {{
            const props = e.features[0].properties;
            new mapboxgl.Popup()
                .setLngLat(e.lngLat)
                .setHTML(`<b>画龙事件 #${{props.event_idx}}</b><br>` +
                         `持续: ${{props.duration}}s<br>` +
                         `峰值: ${{props.max_steering_rate}}°/s`)
                .addTo(map);
        }});
        
        map.on('mouseenter', 'trajectory-lines', () => {{ map.getCanvas().style.cursor = 'pointer'; }});
        map.on('mouseleave', 'trajectory-lines', () => {{ map.getCanvas().style.cursor = ''; }});
    }});
</script>
</body>
</html>
'''
    
    def _finalize_weaving_event(self, events: List[WeavingEvent],
                                 timestamps: np.ndarray,
                                 steering_velocities: np.ndarray,
                                 lateral_accs: np.ndarray,
                                 ts_acc: np.ndarray,
                                 steering_accs: np.ndarray,
                                 ts_err_rate: np.ndarray,
                                 lateral_error_rates: np.ndarray,
                                 rms_timestamps: np.ndarray,
                                 rms_values: np.ndarray,
                                 event_start_idx: int,
                                 event_end_idx: int,
                                 max_gap_sec: float = 0.5):
        """完成画龙事件的记录"""
        if event_end_idx <= event_start_idx:
            return
        
        start_time = rms_timestamps[event_start_idx]
        end_time = rms_timestamps[event_end_idx]
        
        # 计算实际有效持续时间（排除大间隔，如接管期间）
        orig_mask = (timestamps >= start_time) & (timestamps <= end_time)
        event_timestamps = timestamps[orig_mask]
        if len(event_timestamps) > 1:
            ts_diffs = np.diff(event_timestamps)
            # 只累加正常间隔，排除大间隔
            valid_diffs = ts_diffs[ts_diffs <= max_gap_sec]
            duration = float(np.sum(valid_diffs))
        else:
            duration = 0.0
        
        if duration < self.min_duration:
            return
        
        # 在原始数据中找到对应的时间范围
        orig_mask = (timestamps >= start_time) & (timestamps <= end_time)
        
        # 计算峰值
        max_steering_rate = float(np.max(np.abs(steering_velocities[orig_mask]))) if np.any(orig_mask) else 0
        max_lateral_acc = float(np.max(np.abs(lateral_accs[orig_mask]))) if np.any(orig_mask) else 0
        max_rms = float(np.max(rms_values[event_start_idx:event_end_idx+1]))
        
        # 加速度峰值
        acc_mask = (ts_acc >= start_time) & (ts_acc <= end_time)
        max_steering_acc = float(np.max(np.abs(steering_accs[acc_mask]))) if np.any(acc_mask) else 0
        
        # 横向误差变化率峰值
        err_mask = (ts_err_rate >= start_time) & (ts_err_rate <= end_time)
        max_lateral_error_rate = float(np.max(np.abs(lateral_error_rates[err_mask]))) if np.any(err_mask) else 0
        
        # 计算振荡次数（符号翻转）
        if np.any(orig_mask):
            vel_segment = steering_velocities[orig_mask]
            signs = np.sign(vel_segment)
            oscillation_count = int(np.sum(np.diff(signs) != 0))
        else:
            oscillation_count = 0
        
        events.append(WeavingEvent(
            start_time=start_time,
            end_time=end_time,
            max_steering_rate=max_steering_rate,
            max_steering_acc=max_steering_acc,
            max_lateral_error_rate=max_lateral_error_rate,
            max_lateral_acc=max_lateral_acc,
            oscillation_count=oscillation_count,
            max_rms=max_rms  # 新增：最大 RMS 值
        ))
    
    def _detect_weaving_events_legacy(self,
                                timestamps: np.ndarray,
                                steering_velocities: np.ndarray,
                                ts_acc: np.ndarray,
                                steering_accs: np.ndarray,
                                lateral_errors: np.ndarray,
                                ts_err_rate: np.ndarray,
                                lateral_error_rates: np.ndarray,
                                lateral_accs: np.ndarray,
                                speeds: np.ndarray) -> List[WeavingEvent]:
        """
        旧版画龙检测（保留作为备用）
        
        综合判断条件：
        1. 方向盘角速度超阈值 |δ̇| > 25 deg/s
        2. 方向盘角速度符号频繁翻转（振荡）
        3. 持续时间 > 0.5s
        """
        events = []
        
        if len(timestamps) < 10:
            return events
        
        # 计算采样间隔
        dt = np.median(np.diff(timestamps))
        if dt <= 0:
            dt = 0.1
        
        # 符号翻转检测的窗口大小
        window_samples = max(int(self.sign_flip_window / dt), 5)
        
        # 预计算符号翻转前缀，O(n) 获取窗口内翻转次数
        signs = np.sign(steering_velocities)
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        sign_changes = np.concatenate([[0], (np.diff(signs) != 0).astype(int)])
        prefix_flips = np.cumsum(sign_changes)  # prefix_flips[i] 表示 0..i-1 的翻转次数
        
        # 逐点检测
        in_event = False
        event_start_idx = 0
        event_data = {
            'max_steering_rate': 0,
            'max_steering_acc': 0,
            'max_lateral_error_rate': 0,
            'max_lateral_acc': 0,
            'sign_flips': 0
        }
        
        for i in range(len(timestamps)):
            # 计算当前点的指标
            steering_rate = abs(steering_velocities[i])
            lat_acc = abs(lateral_accs[i])
            
            # 计算窗口内的符号翻转次数
            window_start = max(0, i - window_samples)
            # 翻转次数 = prefix_flips[i] - prefix_flips[window_start]
            sign_flips = int(prefix_flips[i] - prefix_flips[window_start])
            
            # 判断是否满足画龙条件
            is_weaving = (
                steering_rate > self.steering_rate_threshold and
                sign_flips >= self.min_sign_flips
            )
            
            if is_weaving:
                if not in_event:
                    # 开始新事件
                    in_event = True
                    event_start_idx = i
                    event_data = {
                        'max_steering_rate': steering_rate,
                        'max_steering_acc': 0,
                        'max_lateral_error_rate': 0,
                        'max_lateral_acc': lat_acc,
                        'sign_flips': sign_flips
                    }
                else:
                    # 更新事件数据
                    event_data['max_steering_rate'] = max(event_data['max_steering_rate'], steering_rate)
                    event_data['max_lateral_acc'] = max(event_data['max_lateral_acc'], lat_acc)
                    event_data['sign_flips'] = max(event_data['sign_flips'], sign_flips)
            else:
                if in_event:
                    # 事件结束，检查持续时间
                    duration = timestamps[i-1] - timestamps[event_start_idx]
                    
                    if duration >= self.min_duration:
                        # 查找对应的加速度峰值
                        acc_mask = (ts_acc >= timestamps[event_start_idx]) & (ts_acc <= timestamps[i-1])
                        if np.any(acc_mask):
                            event_data['max_steering_acc'] = float(np.max(np.abs(steering_accs[acc_mask])))
                        
                        # 查找对应的横向误差变化率峰值
                        err_mask = (ts_err_rate >= timestamps[event_start_idx]) & (ts_err_rate <= timestamps[i-1])
                        if np.any(err_mask):
                            event_data['max_lateral_error_rate'] = float(np.max(np.abs(lateral_error_rates[err_mask])))
                        
                        events.append(WeavingEvent(
                            start_time=timestamps[event_start_idx],
                            end_time=timestamps[i-1],
                            max_steering_rate=event_data['max_steering_rate'],
                            max_steering_acc=event_data['max_steering_acc'],
                            max_lateral_error_rate=event_data['max_lateral_error_rate'],
                            max_lateral_acc=event_data['max_lateral_acc'],
                            oscillation_count=event_data['sign_flips']
                        ))
                    
                    in_event = False
        
        # 处理最后一个事件
        if in_event:
            duration = timestamps[-1] - timestamps[event_start_idx]
            if duration >= self.min_duration:
                acc_mask = (ts_acc >= timestamps[event_start_idx])
                if np.any(acc_mask):
                    event_data['max_steering_acc'] = float(np.max(np.abs(steering_accs[acc_mask])))
                
                err_mask = (ts_err_rate >= timestamps[event_start_idx])
                if np.any(err_mask):
                    event_data['max_lateral_error_rate'] = float(np.max(np.abs(lateral_error_rates[err_mask])))
                
                events.append(WeavingEvent(
                    start_time=timestamps[event_start_idx],
                    end_time=timestamps[-1],
                    max_steering_rate=event_data['max_steering_rate'],
                    max_steering_acc=event_data['max_steering_acc'],
                    max_lateral_error_rate=event_data['max_lateral_error_rate'],
                    max_lateral_acc=event_data['max_lateral_acc'],
                    oscillation_count=event_data['sign_flips']
                ))
        
        return events
    
    def _count_sign_flips(self, values: np.ndarray) -> int:
        """计算符号翻转次数"""
        if len(values) < 2:
            return 0
        
        signs = np.sign(values)
        # 过滤掉0值（取前一个符号）
        for i in range(1, len(signs)):
            if signs[i] == 0:
                signs[i] = signs[i-1]
        
        # 计算符号变化次数
        sign_changes = np.sum(np.diff(signs) != 0)
        return int(sign_changes)
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="画龙次数",
            value=0,
            unit="次",
            description="数据不足，无法检测画龙"
        ))
        self.add_result(KPIResult(
            name="画龙频率",
            value=0.0,
            unit="次/百公里",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="平均画龙里程",
            value="∞",
            unit="km/次",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData,
                parsed_debug_data: Optional[Dict] = None, **kwargs):
        """
        收集画龙检测数据（流式模式）
        """
        self._debug_timestamps = None  # 重置缓存
        
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
            
            # 获取方向盘数据
            steering_angle = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle", None)
            steering_vel = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle_velocity", None)
            lat_acc = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_lateral_acceleration", None)
            speed = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_speed", None)
            
            # 获取位置信息（用于轨迹可视化）
            ego_lat = None
            ego_lon = None
            if loc_msg is not None:
                ego_lat = MessageAccessor.get_field(
                    loc_msg, "global_localization.position.latitude", None)
                ego_lon = MessageAccessor.get_field(
                    loc_msg, "global_localization.position.longitude", None)
            
            # 检查定位可信度
            loc_reliable = True
            if loc_msg is not None:
                loc_status = MessageAccessor.get_field(loc_msg, "status.common", None)
                pos_stddev_east = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.east", None)
                pos_stddev_north = MessageAccessor.get_field(
                    loc_msg, "global_localization.position_stddev.north", None)
                
                if loc_status is not None and loc_status not in self.loc_valid_status:
                    loc_reliable = False
                
                if pos_stddev_east is not None and pos_stddev_north is not None:
                    if max(pos_stddev_east, pos_stddev_north) > self.loc_max_stddev:
                        loc_reliable = False
            
            # 获取横向误差和曲率
            lat_error = None
            curvature = None
            if loc_reliable and parsed_debug_data is not None and len(parsed_debug_data) > 0:
                lat_error = self._find_nearest_debug_data(
                    parsed_debug_data, frame.timestamp, 'lateral_error', tolerance=0.05)
                # 尝试获取曲率（从 control debug 或轨迹）
                curvature = self._find_nearest_debug_data(
                    parsed_debug_data, frame.timestamp, 'ref_kappa', tolerance=0.05)
            
            if steering_vel is None:
                continue
            
            # 存储: (steering_vel, lat_acc, lat_error, loc_reliable, speed, timestamp, steering_angle, lat, lon, curvature)
            streaming_data.weaving_data.append((
                steering_vel,
                lat_acc if lat_acc is not None else 0.0,
                lat_error if lat_error is not None else 0.0,
                loc_reliable,
                speed if speed is not None else 0.0,
                frame.timestamp,
                steering_angle if steering_angle is not None else 0.0,
                ego_lat if ego_lat is not None else 0.0,
                ego_lon if ego_lon is not None else 0.0,
                curvature if curvature is not None else 0.0
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算画龙检测KPI（流式模式）
        
        Note:
            数据来源：
            - 转角/速度数据：优先使用高频数据 (chassis_highfreq, ~50Hz)
            - 位置/横向误差/曲率：仍从 weaving_data (10Hz 同步帧) 获取
            - 最终结果会插值对齐到高频时间戳
        """
        self.clear_results()
        
        # 检查是否有高频底盘数据可用
        use_highfreq = len(streaming_data.chassis_highfreq) > 0 and len(streaming_data.chassis_auto_states) > 0
        
        if use_highfreq:
            # ========== 使用高频数据 (~50Hz) ==========
            # 过滤自动驾驶状态
            auto_indices = [i for i, (is_auto, _) in enumerate(streaming_data.chassis_auto_states) 
                           if is_auto and i < len(streaming_data.chassis_highfreq)]
            
            if len(auto_indices) < 20:
                self._add_empty_results()
                return self.get_results()
            
            # 高频数据: (steering_angle, steering_vel, speed, lat_acc, timestamp)
            steering_angles = np.array([streaming_data.chassis_highfreq[i][0] for i in auto_indices])
            steering_velocities_raw = np.array([streaming_data.chassis_highfreq[i][1] for i in auto_indices])
            speeds = np.array([streaming_data.chassis_highfreq[i][2] for i in auto_indices])
            lateral_accs = np.array([streaming_data.chassis_highfreq[i][3] for i in auto_indices])
            timestamps = np.array([streaming_data.chassis_highfreq[i][4] for i in auto_indices])
            
            # 从 weaving_data 获取位置和其他数据（低频，需要插值）
            if len(streaming_data.weaving_data) > 0:
                weaving_ts = np.array([d[5] for d in streaming_data.weaving_data])
                lateral_errors_lowfreq = np.array([d[2] for d in streaming_data.weaving_data])
                loc_reliable_lowfreq = np.array([d[3] for d in streaming_data.weaving_data])
                latitudes_lowfreq = np.array([d[7] for d in streaming_data.weaving_data])
                longitudes_lowfreq = np.array([d[8] for d in streaming_data.weaving_data])
                curvatures_lowfreq = np.array([d[9] if len(d) > 9 else 0.0 for d in streaming_data.weaving_data])

                # 确保低频数据按时间排序，避免插值错误
                if len(weaving_ts) > 1:
                    sort_idx = np.argsort(weaving_ts)
                    weaving_ts = weaving_ts[sort_idx]
                    lateral_errors_lowfreq = lateral_errors_lowfreq[sort_idx]
                    loc_reliable_lowfreq = loc_reliable_lowfreq[sort_idx]
                    latitudes_lowfreq = latitudes_lowfreq[sort_idx]
                    longitudes_lowfreq = longitudes_lowfreq[sort_idx]
                    curvatures_lowfreq = curvatures_lowfreq[sort_idx]
                
                # 插值到高频时间戳
                lateral_errors = np.interp(timestamps, weaving_ts, lateral_errors_lowfreq)
                loc_reliable_flags = np.interp(timestamps, weaving_ts, loc_reliable_lowfreq.astype(float)) > 0.5
                latitudes = np.interp(timestamps, weaving_ts, latitudes_lowfreq)
                longitudes = np.interp(timestamps, weaving_ts, longitudes_lowfreq)
                curvatures = np.interp(timestamps, weaving_ts, curvatures_lowfreq)
            else:
                # 没有位置数据，使用默认值
                lateral_errors = np.zeros_like(timestamps)
                loc_reliable_flags = np.ones(len(timestamps), dtype=bool)
                latitudes = np.zeros_like(timestamps)
                longitudes = np.zeros_like(timestamps)
                curvatures = np.zeros_like(timestamps)
        else:
            # ========== 回退到低频数据 (10Hz 同步帧) ==========
            if len(streaming_data.weaving_data) < 20:
                self._add_empty_results()
                return self.get_results()
            
            # 解构数据 (steering_vel, lat_acc, lat_error, loc_reliable, speed, timestamp, steering_angle, lat, lon, curvature)
            timestamps = np.array([d[5] for d in streaming_data.weaving_data])
            steering_velocities_raw = np.array([d[0] for d in streaming_data.weaving_data])
            lateral_accs = np.array([d[1] for d in streaming_data.weaving_data])
            lateral_errors = np.array([d[2] for d in streaming_data.weaving_data])
            loc_reliable_flags = np.array([d[3] for d in streaming_data.weaving_data])
            speeds = np.array([d[4] for d in streaming_data.weaving_data])
            steering_angles = np.array([d[6] for d in streaming_data.weaving_data])
            latitudes = np.array([d[7] for d in streaming_data.weaving_data])
            longitudes = np.array([d[8] for d in streaming_data.weaving_data])
            curvatures = np.array([d[9] if len(d) > 9 else 0.0 for d in streaming_data.weaving_data])
        
        # 动态估计实际采样率
        if len(timestamps) >= 2:
            dt_array = np.diff(timestamps)
            actual_fs = 1.0 / np.median(dt_array)
            actual_fs = max(1.0, min(actual_fs, 100.0))  # 限制在合理范围
            
            # 调试输出：确认采样率和原始数据
            print(f"[画龙检测] 数据点数: {len(timestamps)}, 时长: {timestamps[-1]-timestamps[0]:.1f}s")
            print(f"[画龙检测] 实际采样率: {actual_fs:.1f} Hz (dt median: {np.median(dt_array)*1000:.1f}ms)")
            print(f"[画龙检测] 转角范围: {np.min(steering_angles):.1f}° ~ {np.max(steering_angles):.1f}°")
            
            # 过滤原始转角跳变：相邻帧转角变化超过阈值的认为是数据异常
            # 在 50Hz 下，正常最大转角变化约 300°/s * 0.02s = 6°/帧
            max_angle_change_per_frame = 10.0  # °，保守一点
            angle_diff = np.abs(np.diff(steering_angles))
            jump_indices = np.where(angle_diff > max_angle_change_per_frame)[0]
            if len(jump_indices) > 0:
                print(f"[画龙检测] 发现 {len(jump_indices)} 个转角跳变点(>{max_angle_change_per_frame}°/帧)")
                # 用前一帧的值替换跳变点
                for idx in jump_indices:
                    if idx + 1 < len(steering_angles):
                        steering_angles[idx + 1] = steering_angles[idx]
        else:
            actual_fs = self.sampling_rate_hz
        
        # ========== 转角速度计算逻辑 ==========
        # 1. 对转角进行低通滤波
        # 注意：滤波截止频率需 < Nyquist (actual_fs/2)，否则跳过滤波
        angle_cutoff = min(self.angle_filter_cutoff, actual_fs / 2 - 0.1)
        if angle_cutoff > 0:
            steering_angles_filtered = self._apply_lowpass_filter(
                steering_angles, angle_cutoff, actual_fs, order=2)
        else:
            steering_angles_filtered = steering_angles
        
        # 2. 从滤波后的转角计算导数得到转角速度
        ts_rate, steering_velocities = SignalProcessor.compute_derivative(
            timestamps, steering_angles_filtered)
        
        # 调试输出：转角速度计算结果（滤波前）
        if len(steering_velocities) > 0:
            print(f"[画龙检测] 转角速度(滤波前): min={np.min(steering_velocities):.1f}°/s, max={np.max(steering_velocities):.1f}°/s, RMS={np.sqrt(np.mean(steering_velocities**2)):.1f}°/s")
        
        # 2.5 异常值过滤：转角速度超过 ±300°/s 的认为是数据跳变，clip 到合理范围
        # 正常方向盘最快转速约 200-250°/s，超过 300°/s 基本是数据异常
        max_steering_rate = 300.0  # °/s
        outlier_mask = np.abs(steering_velocities) > max_steering_rate
        outlier_count = int(np.sum(outlier_mask))
        outlier_indices = np.where(outlier_mask)[0]
        if outlier_count > 0:
            print(f"[画龙检测] 发现 {outlier_count} 个异常转角速度点(>{max_steering_rate}°/s)，已裁剪")
            steering_velocities = np.clip(steering_velocities, -max_steering_rate, max_steering_rate)
        
        # 3. 对转角速度进行低通滤波
        rate_cutoff = min(self.rate_filter_cutoff, actual_fs / 2 - 0.1)
        if rate_cutoff > 0:
            steering_velocities = self._apply_lowpass_filter(
                steering_velocities, rate_cutoff, actual_fs, order=2)
            # 调试输出：转角速度（滤波后）
            print(f"[画龙检测] 转角速度(滤波后 cutoff={rate_cutoff:.1f}Hz): RMS={np.sqrt(np.mean(steering_velocities**2)):.1f}°/s")
        # else: 保持原值，不滤波
        
        # 将其他信号对齐到转角速度的时间戳
        lateral_accs = np.interp(ts_rate, timestamps, lateral_accs)
        lateral_errors = np.interp(ts_rate, timestamps, lateral_errors)
        loc_reliable_flags = np.interp(ts_rate, timestamps, loc_reliable_flags.astype(float)) > 0.5
        speeds = np.interp(ts_rate, timestamps, speeds)
        steering_angles = np.interp(ts_rate, timestamps, steering_angles)
        latitudes = np.interp(ts_rate, timestamps, latitudes)
        longitudes = np.interp(ts_rate, timestamps, longitudes)
        curvatures = np.interp(ts_rate, timestamps, curvatures)
        timestamps = ts_rate  # 更新时间戳为转角速度的时间戳
        
        # 计算方向盘角加速度
        ts_acc, steering_accs = SignalProcessor.compute_derivative(timestamps, steering_velocities)
        
        # 计算横向误差变化率
        ts_err_rate, lateral_error_rates = SignalProcessor.compute_derivative(timestamps, lateral_errors)
        
        # 检测画龙事件
        # 如果曲率数据全为0，则不传入（让检测逻辑使用备选方案）
        curvatures_to_use = curvatures if np.any(curvatures != 0) else None
        
        weaving_events = self._detect_weaving_events(
            timestamps, steering_angles, steering_velocities, ts_acc, steering_accs,
            lateral_errors, ts_err_rate, lateral_error_rates,
            lateral_accs, speeds, latitudes, longitudes, curvatures_to_use
        )
        
        # 计算统计信息
        reliable_lateral_errors = lateral_errors[loc_reliable_flags]
        max_lateral_error = np.max(np.abs(reliable_lateral_errors)) if len(reliable_lateral_errors) > 0 else 0
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 添加结果
        weaving_result = KPIResult(
            name="画龙次数",
            value=len(weaving_events),
            unit="次",
            description=(f"行驶时方向盘周期性振荡（速度>{self.min_speed}km/h，"
                        f"{self.window_duration}s内有效过零≥{self.min_zero_crossings}次，"
                        f"转角峰谷差≥{self.min_steering_amplitude}°，"
                        f"排除转弯：单向变化>80%）"),
            details={
                'min_speed': self.min_speed,
                'min_steering_amplitude': self.min_steering_amplitude,
                'window_duration': self.window_duration,
                'min_zero_crossings': self.min_zero_crossings,
                'min_duration': self.min_duration,
                'min_oscillation_cycles': self.min_oscillation_cycles
            }
        )
        
        for i, e in enumerate(weaving_events):
            weaving_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=(f"画龙 #{i+1}：持续 {e.duration:.2f}s，"
                           f"RMS {e.max_rms:.1f}°/s，"
                           f"峰值 {e.max_steering_rate:.1f}°/s，"
                           f"振荡 {e.oscillation_count} 次"),
                value=e.max_rms,
                threshold=e.rms_threshold if e.rms_threshold > 0 else self.steering_rate_threshold
            )
        self.add_result(weaving_result)

        # 记录转角速度异常点（用于定位数据问题）
        if outlier_count > 0:
            max_samples = 50
            # 按异常幅度排序，取最严重的若干点
            sorted_idx = outlier_indices[np.argsort(np.abs(steering_velocities[outlier_indices]))[::-1]]
            sample_idx = sorted_idx[:max_samples]
            outlier_result = KPIResult(
                name="转角速度异常点",
                value=outlier_count,
                unit="个",
                description=f"转角速度 |δ̇| > {max_steering_rate}°/s 的异常点（显示前{len(sample_idx)}个）",
                details={
                    'threshold_deg_s': max_steering_rate,
                    'total_count': outlier_count,
                    'sample_count': int(len(sample_idx))
                }
            )
            for idx in sample_idx:
                ts = float(timestamps[idx])
                vel = float(steering_velocities[idx])
                speed = float(speeds[idx]) if idx < len(speeds) else 0.0
                angle = float(steering_angles[idx]) if idx < len(steering_angles) else 0.0
                outlier_result.add_anomaly(
                    timestamp=ts,
                    bag_name=bag_mapper.get_bag_name(ts),
                    description=(f"转角速度异常点：{vel:.1f}°/s，"
                                 f"转角 {angle:.1f}°，速度 {speed:.1f}km/h"),
                    value=vel,
                    threshold=max_steering_rate
                )
            self.add_result(outlier_result)
        
        # 获取自动驾驶里程
        auto_mileage_km = kwargs.get('auto_mileage_km', 0)
        
        weaving_count = len(weaving_events)
        weaving_per_100km = (weaving_count / auto_mileage_km * 100) if auto_mileage_km > 0 else 0
        avg_weaving_mileage = (auto_mileage_km / weaving_count) if weaving_count > 0 else float('inf')
        
        self.add_result(KPIResult(
            name="画龙频率",
            value=round(weaving_per_100km, 2),
            unit="次/百公里",
            description="每百公里自动驾驶里程的画龙次数"
        ))
        
        self.add_result(KPIResult(
            name="平均画龙里程",
            value="∞" if weaving_count == 0 else round(avg_weaving_mileage, 3),
            unit="km/次",
            description="平均每多少公里自动驾驶里程画龙一次"
        ))
        
        # 生成画龙轨迹可视化
        output_dir = kwargs.get('output_dir')
        if output_dir and weaving_events:
            self._generate_weaving_map(weaving_events, output_dir)
        
        return self.get_results()
