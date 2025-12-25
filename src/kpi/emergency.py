"""
紧急事件检测KPI
包括急减速、横向猛打、顿挫检测
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

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
        
        # 加载舒适性配置（用于获取速度相关阈值）
        self.comfort_config = self.config.get('kpi', {}).get('comfort', {})
        
        # 转向配置（横向猛打阈值）
        steering_config = self.config.get('kpi', {}).get('steering', {})
        # 转角速度阈值 (°/s)，按速度分段
        self.lateral_speed_thresholds = steering_config.get('speed_thresholds', {
            0: 200, 10: 180, 20: 160, 30: 140, 40: 120,
            50: 100, 60: 80, 70: 60
        })
        # 转角加速度阈值 (°/s²)，按速度分段
        self.steering_acc_thresholds = steering_config.get('steering_acc_thresholds', {
            100: 120, 80: 150, 60: 200, 30: 250, 10: 400, 0: 600
        })
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]  # 有效的定位状态
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算紧急事件检测KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
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
    
    def _detect_hard_braking(self, 
                              timestamps: np.ndarray,
                              accelerations: np.ndarray,
                              speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测急减速事件（使用速度相关阈值）
        
        对每个数据点根据实时速度判断是否超过 fail 阈值
        """
        events = []
        
        # 逐点判断是否超过速度相关阈值
        over_threshold_mask = np.zeros(len(accelerations), dtype=bool)
        thresholds_used = []
        
        for i, (acc, spd) in enumerate(zip(accelerations, speeds)):
            threshold = self._get_deceleration_threshold(spd)
            thresholds_used.append(threshold)
            if acc < threshold:  # 减速度为负值，小于阈值表示更急
                over_threshold_mask[i] = True
        
        # 检测连续超阈值事件
        in_event = False
        event_start_idx = None
        
        for i in range(len(over_threshold_mask)):
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
    
    def _detect_jerk_events(self,
                             timestamps: np.ndarray,
                             accelerations: np.ndarray,
                             speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测顿挫事件（使用速度相关阈值）
        
        对每个 jerk 点根据实时速度判断是否超过 peak 阈值
        """
        events = []
        
        # 计算jerk
        ts_jerk, jerks = SignalProcessor.compute_jerk(timestamps, accelerations)
        
        if len(jerks) == 0:
            return events
        
        # 将 speeds 对齐到 jerk 时间戳（使用最近的速度值）
        jerk_speeds = np.zeros(len(ts_jerk))
        for i, ts_j in enumerate(ts_jerk):
            # 找到最近的速度时间戳
            closest_idx = np.argmin(np.abs(timestamps - ts_j))
            jerk_speeds[i] = speeds[closest_idx] if closest_idx < len(speeds) else speeds[-1]
        
        # 逐点判断是否超过速度相关阈值
        over_threshold_mask = np.zeros(len(jerks), dtype=bool)
        
        for i, (jerk_val, spd) in enumerate(zip(jerks, jerk_speeds)):
            threshold = self._get_jerk_threshold(spd)
            if abs(jerk_val) > threshold:
                over_threshold_mask[i] = True
        
        # 检测连续超阈值事件
        in_event = False
        event_start_idx = None
        
        for i in range(len(over_threshold_mask)):
            if over_threshold_mask[i] and not in_event:
                in_event = True
                event_start_idx = i
            elif not over_threshold_mask[i] and in_event:
                in_event = False
                # 检查持续时间
                duration = ts_jerk[i-1] - ts_jerk[event_start_idx]
                if duration >= self.jerk_min_duration:
                    # 计算峰值
                    event_mask = (ts_jerk >= ts_jerk[event_start_idx]) & (ts_jerk <= ts_jerk[i-1])
                    peak_value = np.max(np.abs(jerks[event_mask])) if np.any(event_mask) else abs(jerks[event_start_idx])
                    
                    events.append(EmergencyEvent(
                        event_type='jerk_event',
                        start_time=ts_jerk[event_start_idx],
                        end_time=ts_jerk[i-1],
                        peak_value=peak_value
                    ))
        
        # 处理最后一个事件
        if in_event:
            duration = ts_jerk[-1] - ts_jerk[event_start_idx]
            if duration >= self.jerk_min_duration:
                event_mask = (ts_jerk >= ts_jerk[event_start_idx])
                peak_value = np.max(np.abs(jerks[event_mask])) if np.any(event_mask) else abs(jerks[event_start_idx])
                
                events.append(EmergencyEvent(
                    event_type='jerk_event',
                    start_time=ts_jerk[event_start_idx],
                    end_time=ts_jerk[-1],
                    peak_value=peak_value
                ))
        
        return events
    
    def _detect_lateral_jerk(self,
                              timestamps: np.ndarray,
                              steering_velocities: np.ndarray,
                              speeds: np.ndarray) -> List[EmergencyEvent]:
        """
        检测横向猛打事件
        同时检测：转角速度超限 + 转角加速度超限
        """
        events = []
        
        if len(steering_velocities) == 0:
            return events
        
        # ========== 1. 转角速度超限检测 ==========
        vel_thresholds = np.array([self._get_lateral_threshold(s) for s in speeds])
        vel_over_threshold = np.abs(steering_velocities) > vel_thresholds
        
        # 找连续区间
        in_event = False
        event_start_idx = 0
        
        for i in range(len(vel_over_threshold)):
            if vel_over_threshold[i] and not in_event:
                in_event = True
                event_start_idx = i
            elif not vel_over_threshold[i] and in_event:
                in_event = False
                
                events.append(EmergencyEvent(
                    event_type='lateral_jerk_vel',  # 转角速度超限
                    start_time=timestamps[event_start_idx],
                    end_time=timestamps[i - 1],
                    peak_value=float(np.max(np.abs(steering_velocities[event_start_idx:i]))),
                    speed_at_event=float(np.mean(speeds[event_start_idx:i]))
                ))
        
        # 检查是否在事件中结束
        if in_event:
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
            acc_thresholds = np.array([self._get_steering_acc_threshold(s) for s in speeds_at_acc])
            acc_over_threshold = np.abs(steering_acc) > acc_thresholds
            
            in_event = False
            event_start_idx = 0
            
            for i in range(len(acc_over_threshold)):
                if acc_over_threshold[i] and not in_event:
                    in_event = True
                    event_start_idx = i
                elif not acc_over_threshold[i] and in_event:
                    in_event = False
                    
                    events.append(EmergencyEvent(
                        event_type='lateral_jerk_acc',  # 转角加速度超限
                        start_time=ts_acc[event_start_idx],
                        end_time=ts_acc[i - 1],
                        peak_value=float(np.max(np.abs(steering_acc[event_start_idx:i]))),
                        speed_at_event=float(np.mean(speeds_at_acc[event_start_idx:i]))
                    ))
            
            if in_event:
                events.append(EmergencyEvent(
                    event_type='lateral_jerk_acc',
                    start_time=ts_acc[event_start_idx],
                    end_time=ts_acc[-1],
                    peak_value=float(np.max(np.abs(steering_acc[event_start_idx:]))),
                    speed_at_event=float(np.mean(speeds_at_acc[event_start_idx:]))
                ))
        
        # 按时间排序
        events.sort(key=lambda e: e.start_time)
        
        return events
    
    def _get_lateral_threshold(self, speed_kmh: float) -> float:
        """根据速度获取转角速度阈值"""
        sorted_speeds = sorted(self.lateral_speed_thresholds.keys(), reverse=True)
        
        for threshold_speed in sorted_speeds:
            if speed_kmh >= threshold_speed:
                return self.lateral_speed_thresholds[threshold_speed]
        
        return self.lateral_speed_thresholds.get(0, 150)
    
    def _get_steering_acc_threshold(self, speed_kmh: float) -> float:
        """根据速度获取转角加速度阈值"""
        sorted_speeds = sorted(self.steering_acc_thresholds.keys(), reverse=True)
        
        for threshold_speed in sorted_speeds:
            if speed_kmh > threshold_speed:
                return self.steering_acc_thresholds[threshold_speed]
        
        return self.steering_acc_thresholds.get(0, 600)
    
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
        
        纵向加速度来源：/control/control.chassis_control.target_longitudinal_acceleration
        """
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            chassis_msg = frame.messages.get("/vehicle/chassis_domain_report")
            control_msg = frame.messages.get("/control/control")
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
            
            # 获取纵向加速度（优先使用 control topic）
            lon_acc = None
            if control_msg is not None:
                lon_acc = MessageAccessor.get_field(
                    control_msg, "chassis_control.target_longitudinal_acceleration", None)
            
            # 如果 control topic 没有数据，则跳过
            if lon_acc is None:
                continue
            
            # 获取其他数据（从 chassis）
            steering_vel = MessageAccessor.get_field(
                chassis_msg, "eps_system.actual_steering_angle_velocity", None)
            speed = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_speed", None)
            
            # 存储: (lon_acc, steering_vel, speed, timestamp)
            streaming_data.emergency_data.append((
                lon_acc,
                steering_vel if steering_vel is not None else 0.0,
                speed if speed is not None else 0.0,
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算紧急事件KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.emergency_data) < 10:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据
        lon_accelerations = np.array([d[0] for d in streaming_data.emergency_data])
        steering_velocities = np.array([d[1] for d in streaming_data.emergency_data])
        speeds = np.array([d[2] for d in streaming_data.emergency_data])
        timestamps = np.array([d[3] for d in streaming_data.emergency_data])
        
        # 1. 检测急减速
        hard_braking_events = self._detect_hard_braking(
            timestamps, lon_accelerations, speeds)
        
        # 2. 检测顿挫（需要速度信息）
        jerk_events = self._detect_jerk_events(timestamps, lon_accelerations, speeds)
        
        # 3. 检测横向猛打（转角速度超限 + 转角加速度超限）
        lateral_jerk_events = self._detect_lateral_jerk(
            timestamps, steering_velocities, speeds)
        
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
                'sample_count': len(timestamps)
            }
        )
        for i, e in enumerate(hard_braking_events):
            threshold_used = abs(self._get_deceleration_threshold(e.speed_at_event))
            hard_braking_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=f"急减速 #{i+1}@{e.speed_at_event:.0f}km/h：{e.peak_value:.2f}<{threshold_used:.2f}m/s²",
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
            event_start_idx = np.argmin(np.abs(timestamps - e.start_time))
            event_speed = speeds[event_start_idx] if event_start_idx < len(speeds) else 30.0
            threshold_used = self._get_jerk_threshold(event_speed)
            jerk_result.add_anomaly(
                timestamp=e.start_time,
                bag_name=bag_mapper.get_bag_name(e.start_time),
                description=f"顿挫 #{i+1}@{event_speed:.0f}km/h：|{e.peak_value:.2f}|>{threshold_used:.2f}m/s³",
                value=e.peak_value,
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
            if e.event_type == 'lateral_jerk_vel':
                threshold = self._get_lateral_threshold(e.speed_at_event)
                lateral_result.add_anomaly(
                    timestamp=e.start_time,
                    bag_name=bag_mapper.get_bag_name(e.start_time),
                    description=f"横向猛打 #{i+1}(转角速度)：{e.peak_value:.1f}°/s，车速 {e.speed_at_event:.1f}km/h",
                    value=e.peak_value,
                    threshold=threshold
                )
            else:  # lateral_jerk_acc
                threshold = self._get_steering_acc_threshold(e.speed_at_event)
                lateral_result.add_anomaly(
                    timestamp=e.start_time,
                    bag_name=bag_mapper.get_bag_name(e.start_time),
                    description=f"横向猛打 #{i+1}(转角加速度)：{e.peak_value:.1f}°/s²，车速 {e.speed_at_event:.1f}km/h",
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

