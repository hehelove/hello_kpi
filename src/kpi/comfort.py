"""
舒适性KPI
计算加速/制动舒适性 (jerk)

L4 自动驾驶动力学 KPI：
- 速度相关阈值：不同速度区间使用不同阈值
- 低速过滤：< 5 km/h 不统计 jerk，避免噪声放大
- 多尺度统计：瞬时峰值、RMS（1s窗口）、P95
- 三级阈值：comfort / warn / fail
"""
from typing import List, Dict, Any, Optional
import numpy as np

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, MergeStrategy, AnomalyRecord, merge_consecutive_events, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor


class ComfortKPI(BaseKPI):
    """舒适性KPI - L4 动力学标准"""
    
    @property
    def name(self) -> str:
        return "舒适性"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/vehicle/chassis_domain_report",
            "/control/control",  # 纵向加速度来源
            "/localization/localization"  # 用于检查定位可信度
        ]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 从统一配置获取 comfort 配置（包含动力学阈值）
        comfort_config = self.config.get('kpi', {}).get('comfort', {})
        
        # Jerk 计算参数
        self.target_fs = comfort_config.get('target_fs', None)
        self.filter_method = comfort_config.get('filter_method', 'butter')
        self.cutoff_hz = comfort_config.get('cutoff_hz', None)
        
        # 动力学参数
        self.min_valid_speed_kph = comfort_config.get('min_valid_speed_kph', 5.0)
        self.rms_window_sec = comfort_config.get('rms_window_sec', 1.0)
        
        # 事件定义参数
        event_config = comfort_config.get('event_definition', {})
        self.min_event_gap_sec = event_config.get('min_event_gap_sec', 2.0)
        self.merge_close_events = event_config.get('merge_close_events', True)
        
        # 存储动力学阈值配置（用于速度相关阈值查找）
        self.dynamics_config = comfort_config
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)
    
    def _get_threshold_for_speed(self, config_key: str, metric_type: str, speed_kph: float) -> float:
        """
        根据速度获取对应的阈值
        
        Args:
            config_key: 'longitudinal_jerk', 'lateral_jerk', 等
            metric_type: 'peak' 或 'rms'
            speed_kph: 当前速度 (km/h)
            
        Returns:
            对应速度区间的阈值
        """
        try:
            ranges = self.dynamics_config.get(config_key, {}).get(metric_type, {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                if speed_range[0] <= speed_kph < speed_range[1]:
                    return r.get('threshold', 3.0)
        except Exception:
            pass
        # 默认阈值
        if 'lateral' in config_key:
            return 2.5 if metric_type == 'peak' else 1.0
        return 3.0 if metric_type == 'peak' else 1.5
    
    def _get_acc_threshold_for_speed(self, config_key: str, level: str, speed_kph: float) -> float:
        """
        根据速度获取加速度阈值
        
        Args:
            config_key: 'longitudinal_acceleration', 'lateral_acceleration', 等
            level: 'comfort', 'warn', 'fail'
            speed_kph: 当前速度 (km/h)
        """
        try:
            ranges = self.dynamics_config.get(config_key, {}).get('ranges', [])
            for r in ranges:
                speed_range = r.get('speed_kph', [0, 999])
                if speed_range[0] <= speed_kph < speed_range[1]:
                    return r.get(level, 3.0)
        except Exception:
            pass
        return 3.0
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算舒适性KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _create_jerk_events(self, lon_jerk_result, 
                             bag_mapper: BagTimeMapper,
                             speeds: np.ndarray = None,
                             avg_speed: float = 30.0) -> Optional[KPIResult]:
        """
        创建纵向 Jerk 超限事件
        
        连续帧的超限合并为单次事件
        使用实时速度相关的阈值进行判定（每个点根据当时速度判断）
        """
        if lon_jerk_result is None or len(lon_jerk_result.values) == 0:
            return None
        
        lon_ts = lon_jerk_result.timestamps
        lon_vals = lon_jerk_result.values
        
        # 逐点根据实时速度判断纵向 jerk 超限
        lon_over_indices = []
        for i, jerk_val in enumerate(lon_vals):
            spd = speeds[min(i, len(speeds)-1)] if speeds is not None and len(speeds) > 0 else avg_speed
            threshold = self._get_threshold_for_speed('longitudinal_jerk', 'peak', spd)
            if abs(jerk_val) > threshold:
                lon_over_indices.append(i)
        
        # 获取典型阈值范围用于描述
        lon_low = self._get_threshold_for_speed('longitudinal_jerk', 'peak', 10)
        lon_high = self._get_threshold_for_speed('longitudinal_jerk', 'peak', 60)
        
        if len(lon_over_indices) == 0:
            return KPIResult(
                name="纵向Jerk超限事件",
                value=0,
                unit="次",
                description=f"纵向Jerk超限事件（阈值随速度变化: {lon_high}~{lon_low} m/s³）"
            )
        
        # 合并连续帧为事件（使用配置的最小事件间隔）
        max_gap_frames = 3
        max_gap_sec = self.min_event_gap_sec
        
        events = []
        groups = []
        current_group = [lon_over_indices[0]]
        
        for i in range(1, len(lon_over_indices)):
            prev_idx = lon_over_indices[i-1]
            curr_idx = lon_over_indices[i]
            frame_gap_ok = (curr_idx - prev_idx) <= max_gap_frames
            time_gap_ok = (lon_ts[curr_idx] - lon_ts[prev_idx]) <= max_gap_sec
            
            if frame_gap_ok and time_gap_ok:
                current_group.append(curr_idx)
            else:
                groups.append(current_group)
                current_group = [curr_idx]
        groups.append(current_group)
        
        # 为每组创建事件
        for group in groups[:50]:  # 最多 50 个事件
            start_idx = group[0]
            end_idx = group[-1]
            start_ts = float(lon_ts[start_idx])
            end_ts = float(lon_ts[end_idx])
            
            # 获取事件发生时的速度和对应阈值
            event_speed = speeds[min(start_idx, len(speeds)-1)] if speeds is not None and len(speeds) > 0 else avg_speed
            event_thresh = self._get_threshold_for_speed('longitudinal_jerk', 'peak', event_speed)
            
            # 计算实际有效持续时间
            if len(group) > 1:
                group_timestamps = [float(lon_ts[idx]) for idx in group]
                ts_diffs = [group_timestamps[j+1] - group_timestamps[j] for j in range(len(group_timestamps)-1)]
                valid_diffs = [d for d in ts_diffs if d <= max_gap_sec]
                duration = sum(valid_diffs)
            else:
                duration = 0.0
            
            # 计算峰值
            lon_peak = max(abs(lon_vals[idx]) for idx in group)
            
            # 生成描述
            desc = f"纵向Jerk超限@{event_speed:.0f}km/h：{lon_peak:.2f}>{event_thresh}m/s³"
            if len(group) > 1:
                desc += f"，持续{len(group)}帧/{duration:.2f}s"
            
            events.append(AnomalyRecord(
                timestamp=start_ts,
                bag_name=bag_mapper.get_bag_name(start_ts),
                description=desc,
                value={'lon_peak': float(lon_peak)},
                peak_value=float(lon_peak),
                threshold={'lon': event_thresh, 'speed_kph': event_speed},
                end_timestamp=end_ts if duration > 0 else 0,
                frame_count=len(group)
            ))
        
        result = KPIResult(
            name="纵向Jerk超限事件",
            value=len(events),
            unit="次",
            description=f"纵向Jerk超限事件数（阈值随速度: {lon_high}~{lon_low} m/s³）",
            details={
                'event_count': len(events),
                'threshold_at_10kph': lon_low,
                'threshold_at_60kph': lon_high,
                'min_event_gap_sec': self.min_event_gap_sec
            }
        )
        result.anomalies = events
        
        return result
    
    def _compute_comfort_score(self, lon_stats: Dict, avg_speed: float = 30.0) -> Dict:
        """
        计算综合舒适性评分（0-100）- 仅纵向
        
        评分逻辑：
        - 基础分 100 分
        - 根据纵向指标扣分，扣分项有上限
        - 使用速度相关的阈值
        
        扣分权重（基于行业经验）：
        - 纵向 RMS(1s) max: 权重高，影响体感最直接
        - 瞬时峰值超阈值：突发冲击
        """
        score = 100.0
        deductions = {}
        
        # 获取速度相关阈值
        lon_rms_threshold = self._get_threshold_for_speed('longitudinal_jerk', 'rms', avg_speed)
        lon_peak_threshold = self._get_threshold_for_speed('longitudinal_jerk', 'peak', avg_speed)
        
        # 1. 纵向 RMS(1s) 扣分（最多扣 40 分）
        lon_rms_1s_max = lon_stats.get('j_rms_1s_max', 0)
        if lon_rms_1s_max > lon_rms_threshold:
            # 超过阈值部分，每 1 m/s³ 扣 10 分
            lon_deduct = min(40, (lon_rms_1s_max - lon_rms_threshold) * 10)
            score -= lon_deduct
            deductions['lon_rms_deduct'] = round(lon_deduct, 1)
        
        # 2. 纵向峰值扣分（P99 > 阈值，最多扣 30 分）
        lon_p99 = lon_stats.get('j_p99', 0)
        if lon_p99 > lon_peak_threshold:
            peak_deduct = min(30, (lon_p99 - lon_peak_threshold) * 5)
            score -= peak_deduct
            deductions['lon_peak_deduct'] = round(peak_deduct, 1)
        
        # 3. 纵向 Jerk 均值扣分（最多扣 20 分）
        lon_abs_mean = lon_stats.get('j_abs_mean', 0)
        mean_threshold = lon_rms_threshold * 0.5  # 均值阈值为 RMS 阈值的一半
        if lon_abs_mean > mean_threshold:
            mean_deduct = min(20, (lon_abs_mean - mean_threshold) * 10)
            score -= mean_deduct
            deductions['lon_mean_deduct'] = round(mean_deduct, 1)
        
        # 确保分数在 0-100 范围内
        score = max(0, min(100, score))
        
        # 评级
        if score >= 90:
            grade = 'A（优秀）'
        elif score >= 75:
            grade = 'B（良好）'
        elif score >= 60:
            grade = 'C（合格）'
        else:
            grade = 'D（需改进）'
        
        return {
            'score': score,
            'grade': grade,
            'deductions': deductions,
            'lon_rms_1s_max': round(lon_rms_1s_max, 3),
            'lon_p99': round(lon_p99, 3),
            'lon_abs_mean': round(lon_abs_mean, 3)
        }
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="纵向Jerk均值",
            value=0.0,
            unit="m/s³",
            description="数据不足，无法计算舒适性指标"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集加速度数据（流式模式）
        
        纵向加速度来源：/control/control.chassis_control.target_longitudinal_acceleration
        
        Args:
            synced_frames: 同步后的帧列表
            streaming_data: 中间数据容器
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
            
            # 获取速度
            speed = MessageAccessor.get_field(
                chassis_msg, "motion_system.vehicle_speed", None)  # km/h
            
            # 存储: (lon_acc, speed, timestamp)
            # 注意：低速过滤在 compute_from_collected 中进行，这里保留所有数据
            streaming_data.accelerations.append((
                lon_acc,
                speed,    # 可能为 None (km/h)
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算舒适性KPI（流式模式）
        
        特点：
        - 纵向加速度来源：/viz/control/control.chassis_control.target_longitudinal_acceleration
        - 低速过滤：速度 < min_valid_speed_kph 时不统计 jerk
        - 速度相关阈值：根据当前速度动态选择阈值
        """
        self.clear_results()
        
        if len(streaming_data.accelerations) < 10:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据: (lon_acc, speed, timestamp)
        data_records = streaming_data.accelerations
        all_timestamps = np.array([r[2] for r in data_records])
        all_lon_acc = np.array([r[0] for r in data_records])
        all_speeds = np.array([r[1] if r[1] is not None else 0.0 for r in data_records])
        
        # 低速过滤：只在 jerk 计算时使用高速数据
        high_speed_mask = all_speeds >= self.min_valid_speed_kph
        
        if np.sum(high_speed_mask) < 10:
            # 有效高速数据不足，使用全部数据（但标记）
            timestamps = all_timestamps
            lon_accelerations = all_lon_acc
            speeds = all_speeds
            low_speed_filtered = False
        else:
            timestamps = all_timestamps[high_speed_mask]
            lon_accelerations = all_lon_acc[high_speed_mask]
            speeds = all_speeds[high_speed_mask]
            low_speed_filtered = True
        
        # 计算平均速度（用于选择阈值）
        avg_speed = np.mean(speeds) if len(speeds) > 0 else 30.0
        
        # 获取 bag 时间映射器
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 纵向 Jerk 计算
        lon_jerk_result = SignalProcessor.compute_jerk_robust(
            timestamps, lon_accelerations,
            target_fs=self.target_fs,
            filter_method=self.filter_method,
            cutoff=self.cutoff_hz
        )
        
        if len(lon_jerk_result.values) == 0:
            self._add_empty_results()
            return self.get_results()
        
        lon_stats = lon_jerk_result.stats
        lon_params = lon_jerk_result.params
        
        # 纵向 Jerk 结果
        self.add_result(KPIResult(
            name="纵向Jerk均值",
            value=round(lon_stats['j_abs_mean'], 3),
            unit="m/s³",
            description=f"纵向 Jerk 绝对值均值（{lon_params['filter_method']} 滤波）",
            details={
                'mean': round(lon_stats['j_mean'], 4),
                'abs_mean': round(lon_stats['j_abs_mean'], 4),
                'std': round(lon_stats['j_std'], 4),
                'p95': round(lon_stats['j_p95'], 4),
                'rms_1s_max': round(lon_stats['j_rms_1s_max'], 4),
                'sample_count': lon_stats['sample_count']
            }
        ))
        
        self.add_result(KPIResult(
            name="纵向Jerk P95",
            value=round(lon_stats['j_p95'], 3),
            unit="m/s³",
            description="纵向 Jerk 绝对值的 95 分位数"
        ))
        
        # 获取速度相关阈值
        lon_rms_threshold = self._get_threshold_for_speed('longitudinal_jerk', 'rms', avg_speed)
        lon_peak_threshold = self._get_threshold_for_speed('longitudinal_jerk', 'peak', avg_speed)
        
        # 纵向 Jerk RMS(1s) 最大值（带超阈值事件检测）
        lon_rms_result = KPIResult(
            name="纵向Jerk RMS(1s)最大",
            value=round(lon_stats['j_rms_1s_max'], 3),
            unit="m/s³",
            description="1秒窗口内纵向 Jerk RMS 的最大值",
            details={
                'rms_1s_mean': round(lon_stats['j_rms_1s_mean'], 4),
                'rms_1s_max_ts': lon_stats.get('j_rms_1s_max_ts'),
                'threshold': lon_rms_threshold,
                'avg_speed_kph': round(avg_speed, 1),
                'low_speed_filtered': low_speed_filtered
            }
        )
        
        # RMS(1s) 超阈值事件
        if lon_stats['j_rms_1s_max'] > lon_rms_threshold:
            rms_max_ts = lon_stats.get('j_rms_1s_max_ts', timestamps[len(timestamps)//2])
            lon_rms_result.add_anomaly(
                timestamp=float(rms_max_ts),
                bag_name=bag_mapper.get_bag_name(float(rms_max_ts)),
                description=f"纵向Jerk RMS(1s)超阈值：{lon_stats['j_rms_1s_max']:.2f} m/s³ > {lon_rms_threshold} (速度{avg_speed:.0f}km/h)",
                value=float(lon_stats['j_rms_1s_max']),
                threshold=lon_rms_threshold
            )
        self.add_result(lon_rms_result)
        
        # 纵向 Jerk 1s窗口 RMS P95
        self.add_result(KPIResult(
            name="纵向Jerk RMS(1s)P95",
            value=round(lon_stats['j_rms_1s_p95'], 3),
            unit="m/s³",
            description="1秒窗口内纵向 Jerk RMS 的 95 分位数"
        ))
        
        # Jerk 超限事件检测（仅纵向）
        jerk_events_result = self._create_jerk_events(
            lon_jerk_result, bag_mapper,
            speeds=speeds, avg_speed=avg_speed)
        if jerk_events_result:
            self.add_result(jerk_events_result)
        
        # ========== 加速度统计（按实时速度判断超限） ==========
        # 为每个数据点根据实时速度获取阈值并判断超限
        
        # 纵向加速度超限判断（正值 = 加速，负值 = 减速）
        acc_exceed_count = 0
        dec_exceed_count = 0
        pos_acc_indices = lon_accelerations > 0
        neg_acc_indices = lon_accelerations < 0
        pos_acc = lon_accelerations[pos_acc_indices]
        neg_acc = lon_accelerations[neg_acc_indices]
        pos_speeds = speeds[pos_acc_indices] if len(speeds) == len(lon_accelerations) else speeds
        neg_speeds = speeds[neg_acc_indices] if len(speeds) == len(lon_accelerations) else speeds
        
        # 逐点判断加速超限
        for i, (acc_val, spd) in enumerate(zip(pos_acc, pos_speeds if len(pos_speeds) == len(pos_acc) else [avg_speed]*len(pos_acc))):
            threshold = self._get_acc_threshold_for_speed('longitudinal_acceleration', 'warn', spd)
            if acc_val > threshold:
                acc_exceed_count += 1
        
        # 逐点判断减速超限
        for i, (dec_val, spd) in enumerate(zip(np.abs(neg_acc), neg_speeds if len(neg_speeds) == len(neg_acc) else [avg_speed]*len(neg_acc))):
            threshold = self._get_acc_threshold_for_speed('longitudinal_deceleration', 'warn', spd)
            if dec_val > threshold:
                dec_exceed_count += 1
        
        # 纵向加速度统计
        if len(pos_acc) > 0:
            acc_exceed_ratio = acc_exceed_count / len(pos_acc) * 100
            # 获取典型阈值范围用于描述
            low_speed_thresh = self._get_acc_threshold_for_speed('longitudinal_acceleration', 'warn', 10)
            high_speed_thresh = self._get_acc_threshold_for_speed('longitudinal_acceleration', 'warn', 60)
            self.add_result(KPIResult(
                name="纵向加速度峰值",
                value=round(np.max(pos_acc), 3),
                unit="m/s²",
                description=f"纵向加速度最大值（阈值随速度变化: {high_speed_thresh}~{low_speed_thresh}m/s²）",
                details={
                    'mean': round(np.mean(pos_acc), 3),
                    'p95': round(np.percentile(pos_acc, 95), 3),
                    'exceed_count': acc_exceed_count,
                    'total_count': len(pos_acc),
                    'threshold_at_10kph': low_speed_thresh,
                    'threshold_at_60kph': high_speed_thresh
                }
            ))
            self.add_result(KPIResult(
                name="加速超限率",
                value=round(acc_exceed_ratio, 2),
                unit="%",
                description="加速度超过当前速度对应阈值的比例（实时判断）"
            ))
        
        # 纵向减速度统计
        if len(neg_acc) > 0:
            dec_values = np.abs(neg_acc)
            dec_exceed_ratio = dec_exceed_count / len(dec_values) * 100
            low_speed_thresh = self._get_acc_threshold_for_speed('longitudinal_deceleration', 'warn', 10)
            high_speed_thresh = self._get_acc_threshold_for_speed('longitudinal_deceleration', 'warn', 60)
            self.add_result(KPIResult(
                name="纵向减速度峰值",
                value=round(np.max(dec_values), 3),
                unit="m/s²",
                description=f"纵向减速度最大值（阈值随速度变化: {high_speed_thresh}~{low_speed_thresh}m/s²）",
                details={
                    'mean': round(np.mean(dec_values), 3),
                    'p95': round(np.percentile(dec_values, 95), 3),
                    'exceed_count': dec_exceed_count,
                    'total_count': len(dec_values),
                    'threshold_at_10kph': low_speed_thresh,
                    'threshold_at_60kph': high_speed_thresh
                }
            ))
            self.add_result(KPIResult(
                name="减速超限率",
                value=round(dec_exceed_ratio, 2),
                unit="%",
                description="减速度超过当前速度对应阈值的比例（实时判断）"
            ))
        
        # Jerk 超限率（逐点根据速度判断）
        lon_jerk_exceed_count = 0
        for i, jerk_val in enumerate(lon_jerk_result.values):
            # 找到对应的速度（通过时间戳匹配或索引）
            spd = speeds[min(i, len(speeds)-1)] if len(speeds) > 0 else avg_speed
            threshold = self._get_threshold_for_speed('longitudinal_jerk', 'peak', spd)
            if abs(jerk_val) > threshold:
                lon_jerk_exceed_count += 1
        lon_jerk_exceed = lon_jerk_exceed_count / len(lon_jerk_result.values) * 100
        
        low_speed_thresh = self._get_threshold_for_speed('longitudinal_jerk', 'peak', 10)
        high_speed_thresh = self._get_threshold_for_speed('longitudinal_jerk', 'peak', 60)
        self.add_result(KPIResult(
            name="纵向Jerk超限率",
            value=round(lon_jerk_exceed, 2),
            unit="%",
            description=f"纵向Jerk超过当前速度阈值的比例（阈值: {high_speed_thresh}~{low_speed_thresh}m/s³）"
        ))
        
        # 速度统计和舒适性评分暂时不输出
        # if len(speeds) > 0:
        #     speed_stats = SignalProcessor.compute_statistics(speeds)
        #     self.add_result(KPIResult(
        #         name="平均速度",
        #         value=round(speed_stats['mean'], 2),
        #         unit="km/h",
        #         description="自动驾驶状态下的平均速度（用于阈值选择）"
        #     ))
        # 
        # comfort_score = self._compute_comfort_score(lon_stats, avg_speed=avg_speed)
        # self.add_result(KPIResult(
        #     name="舒适性综合评分",
        #     value=round(comfort_score['score'], 1),
        #     unit="分",
        #     description=f"综合舒适性评分（0-100），平均速度{avg_speed:.0f}km/h",
        #     details={
        #         **comfort_score,
        #         'avg_speed_kph': round(avg_speed, 1),
        #         'low_speed_filtered': low_speed_filtered,
        #         'min_valid_speed_kph': self.min_valid_speed_kph
        #     }
        # ))
        
        return self.get_results()
