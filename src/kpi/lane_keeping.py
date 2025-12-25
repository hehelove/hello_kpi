"""
控制指标KPI
计算直道和弯道的横向偏差
使用规划轨迹的kappa值判断直道/弯道
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import numpy as np

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, merge_consecutive_events, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.signal import SignalProcessor
from ..utils.geo import GeoConverter


@dataclass
class LaneKeepingSegment:
    """车道保持段"""
    start_time: float
    end_time: float
    road_type: str  # 'straight' or 'curve'
    lateral_errors: List[float]
    
    @property
    def mean_error(self) -> float:
        if len(self.lateral_errors) == 0:
            return 0.0
        return float(np.mean(np.abs(self.lateral_errors)))
    
    @property
    def max_error(self) -> float:
        if len(self.lateral_errors) == 0:
            return 0.0
        return float(np.max(np.abs(self.lateral_errors)))
    
    @property
    def std_error(self) -> float:
        if len(self.lateral_errors) == 0:
            return 0.0
        return float(np.std(self.lateral_errors))


class LaneKeepingKPI(BaseKPI):
    """控制指标KPI"""
    
    @property
    def name(self) -> str:
        return "控制指标"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/control/debug",  # reserved0需要proto解析
            "/vehicle/chassis_domain_report",
            "/planning/trajectory",  # 用于获取kappa判断直道/弯道
            "/localization/localization"  # 用于获取自车位置
        ]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        self.curvature_threshold = self.config.get('kpi', {}).get(
            'road_type', {}).get('curvature_threshold', 0.002)
        
        # 初始化地理坐标转换器
        base_config = self.config.get('base', {})
        utm_zone = base_config.get('utm_zone', 51)
        hemisphere = base_config.get('hemisphere', 'N')
        self.geo_converter = GeoConverter(utm_zone=utm_zone, hemisphere=hemisphere)
        
        # 最大匹配距离阈值
        self.max_distance_threshold = self.config.get('kpi', {}).get(
            'curvature', {}).get('max_distance_threshold', 5.0)
        
        # 定位可信度配置
        loc_config = self.config.get('kpi', {}).get('localization', {})
        self.loc_valid_status = [3, 7]  # 有效的定位状态
        self.loc_max_stddev = loc_config.get('medium_stddev_threshold', 0.2)  # 最大允许标准差 (m)
        
        self._debug_timestamps = None  # 缓存时间戳列表
    
    def _find_nearest_debug_data(self, parsed_data: Dict, target_ts: float, 
                                   key: str, tolerance: float = 0.05) -> Optional[float]:
        """
        根据时间戳查找最近的调试数据
        
        Args:
            parsed_data: 解析后的调试数据 {timestamp: {key: value}}
            target_ts: 目标时间戳
            key: 要获取的键名
            tolerance: 时间容差(秒)
        """
        if not parsed_data:
            return None
        
        # 缓存排序后的时间戳
        if self._debug_timestamps is None:
            self._debug_timestamps = sorted(parsed_data.keys())
        
        # 二分查找最近的时间戳
        timestamps = self._debug_timestamps
        left, right = 0, len(timestamps) - 1
        
        while left < right:
            mid = (left + right) // 2
            if timestamps[mid] < target_ts:
                left = mid + 1
            else:
                right = mid
        
        # 检查前后两个位置
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
        """计算控制指标KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, 
                                           parsed_debug_data=parsed_debug_data, 
                                           **kwargs)
    
    def _get_trajectory_kappa(self, loc_msg: Any, traj_msg: Any) -> float:
        """
        从规划轨迹获取当前位置的kappa值
        
        Args:
            loc_msg: 定位消息
            traj_msg: 轨迹消息
            
        Returns:
            kappa值，获取失败返回0.0
        """
        if loc_msg is None or traj_msg is None:
            return 0.0
        
        # 获取自车经纬度
        ego_lat = MessageAccessor.get_field(
            loc_msg, "global_localization.position.latitude", None)
        ego_lon = MessageAccessor.get_field(
            loc_msg, "global_localization.position.longitude", None)
        
        if ego_lat is None or ego_lon is None or ego_lat == 0 or ego_lon == 0:
            return 0.0
        
        # 提取路径点
        path_points = self._extract_path_points(traj_msg)
        
        if len(path_points) == 0:
            return 0.0
        
        # 将自车经纬度转换为UTM坐标
        try:
            ego_x, ego_y = self.geo_converter.latlon_to_utm(ego_lat, ego_lon)
        except Exception:
            return 0.0
        
        # 向量化查找最近点
        xs = np.array([pt['x'] for pt in path_points])
        ys = np.array([pt['y'] for pt in path_points])
        kappas = np.array([pt['kappa'] for pt in path_points])
        
        # 计算距离平方
        dist_sq = (xs - ego_x) ** 2 + (ys - ego_y) ** 2
        min_idx = np.argmin(dist_sq)
        min_distance = np.sqrt(dist_sq[min_idx])
        
        # 距离超过阈值则返回0
        if min_distance > self.max_distance_threshold:
            return 0.0
        
        return kappas[min_idx]
    
    def _extract_path_points(self, traj_msg: Any) -> List[Dict]:
        """
        从轨迹消息中提取路径点
        
        优先使用 path_point[] 数组，如果为空则使用 trajectory_point[].path_point
        
        Args:
            traj_msg: 轨迹消息
            
        Returns:
            路径点列表，每个点包含 x, y, kappa
        """
        path_points = []
        
        # 方法1: 尝试从 path_point[] 获取
        raw_path_points = MessageAccessor.get_field(traj_msg, "path_point", [])
        
        if raw_path_points and len(raw_path_points) > 0:
            for pt in raw_path_points:
                x = MessageAccessor.get_field(pt, "x", None)
                y = MessageAccessor.get_field(pt, "y", None)
                kappa = MessageAccessor.get_field(pt, "kappa", 0.0)
                
                if x is not None and y is not None:
                    path_points.append({
                        'x': x,
                        'y': y,
                        'kappa': kappa
                    })
        
        # 方法2: 如果 path_point 为空，从 trajectory_point[].path_point 获取
        # if len(path_points) == 0:
        #     traj_points = MessageAccessor.get_field(traj_msg, "trajectory_point", [])
            
        #     if traj_points:
        #         for traj_pt in traj_points:
        #             pt = MessageAccessor.get_field(traj_pt, "path_point", None)
        #             if pt is not None:
        #                 x = MessageAccessor.get_field(pt, "x", None)
        #                 y = MessageAccessor.get_field(pt, "y", None)
        #                 kappa = MessageAccessor.get_field(pt, "kappa", 0.0)
                        
        #                 if x is not None and y is not None:
        #                     path_points.append({
        #                         'x': x,
        #                         'y': y,
        #                         'kappa': kappa
        #                     })
        
        return path_points
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="全路段横向偏差均值",
            value=0.0,
            unit="cm",
            description="数据不足，无法计算控制指标"
        ))
        self.add_result(KPIResult(
            name="最大横向偏移",
            value=0.0,
            unit="cm",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, 
                parsed_debug_data: Optional[Dict] = None, **kwargs):
        """
        收集横向偏差数据（流式模式）
        """
        self._debug_timestamps = None  # 重置缓存
        
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            loc_msg = frame.messages.get("/localization/localization")
            traj_msg = frame.messages.get("/planning/trajectory")
            
            if fm_msg is None:
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
            
            # 获取横向偏差
            lat_error = None
            if parsed_debug_data is not None and len(parsed_debug_data) > 0:
                lat_error = self._find_nearest_debug_data(
                    parsed_debug_data, frame.timestamp, 'lateral_error', tolerance=0.05)
            
            if lat_error is None:
                continue
            
            # 获取轨迹 kappa
            kappa = self._get_trajectory_kappa(loc_msg, traj_msg)
            
            # 存储: (lateral_error, kappa, timestamp)
            streaming_data.lateral_errors.append((
                lat_error,
                kappa,
                frame.timestamp
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算控制指标KPI（流式模式）
        """
        self.clear_results()
        
        if len(streaming_data.lateral_errors) < 10:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据
        lateral_errors = np.array([d[0] for d in streaming_data.lateral_errors])
        kappas = np.array([d[1] for d in streaming_data.lateral_errors])
        timestamps = np.array([d[2] for d in streaming_data.lateral_errors])
        
        # 使用轨迹 kappa 分类直道/弯道
        road_types = ['straight' if abs(k) < self.curvature_threshold else 'curve' 
                      for k in kappas]
        
        # 分别统计
        straight_errors = [lateral_errors[i] for i in range(len(road_types)) 
                          if road_types[i] == 'straight']
        curve_errors = [lateral_errors[i] for i in range(len(road_types)) 
                       if road_types[i] == 'curve']
        
        # 全路段统计
        all_stats = SignalProcessor.compute_statistics(lateral_errors)
        straight_stats = SignalProcessor.compute_statistics(np.array(straight_errors)) if straight_errors else {}
        curve_stats = SignalProcessor.compute_statistics(np.array(curve_errors)) if curve_errors else {}
        
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # 添加结果
        self.add_result(KPIResult(
            name="全路段横向偏差均值",
            value=round(all_stats['abs_mean'] * 100, 2),
            unit="cm",
            description="全路段的平均横向偏差绝对值",
            details={
                'mean_m': all_stats['mean'],
                'std_m': all_stats['std'],
                'max_m': all_stats['max'],
                'rms_m': all_stats['rms'],
                'p95_m': all_stats['p95'],
                'sample_count': len(lateral_errors)
            }
        ))
        
        self.add_result(KPIResult(
            name="横向偏差P95",
            value=round(all_stats['p95'] * 100, 2),
            unit="cm",
            description="横向偏差绝对值的95分位数"
        ))
        
        self.add_result(KPIResult(
            name="最大横向偏移",
            value=round(all_stats['max'] * 100, 2),
            unit="cm",
            description="自动驾驶过程中的最大横向偏移"
        ))
        
        if straight_stats:
            self.add_result(KPIResult(
                name="直道保持精度",
                value=round(straight_stats['abs_mean'] * 100, 2),
                unit="cm",
                description="直道的平均横向偏差",
                details={
                    'sample_count': len(straight_errors),
                    'percentage': round(len(straight_errors) / len(lateral_errors) * 100, 2)
                }
            ))
        
        if curve_stats:
            self.add_result(KPIResult(
                name="弯道保持精度",
                value=round(curve_stats['abs_mean'] * 100, 2),
                unit="cm",
                description="弯道的平均横向偏差",
                details={
                    'sample_count': len(curve_errors),
                    'percentage': round(len(curve_errors) / len(lateral_errors) * 100, 2)
                }
            ))
        
        # 超限统计（包含溯源记录）
        threshold_10cm = 0.10
        threshold_20cm = 0.20
        threshold_30cm = 0.30
        
        over_10cm_mask = np.abs(lateral_errors) > threshold_10cm
        over_20cm_mask = np.abs(lateral_errors) > threshold_20cm
        over_30cm_mask = np.abs(lateral_errors) > threshold_30cm
        
        over_10cm = np.sum(over_10cm_mask)
        over_20cm = np.sum(over_20cm_mask)
        over_30cm = np.sum(over_30cm_mask)
        
        # 合并连续帧事件，统计实际事件次数
        over_30cm_indices = np.where(over_30cm_mask)[0]
        merged_events = merge_consecutive_events(
            indices=list(over_30cm_indices),
            timestamps=list(timestamps),
            values=list(lateral_errors * 100),  # 转为 cm
            bag_mapper=bag_mapper,
            description_template="横向偏差超限：均值{value}cm，峰值{peak}cm，持续{count}帧/{duration}s",
            threshold=30.0,
            max_gap_frames=3,
            max_events=50
        )
        
        over_limit_result = KPIResult(
            name="横向偏差超限率",
            value=round(over_20cm / len(lateral_errors) * 100, 2),
            unit="%",
            description="横向偏差超过20cm的比例",
            details={
                'over_10cm_count': int(over_10cm),
                'over_10cm_rate': round(over_10cm / len(lateral_errors) * 100, 2),
                'over_20cm_count': int(over_20cm),
                'over_20cm_rate': round(over_20cm / len(lateral_errors) * 100, 2),
                'over_30cm_count': int(over_30cm),
                'over_30cm_rate': round(over_30cm / len(lateral_errors) * 100, 2),
                'over_30cm_event_count': len(merged_events)
            }
        )
        
        # 添加合并后的事件
        for event in merged_events:
            over_limit_result.anomalies.append(event)
        
        self.add_result(over_limit_result)
        
        return self.get_results()

