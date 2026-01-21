"""
道路平均曲率KPI
根据规划轨迹和自车定位计算道路平均曲率
"""
import os
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass
import numpy as np

from .base_kpi import BaseKPI, KPIResult, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.geo import GeoConverter
from ..utils.map_visualizer import MapVisualizer, TrajectoryPoint


@dataclass
class CurvaturePoint:
    """曲率采样点"""
    timestamp: float
    ego_x: float  # 自车UTM x坐标
    ego_y: float  # 自车UTM y坐标
    nearest_x: float  # 最近路径点 x
    nearest_y: float  # 最近路径点 y
    distance: float  # 到最近点的距离
    kappa: float  # 曲率值


@dataclass
class MatchedPair:
    """匹配点对：自车位置 + 最近 refline 点"""
    ego_lat: float
    ego_lon: float
    refline_lat: float
    refline_lon: float
    distance: float
    kappa: float
    timestamp: float


class CurvatureKPI(BaseKPI):
    """道路平均曲率KPI"""
    
    @property
    def name(self) -> str:
        return "道路平均曲率"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            "/function/function_manager",
            "/localization/localization",
            "/planning/trajectory"
        ]
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        
        # 曲率相关配置
        curvature_config = self.config.get('kpi', {}).get('curvature', {})
        self.max_distance_threshold = curvature_config.get('max_distance_threshold', 5.0)  # 最大匹配距离阈值(m)
        
        road_type_config = self.config.get('kpi', {}).get('road_type', {})
        # 直道/弯道判断阈值（曲率 = 1/半径）
        self.curvature_threshold = road_type_config.get('curvature_threshold', 0.003)  # 直道阈值：曲率<0.003 = 半径>333m
        # 小弯道/大弯道判断阈值
        self.large_curve_threshold = road_type_config.get('large_curve_threshold', 0.02)  # 大弯道阈值：曲率>=0.02 = 半径<50m
        
        # 初始化地理坐标转换器
        base_config = self.config.get('base', {})
        utm_zone = base_config.get('utm_zone', 51)
        hemisphere = base_config.get('hemisphere', 'N')
        self.geo_converter = GeoConverter(utm_zone=utm_zone, hemisphere=hemisphere)
        
        # 地图可视化配置
        viz_config = curvature_config.get('map_viz', {})
        self.map_viz_enabled = viz_config.get('enabled', True)
        self.map_viz_output_dir = viz_config.get('output_dir', 'output/map_viz')
        self.mapbox_token = viz_config.get('mapbox_token', None)
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算道路平均曲率KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _generate_map_visualization(self, matched_pairs: List[MatchedPair]) -> None:
        """
        生成 Mapbox 地图可视化
        
        同时显示自车位置点和匹配的最近 refline 点
        
        Args:
            matched_pairs: 匹配点对列表 (WGS84坐标)
        """
        try:
            if not matched_pairs:
                return
            
            # 创建地图可视化器
            visualizer = MapVisualizer(mapbox_token=self.mapbox_token)
            
            # 确保输出目录存在
            os.makedirs(self.map_viz_output_dir, exist_ok=True)
            
            # 生成 HTML 地图
            output_path = os.path.join(self.map_viz_output_dir, "refline_map.html")
            
            visualizer.generate_matched_pairs_map(
                matched_pairs=matched_pairs,
                output_path=output_path,
                title="Refline 匹配点可视化 (自车位置 vs 最近点)",
                curvature_threshold=self.curvature_threshold,
                large_curve_threshold=self.large_curve_threshold
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            pass  # 地图可视化失败不影响主流程
    
    def _extract_path_points(self, traj_msg: Any) -> List[Dict]:
        """
        从轨迹消息中提取 refline 路径点
        
        使用 path_point[] 数组 (UTM 坐标)
        
        Args:
            traj_msg: 轨迹消息
            
        Returns:
            路径点列表，每个点包含 x, y, kappa
        """
        path_points = []
        
        # 从 path_point[] 获取 refline 点
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
        
        return path_points
    
    def _find_nearest_point(self, ego_x: float, ego_y: float,
                            path_points: List[Dict]) -> Optional[tuple]:
        """
        查找距离自车最近的路径点，并使用垂足投影计算精确距离
        
        Args:
            ego_x: 自车UTM x坐标
            ego_y: 自车UTM y坐标
            path_points: 路径点列表
            
        Returns:
            (nearest_x, nearest_y, distance, kappa) 或 None
            - nearest_x, nearest_y: 最近点坐标 (用于可视化)
            - distance: 垂足投影距离 (更精确)
            - kappa: 最近点的曲率值
        """
        if not path_points:
            return None
        
        n = len(path_points)
        
        # 向量化处理：构建numpy数组
        xs = np.array([pt['x'] for pt in path_points])
        ys = np.array([pt['y'] for pt in path_points])
        kappas = np.array([pt['kappa'] for pt in path_points])
        
        # 向量化计算所有距离的平方 (避免sqrt以提高性能)
        dist_sq = (xs - ego_x) ** 2 + (ys - ego_y) ** 2
        
        # 找到最小距离的索引
        min_idx = np.argmin(dist_sq)
        
        # 获取最近点的 kappa (用于判断直道/弯道)
        nearest_kappa = kappas[min_idx]
        nearest_x = xs[min_idx]
        nearest_y = ys[min_idx]
        
        # 使用垂足投影计算精确距离
        projected_distance = self._compute_projected_distance(
            ego_x, ego_y, xs, ys, kappas, min_idx
        )
        
        return (nearest_x, nearest_y, projected_distance, nearest_kappa)
    
    def _compute_projected_distance(self, ego_x: float, ego_y: float,
                                     xs: np.ndarray, ys: np.ndarray,
                                     kappas: np.ndarray, min_idx: int) -> float:
        """
        使用垂足投影计算自车到 refline 的精确距离
        
        算法：
        1. 取最近点及其前后点形成线段
        2. 计算自车位置到线段的垂足投影
        3. 如果是弯道，进行曲率补偿
        
        Args:
            ego_x, ego_y: 自车位置
            xs, ys: refline 点坐标数组
            kappas: 曲率数组
            min_idx: 最近点索引
            
        Returns:
            垂足投影距离 (带弯道补偿)
        """
        n = len(xs)
        
        # 确定要检查的线段 (最近点前后的两个线段)
        segments = []
        
        # 线段1: min_idx-1 到 min_idx
        if min_idx > 0:
            segments.append((min_idx - 1, min_idx))
        
        # 线段2: min_idx 到 min_idx+1
        if min_idx < n - 1:
            segments.append((min_idx, min_idx + 1))
        
        if not segments:
            # 只有一个点，返回点到点距离
            return np.sqrt((xs[min_idx] - ego_x) ** 2 + (ys[min_idx] - ego_y) ** 2)
        
        # 计算到每个线段的垂足距离，取最小值
        min_proj_distance = float('inf')
        best_kappa = kappas[min_idx]
        
        for i, j in segments:
            proj_dist, on_segment = self._point_to_segment_distance(
                ego_x, ego_y, xs[i], ys[i], xs[j], ys[j]
            )
            
            if proj_dist < min_proj_distance:
                min_proj_distance = proj_dist
                # 使用线段两端点的平均曲率
                best_kappa = (kappas[i] + kappas[j]) / 2
        
        # 弯道补偿：在高曲率区域，离散线段会产生弦误差
        # 弦误差约为 (弧长^2 * 曲率) / 8，这里简化处理
        abs_kappa = abs(best_kappa)
        if abs_kappa > self.curvature_threshold:
            # 估算线段长度 (用于补偿计算)
            if len(segments) > 0:
                i, j = segments[0]
                segment_len = np.sqrt((xs[j] - xs[i]) ** 2 + (ys[j] - ys[i]) ** 2)
                # 弦误差补偿: chord_error ≈ (L^2 * κ) / 8
                chord_error = (segment_len ** 2 * abs_kappa) / 8
                # 从距离中减去弦误差的一部分 (保守估计)
                min_proj_distance = max(0, min_proj_distance - chord_error * 0.5)
        
        return min_proj_distance
    
    def _point_to_segment_distance(self, px: float, py: float,
                                    x1: float, y1: float,
                                    x2: float, y2: float) -> Tuple[float, bool]:
        """
        计算点到线段的垂足投影距离
        
        Args:
            px, py: 点坐标
            x1, y1: 线段起点
            x2, y2: 线段终点
            
        Returns:
            (distance, on_segment): 距离和垂足是否在线段上
        """
        # 线段向量
        dx = x2 - x1
        dy = y2 - y1
        
        # 线段长度的平方
        seg_len_sq = dx * dx + dy * dy
        
        if seg_len_sq < 1e-10:
            # 线段退化为点
            return np.sqrt((px - x1) ** 2 + (py - y1) ** 2), True
        
        # 计算投影参数 t (0 <= t <= 1 表示垂足在线段上)
        t = ((px - x1) * dx + (py - y1) * dy) / seg_len_sq
        
        if t < 0:
            # 垂足在线段起点之前，使用起点距离
            return np.sqrt((px - x1) ** 2 + (py - y1) ** 2), False
        elif t > 1:
            # 垂足在线段终点之后，使用终点距离
            return np.sqrt((px - x2) ** 2 + (py - y2) ** 2), False
        else:
            # 垂足在线段上，计算垂直距离
            proj_x = x1 + t * dx
            proj_y = y1 + t * dy
            return np.sqrt((px - proj_x) ** 2 + (py - proj_y) ** 2), True
    
    @staticmethod
    def find_nearest_point_vectorized(ego_x: float, ego_y: float,
                                       path_xs: np.ndarray, path_ys: np.ndarray,
                                       path_kappas: np.ndarray) -> Optional[tuple]:
        """
        静态方法：向量化查找最近点 (供其他KPI模块调用)
        
        Args:
            ego_x: 自车UTM x坐标
            ego_y: 自车UTM y坐标
            path_xs: 路径点x坐标数组
            path_ys: 路径点y坐标数组
            path_kappas: 路径点kappa数组
            
        Returns:
            (nearest_x, nearest_y, distance, kappa) 或 None
        """
        if len(path_xs) == 0:
            return None
        
        # 向量化计算所有距离的平方
        dist_sq = (path_xs - ego_x) ** 2 + (path_ys - ego_y) ** 2
        
        # 找到最小距离的索引
        min_idx = np.argmin(dist_sq)
        
        # 只对最近点计算实际距离
        min_distance = np.sqrt(dist_sq[min_idx])
        
        return (path_xs[min_idx], path_ys[min_idx], min_distance, path_kappas[min_idx])
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="道路平均曲率",
            value=0.0,
            unit="1/m",
            description="数据不足，无法计算道路平均曲率"
        ))
        self.add_result(KPIResult(
            name="直道比例",
            value=0.0,
            unit="%",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="弯道比例",
            value=0.0,
            unit="%",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="小弯道比例",
            value=0.0,
            unit="%",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="大弯道比例",
            value=0.0,
            unit="%",
            description="数据不足"
        ))
        self.add_result(KPIResult(
            name="轨迹匹配平均距离",
            value=0.0,
            unit="m",
            description="数据不足"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集曲率数据（流式模式）
        """
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
            
            if loc_msg is None or traj_msg is None:
                continue
            
            # 获取自车定位
            ego_lat = MessageAccessor.get_field(
                loc_msg, "global_localization.position.latitude", None)
            ego_lon = MessageAccessor.get_field(
                loc_msg, "global_localization.position.longitude", None)
            
            if ego_lat is None or ego_lon is None or ego_lat == 0 or ego_lon == 0:
                continue
            
            # 提取路径点
            path_points = self._extract_path_points(traj_msg)
            if len(path_points) == 0:
                continue
            
            # 转换坐标
            try:
                ego_x, ego_y = self.geo_converter.latlon_to_utm(ego_lat, ego_lon)
            except Exception:
                continue
            
            # 查找最近点
            nearest_point = self._find_nearest_point(ego_x, ego_y, path_points)
            if nearest_point is None:
                continue
            
            nearest_x, nearest_y, distance, kappa = nearest_point
            
            if distance > self.max_distance_threshold:
                continue
            
            # 转换 refline 点坐标为经纬度（用于地图可视化）
            try:
                refline_lat, refline_lon = self.geo_converter.utm_to_latlon(nearest_x, nearest_y)
            except Exception:
                refline_lat, refline_lon = 0, 0
            
            # 存储: (kappa, distance, timestamp, ego_lat, ego_lon, refline_lat, refline_lon)
            streaming_data.curvature_data.append((
                kappa,
                distance,
                frame.timestamp,
                ego_lat,
                ego_lon,
                refline_lat,
                refline_lon
            ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算曲率KPI（流式模式）
        """
        self.clear_results()
        
        # 获取输出目录（用于可视化）
        output_dir = kwargs.get('output_dir')
        if output_dir:
            self.map_viz_output_dir = os.path.join(output_dir, 'map_viz')
        
        if len(streaming_data.curvature_data) == 0:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据 (kappa, distance, timestamp, ego_lat, ego_lon, refline_lat, refline_lon)
        all_kappas = np.array([abs(d[0]) for d in streaming_data.curvature_data])
        distances = np.array([d[1] for d in streaming_data.curvature_data])
        
        valid_frame_count = len(all_kappas)
        
        # 计算统计值
        mean_kappa = float(np.mean(all_kappas))
        std_kappa = float(np.std(all_kappas))
        max_kappa = float(np.max(all_kappas))
        min_kappa = float(np.min(all_kappas))
        median_kappa = float(np.median(all_kappas))
        
        # 统计直道/弯道比例（区分小弯道和大弯道）
        # 直道：曲率 < 0.003（半径 > 333m）
        # 小弯道（正常弯道）：0.003 <= 曲率 < 0.02（半径 50m ~ 333m）
        # 大弯道（路口转弯）：曲率 >= 0.02（半径 < 50m）
        straight_count = np.sum(all_kappas < self.curvature_threshold)
        small_curve_count = np.sum((all_kappas >= self.curvature_threshold) & (all_kappas < self.large_curve_threshold))
        large_curve_count = np.sum(all_kappas >= self.large_curve_threshold)
        curve_count = small_curve_count + large_curve_count
        
        straight_ratio = straight_count / valid_frame_count * 100
        curve_ratio = curve_count / valid_frame_count * 100
        small_curve_ratio = small_curve_count / valid_frame_count * 100
        large_curve_ratio = large_curve_count / valid_frame_count * 100
        
        # 统计距离
        mean_distance = float(np.mean(distances))
        max_distance = float(np.max(distances))
        
        # 添加结果
        self.add_result(KPIResult(
            name="道路平均曲率",
            value=round(mean_kappa, 6),
            unit="1/m",
            description="自动驾驶期间道路平均曲率(|kappa|的平均值)",
            details={
                'mean': round(mean_kappa, 6),
                'std': round(std_kappa, 6),
                'max': round(max_kappa, 6),
                'min': round(min_kappa, 6),
                'median': round(median_kappa, 6),
                'sample_count': valid_frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="直道比例",
            value=round(straight_ratio, 2),
            unit="%",
            description=f"曲率<{self.curvature_threshold}的路段占比",
            details={
                'straight_frames': int(straight_count),
                'total_frames': valid_frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="弯道比例",
            value=round(curve_ratio, 2),
            unit="%",
            description=f"曲率>={self.curvature_threshold}的路段占比（含小弯道和大弯道）",
            details={
                'curve_frames': int(curve_count),
                'small_curve_frames': int(small_curve_count),
                'large_curve_frames': int(large_curve_count),
                'total_frames': valid_frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="小弯道比例",
            value=round(small_curve_ratio, 2),
            unit="%",
            description=f"正常弯道：{self.curvature_threshold}<=曲率<{self.large_curve_threshold}（半径50m~333m）",
            details={
                'small_curve_frames': int(small_curve_count),
                'curvature_range': f'{self.curvature_threshold}~{self.large_curve_threshold}',
                'radius_range': '50m~333m'
            }
        ))
        
        self.add_result(KPIResult(
            name="大弯道比例",
            value=round(large_curve_ratio, 2),
            unit="%",
            description=f"路口转弯：曲率>={self.large_curve_threshold}（半径<50m）",
            details={
                'large_curve_frames': int(large_curve_count),
                'curvature_threshold': self.large_curve_threshold,
                'radius_threshold': '<50m'
            }
        ))
        
        self.add_result(KPIResult(
            name="轨迹匹配平均距离",
            value=round(mean_distance, 3),
            unit="m",
            description="自车位置与最近轨迹点的平均距离",
            details={
                'mean_distance': round(mean_distance, 3),
                'max_distance': round(max_distance, 3)
            }
        ))
        
        # 生成地图可视化（流式模式）
        if self.map_viz_enabled:
            # 构建 MatchedPair 列表（兼容新旧数据格式）
            matched_pairs = []
            for d in streaming_data.curvature_data:
                # 检查数据长度，兼容旧格式 (kappa, distance, timestamp)
                if len(d) >= 7:
                    kappa, distance, timestamp, ego_lat, ego_lon, refline_lat, refline_lon = d[:7]
                    if ego_lat != 0 and ego_lon != 0 and refline_lat != 0 and refline_lon != 0:
                        matched_pairs.append(MatchedPair(
                            ego_lat=ego_lat,
                            ego_lon=ego_lon,
                            refline_lat=refline_lat,
                            refline_lon=refline_lon,
                            distance=distance,
                            kappa=kappa,
                            timestamp=timestamp
                        ))
            
            if matched_pairs:
                self._generate_map_visualization(matched_pairs)
        
        return self.get_results()
