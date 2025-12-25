"""
ROI障碍物统计KPI
计算三级ROI区域（近/中/远）内的障碍物数量
支持分方向距离统计（前方/侧前方/侧方）
"""
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import os
import random
from pathlib import Path

from src.constants import Topics

from .base_kpi import BaseKPI, KPIResult, BagTimeMapper, AnomalyRecord, StreamingData
from ..data_loader.bag_reader import MessageAccessor
from ..utils.geometry import BoundingBox, Rectangle, create_ego_roi, check_box_in_roi

# 尝试导入matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.lines import Line2D
    import matplotlib.font_manager as fm
    
    # 尝试查找并加载中文字体
    def _find_chinese_font():
        """查找可用的中文字体"""
        # 优先尝试的字体列表
        preferred_fonts = [
            'Noto Sans CJK SC',
            'Noto Sans CJK SC Regular',
            'Source Han Sans CN',
            'WenQuanYi Micro Hei',
            'SimHei',
            'Microsoft YaHei',
        ]
        
        # 获取系统所有字体
        system_fonts = {f.name for f in fm.fontManager.ttflist}
        
        for font in preferred_fonts:
            if font in system_fonts:
                return font
        
        # 尝试从字体路径直接加载
        font_paths = [
            '/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc',
            '/usr/share/fonts/noto-cjk/NotoSansCJK-Regular.ttc',
        ]
        for path in font_paths:
            if os.path.exists(path):
                try:
                    fm.fontManager.addfont(path)
                    return 'Noto Sans CJK SC'
                except Exception:
                    pass
        
        return None
    
    _chinese_font = _find_chinese_font()
    
    if _chinese_font:
        plt.rcParams['font.sans-serif'] = [_chinese_font, 'DejaVu Sans', 'sans-serif']
    else:
        # 如果找不到中文字体，使用英文标签
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'sans-serif']
    
    plt.rcParams['axes.unicode_minus'] = False
    
    # 是否使用英文标签（找不到中文字体时自动切换）
    USE_ENGLISH_LABELS = _chinese_font is None
    
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    USE_ENGLISH_LABELS = True


class ObstacleDirection(Enum):
    """障碍物相对自车的方向"""
    FRONT = "front"
    FRONT_LEFT = "front_left"
    FRONT_RIGHT = "front_right"
    LEFT = "left"
    RIGHT = "right"
    REAR = "rear"
    REAR_LEFT = "rear_left"
    REAR_RIGHT = "rear_right"


@dataclass
class ObstacleInfo:
    """障碍物信息"""
    track_id: int  # 障碍物跟踪ID
    position_x: float  # 自车坐标系 x (前方为正)
    position_y: float  # 自车坐标系 y (左侧为正)
    length: float
    width: float
    heading_to_ego: float
    obstacle_type: int
    motion_status: int  # 1=moving, 3=static
    velocity_x: float = 0.0
    velocity_y: float = 0.0
    
    # 自车尺寸（由 KPI 类设置）
    ego_front: float = 3.79
    ego_rear: float = 0.883
    ego_left: float = 0.945
    ego_right: float = 0.945
    
    @property
    def is_static(self) -> bool:
        return self.motion_status == 3
    
    @property
    def type_name(self) -> str:
        type_map = {
            0: "Unk", 30: "Ped", 60: "Veh", 90: "Cyc",
            120: "Ani", 150: "Sta", 180: "Con"
        }
        return type_map.get(self.obstacle_type, "Unk")
    
    @property
    def type_name_cn(self) -> str:
        type_map = {
            0: "未知", 30: "人", 60: "车", 90: "自行车",
            120: "动物", 150: "静态", 180: "锥桶"
        }
        return type_map.get(self.obstacle_type, "未知")
    
    @property
    def center_distance(self) -> float:
        """障碍物中心点到后轴中心的距离（用于快速筛选）"""
        return np.sqrt(self.position_x ** 2 + self.position_y ** 2)
    
    def get_corners(self) -> List[Tuple[float, float]]:
        """获取障碍物四个角点坐标"""
        cos_yaw = np.cos(self.heading_to_ego)
        sin_yaw = np.sin(self.heading_to_ego)
        half_l, half_w = self.length / 2, self.width / 2
        
        local_corners = [(-half_l, -half_w), (-half_l, half_w),
                        (half_l, half_w), (half_l, -half_w)]
        corners = []
        for lx, ly in local_corners:
            gx = self.position_x + lx * cos_yaw - ly * sin_yaw
            gy = self.position_y + lx * sin_yaw + ly * cos_yaw
            corners.append((gx, gy))
        return corners
    
    def get_nearest_point_to_ego(self) -> Tuple[float, float]:
        """获取障碍物边界框上离自车边界框最近的点"""
        corners = self.get_corners()
        # 简化：取所有角点中离自车边界框最近的点
        min_dist = float('inf')
        nearest = corners[0]
        
        for cx, cy in corners:
            # 计算角点到自车边界框的距离
            dist = self._point_to_ego_box_distance(cx, cy)
            if dist < min_dist:
                min_dist = dist
                nearest = (cx, cy)
        
        return nearest
    
    def _point_to_ego_box_distance(self, px: float, py: float) -> float:
        """计算点到自车边界框的距离"""
        # 自车边界框范围
        x_min, x_max = -self.ego_rear, self.ego_front
        y_min, y_max = -self.ego_right, self.ego_left
        
        # 计算点到矩形的距离
        dx = max(x_min - px, 0, px - x_max)
        dy = max(y_min - py, 0, py - y_max)
        
        return np.sqrt(dx**2 + dy**2)
    
    @property
    def distance_to_ego(self) -> float:
        """障碍物边界框到自车边界框的最短距离"""
        corners = self.get_corners()
        min_dist = float('inf')
        
        for cx, cy in corners:
            dist = self._point_to_ego_box_distance(cx, cy)
            min_dist = min(min_dist, dist)
        
        # 如果障碍物中心在自车边界框内，返回负值（碰撞）
        if ((-self.ego_rear <= self.position_x <= self.ego_front) and 
            (-self.ego_right <= self.position_y <= self.ego_left)):
            return -min_dist
        
        return min_dist
    
    @property
    def direction(self) -> ObstacleDirection:
        """
        基于边界框位置判断障碍物相对于自车的方向
        
        判断逻辑（基于障碍物边界框与自车边界框的相对位置）：
        
        1. 正前方 (FRONT): 障碍物在自车前方，且在碰撞路径上
           - 障碍物后边缘 > 自车前边缘
           - 横向偏移 ≤ 自车半宽 + 0.5m
        
        2. 侧前方 (FRONT_LEFT/RIGHT): 障碍物整体在自车前方，但不在碰撞路径上
           - 障碍物后边缘 > 自车前边缘
           - 横向偏移 > 自车半宽 + 0.5m
        
        3. 侧方 (LEFT/RIGHT): 障碍物与自车有纵向重叠（并排）
           - 障碍物后边缘 ≤ 自车前边缘 且 障碍物前边缘 ≥ 自车后边缘
        
        4. 侧后方 (REAR_LEFT/RIGHT): 障碍物整体在自车后方但有横向偏移
           - 障碍物前边缘 < 自车后边缘
           - 横向偏移 > 自车半宽 + 0.5m
        
        5. 正后方 (REAR): 障碍物在自车正后方
        """
        corners = self.get_corners()
        
        # 障碍物边界框的 x 范围（纵向）
        obs_x_min = min(c[0] for c in corners)  # 障碍物后边缘
        obs_x_max = max(c[0] for c in corners)  # 障碍物前边缘
        
        # 障碍物边界框的 y 范围（横向）
        obs_y_min = min(c[1] for c in corners)
        obs_y_max = max(c[1] for c in corners)
        
        # 自车边界
        ego_front = self.ego_front
        ego_rear = -self.ego_rear
        ego_left = self.ego_left
        ego_right = -self.ego_right
        
        # 横向偏移判断阈值（自车半宽 + 余量）
        ego_half_width = (self.ego_left + self.ego_right) / 2
        lateral_margin = 0.2
        
        # 判断障碍物是否在碰撞路径上（横向重叠）
        def is_in_collision_path():
            # 障碍物的横向范围是否与自车碰撞路径重叠
            collision_left = ego_half_width + lateral_margin
            collision_right = -(ego_half_width + lateral_margin)
            return obs_y_min <= collision_left and obs_y_max >= collision_right
        
        # 判断左右侧
        def get_side():
            center_y = self.position_y
            return 'left' if center_y > 0 else 'right'
        
        # === 判断纵向位置关系 ===
        
        # 1. 障碍物整体在自车前方（后边缘 > 自车前边缘）
        if obs_x_min > ego_front:
            if is_in_collision_path():
                return ObstacleDirection.FRONT
            else:
                return ObstacleDirection.FRONT_LEFT if get_side() == 'left' else ObstacleDirection.FRONT_RIGHT
        
        # 2. 障碍物整体在自车后方（前边缘 < 自车后边缘）
        elif obs_x_max < ego_rear:
            if is_in_collision_path():
                return ObstacleDirection.REAR
            else:
                return ObstacleDirection.REAR_LEFT if get_side() == 'left' else ObstacleDirection.REAR_RIGHT
        
        # 3. 障碍物与自车有纵向重叠（侧方）
        else:
            return ObstacleDirection.LEFT if get_side() == 'left' else ObstacleDirection.RIGHT
    
    @property
    def front_distance(self) -> float:
        """前方距离：障碍物最近点到自车前保险杠的纵向距离"""
        corners = self.get_corners()
        # 取最靠近自车（x 最小）的角点
        min_x = min(c[0] for c in corners)
        # 到自车前保险杠的距离
        return max(0, min_x - self.ego_front)
    
    @property
    def side_distance(self) -> float:
        """侧向距离：障碍物最近点到自车侧边的横向距离"""
        corners = self.get_corners()
        
        if self.position_y > 0:  # 左侧
            min_y = min(c[1] for c in corners)
            return max(0, min_y - self.ego_left)
        else:  # 右侧
            max_y = max(c[1] for c in corners)
            return max(0, -self.ego_right - max_y)
    
    def is_in_collision_path(self, lateral_margin: float = 0.3) -> bool:
        """
        判断障碍物是否在自车的碰撞路径上（用于 TTC 计算）
        
        基于边界框的精确判断：
        1. 障碍物后边缘必须在自车前边缘之前（即障碍物整体在自车前方）
        2. 障碍物横向范围与碰撞走廊有重叠
        
        Args:
            lateral_margin: 横向安全余量（米），默认 0.3m
        
        Returns:
            True 如果障碍物在碰撞路径上
        """
        corners = self.get_corners()
        
        # 障碍物边界框
        obs_x_min = min(c[0] for c in corners)  # 障碍物后边缘（最近点）
        obs_y_min = min(c[1] for c in corners)  # 右边界
        obs_y_max = max(c[1] for c in corners)  # 左边界
        
        # 条件1: 障碍物后边缘必须在自车前边缘之前
        if obs_x_min <= self.ego_front:
            return False
        
        # 条件2: 横向重叠检查（障碍物横向范围与碰撞走廊重叠）
        ego_half_width = (self.ego_left + self.ego_right) / 2
        collision_left = ego_half_width + lateral_margin
        collision_right = -(ego_half_width + lateral_margin)
        
        # 判断是否有横向重叠
        return obs_y_min <= collision_left and obs_y_max >= collision_right


@dataclass
class ROIFrameStats:
    """单帧 ROI 统计"""
    total_count: int = 0
    static_count: int = 0
    moving_count: int = 0
    type_counts: Dict[str, int] = field(default_factory=dict)
    min_distance: float = float('inf')


@dataclass
class DirectionalDistances:
    """分方向距离统计"""
    front: List[float] = field(default_factory=list)        # 前方障碍物距离
    front_side: List[float] = field(default_factory=list)   # 侧前方障碍物距离
    side: List[float] = field(default_factory=list)         # 侧方障碍物距离


@dataclass
class FrameVizData:
    """用于可视化的帧数据"""
    frame_idx: int
    timestamp: float
    obstacles: List[ObstacleInfo]
    near_stat: ROIFrameStats
    mid_stat: ROIFrameStats
    far_stat: ROIFrameStats
    ego_speed: float


@dataclass
class TTCDataPoint:
    """TTC 数据点（用于可视化）"""
    timestamp: float
    ttc: float
    front_dist: float
    ego_speed: float
    relative_vel: float
    is_danger: bool = False  # TTC < 3s
    # 障碍物信息（用于危险事件可视化）
    obs_x: float = 0.0  # 障碍物 x 坐标（相对于自车）
    obs_y: float = 0.0  # 障碍物 y 坐标
    obs_vx: float = 0.0  # 障碍物速度 x 分量
    obs_length: float = 4.5  # 障碍物长度
    obs_width: float = 2.0  # 障碍物宽度
    obs_type: str = "Unknown"  # 障碍物类型
    obs_type_cn: str = "未知"  # 障碍物类型（中文）
    track_id: int = 0  # 障碍物跟踪 ID
    frame_idx: int = 0


class ROIObstaclesKPI(BaseKPI):
    """三级ROI障碍物统计KPI"""
    
    @property
    def name(self) -> str:
        return "ROI障碍物统计"
    
    @property
    def required_topics(self) -> List[str]:
        return [
            Topics.FUNCTION_MANAGER,
            Topics.LOCALIZATION,
            Topics.PERCEPTION_OBSTACLES,
            "/vehicle/chassis_domain_report"
        ]
    
    def __init__(self, config: Optional[Dict] = None):
        super().__init__(config)
        # 自车尺寸
        ego_config = self.config.get('ego_vehicle', {})
        self.ego_front = ego_config.get('front_length', 3.79)
        self.ego_rear = ego_config.get('rear_length', 0.883)
        self.ego_left = ego_config.get('left_width', 0.945)
        self.ego_right = ego_config.get('right_width', 0.945)
        
        # ROI配置
        roi_config = self.config.get('kpi', {}).get('roi', {})
        self.near_radius = roi_config.get('near_radius', 10.0)
        self.mid_roi_config = roi_config.get('mid_roi', {
            'front': 25.0, 'rear': 10.0, 'left': 5.0, 'right': 5.0
        })
        self.far_roi_config = roi_config.get('far_roi', {
            'front': 60.0, 'rear': 25.0, 'left': 7.0, 'right': 7.0
        })
        
        # 障碍物颜色映射 (使用英文缩写)
        self.type_colors = {
            "Unk": '#808080', "Ped": '#FF4444', "Veh": '#9966FF',
            "Cyc": '#00CCCC', "Ani": '#8B4513',
            "Sta": '#555555', "Con": '#FF8C00'
        }
        
        # 可视化配置
        viz_config = roi_config.get('visualization', {})
        self.viz_enabled = viz_config.get('enabled', True)
        self.viz_sample_count = viz_config.get('sample_count', 30)
        self.viz_output_dir = viz_config.get('output_dir', "./output/roi_viz")
    
    def _create_roi_regions(self) -> Tuple[float, Rectangle, Rectangle]:
        """创建 ROI 区域配置（避免重复代码）"""
        near_radius = self.near_radius
        mid_roi = create_ego_roi(
            front=self.ego_front + self.mid_roi_config['front'],
            rear=self.ego_rear + self.mid_roi_config['rear'],
            left=self.ego_left + self.mid_roi_config['left'],
            right=self.ego_right + self.mid_roi_config['right']
        )
        far_roi = create_ego_roi(
            front=self.ego_front + self.far_roi_config['front'],
            rear=self.ego_rear + self.far_roi_config['rear'],
            left=self.ego_left + self.far_roi_config['left'],
            right=self.ego_right + self.far_roi_config['right']
        )
        return near_radius, mid_roi, far_roi
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算ROI障碍物统计KPI - 通过流式模式复用逻辑"""
        return self._compute_via_streaming(synced_frames, **kwargs)
    
    def _extract_obstacles(self, obs_msg: Any) -> List[ObstacleInfo]:
        """从消息中提取障碍物列表"""
        obstacles = []
        if obs_msg is None:
            return obstacles
        
        obs_list = MessageAccessor.get_field(obs_msg, "obstacles", [])
        if not obs_list:
            return obstacles
        
        for obs in obs_list:
            try:
                pos_x = MessageAccessor.get_field(obs, "position.x", None)
                pos_y = MessageAccessor.get_field(obs, "position.y", None)
                if pos_x is None or pos_y is None:
                    continue
                
                track_id = MessageAccessor.get_field(obs, "track_id", 0)
                length = max(0.5, MessageAccessor.get_field(obs, "length", 0.5) or 0.5)
                width = max(0.5, MessageAccessor.get_field(obs, "width", 0.5) or 0.5)
                heading_to_ego = MessageAccessor.get_field(obs, "heading_to_ego", 0.0)
                motion_status = MessageAccessor.get_field(obs, "motion_status", 0)
                vel_x = MessageAccessor.get_field(obs, "velocity.x", 0.0) or 0.0
                vel_y = MessageAccessor.get_field(obs, "velocity.y", 0.0) or 0.0
                
                type_history = MessageAccessor.get_field(obs, "type_history", [])
                obs_type = 0
                if type_history and len(type_history) > 0:
                    obs_type = MessageAccessor.get_field(type_history[0], "type", 0)
                
                obstacles.append(ObstacleInfo(
                    track_id=track_id,
                    position_x=pos_x, position_y=pos_y,
                    length=length, width=width,
                    heading_to_ego=heading_to_ego,
                    obstacle_type=obs_type,
                    motion_status=motion_status,
                    velocity_x=vel_x, velocity_y=vel_y,
                    ego_front=self.ego_front,
                    ego_rear=self.ego_rear,
                    ego_left=self.ego_left,
                    ego_right=self.ego_right
                ))
            except Exception:
                continue
        
        return obstacles
    
    def _compute_roi_stats(self, obstacles: List[ObstacleInfo]) -> ROIFrameStats:
        """计算障碍物列表的统计信息"""
        stat = ROIFrameStats()
        type_counts = {}
        
        for obs in obstacles:
            stat.total_count += 1
            if obs.is_static:
                stat.static_count += 1
            else:
                stat.moving_count += 1
            
            type_name = obs.type_name
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
            
            dist = obs.distance_to_ego
            if dist < stat.min_distance:
                stat.min_distance = dist
        
        stat.type_counts = type_counts
        return stat
    
    def _compute_roi_stats_rect(self, obstacles: List[ObstacleInfo], roi: Rectangle) -> ROIFrameStats:
        """计算矩形 ROI 内的障碍物统计"""
        roi_obstacles = []
        for obs in obstacles:
            obs_box = BoundingBox(
                center_x=obs.position_x, center_y=obs.position_y,
                length=obs.length, width=obs.width,
                yaw=obs.heading_to_ego
            )
            if check_box_in_roi(obs_box, roi):
                roi_obstacles.append(obs)
        
        return self._compute_roi_stats(roi_obstacles)
    
    def _add_near_roi_results(self, stats_list: List[ROIFrameStats], frame_count: int):
        """添加近距离 ROI 结果"""
        if not stats_list:
            return
        
        total_arr = np.array([s.total_count for s in stats_list])
        static_arr = np.array([s.static_count for s in stats_list])
        moving_arr = np.array([s.moving_count for s in stats_list])
        
        self.add_result(KPIResult(
            name="近距离障碍物数(10m圆)",
            value=round(float(np.mean(total_arr)), 2),
            unit="个/帧",
            description=f"半径{self.near_radius}m圆形区域内的平均障碍物数（紧急反应区）",
            details={
                'mean': round(float(np.mean(total_arr)), 2),
                'std': round(float(np.std(total_arr)), 2),
                'max': int(np.max(total_arr)),
                'p95': round(float(np.percentile(total_arr, 95)), 2),
                'p99': round(float(np.percentile(total_arr, 99)), 2),
                'zero_rate': round(float(np.sum(total_arr == 0) / len(total_arr) * 100), 1),
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="近距离静态障碍物数",
            value=round(float(np.mean(static_arr)), 2),
            unit="个/帧",
            description=f"近距离{self.near_radius}m圆形区域内静态障碍物",
            details={'max': int(np.max(static_arr)), 'p95': round(float(np.percentile(static_arr, 95)), 2)}
        ))
        
        self.add_result(KPIResult(
            name="近距离动态障碍物数",
            value=round(float(np.mean(moving_arr)), 2),
            unit="个/帧",
            description=f"近距离{self.near_radius}m圆形区域内动态障碍物",
            details={'max': int(np.max(moving_arr)), 'p95': round(float(np.percentile(moving_arr, 95)), 2)}
        ))
    
    def _add_mid_roi_results(self, stats_list: List[ROIFrameStats], frame_count: int):
        """添加中距离 ROI 结果"""
        if not stats_list:
            return
        
        total_arr = np.array([s.total_count for s in stats_list])
        static_arr = np.array([s.static_count for s in stats_list])
        moving_arr = np.array([s.moving_count for s in stats_list])
        cfg = self.mid_roi_config
        
        self.add_result(KPIResult(
            name="中距离障碍物数",
            value=round(float(np.mean(total_arr)), 2),
            unit="个/帧",
            description=f"前{cfg['front']}m后{cfg['rear']}m左右{cfg['left']}m矩形区域（主动避障区）",
            details={
                'mean': round(float(np.mean(total_arr)), 2),
                'std': round(float(np.std(total_arr)), 2),
                'max': int(np.max(total_arr)),
                'p95': round(float(np.percentile(total_arr, 95)), 2),
                'p99': round(float(np.percentile(total_arr, 99)), 2),
                'config': cfg,
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="中距离静态障碍物数",
            value=round(float(np.mean(static_arr)), 2),
            unit="个/帧",
            description="中距离矩形区域内静态障碍物",
            details={'max': int(np.max(static_arr)), 'p95': round(float(np.percentile(static_arr, 95)), 2)}
        ))
        
        self.add_result(KPIResult(
            name="中距离动态障碍物数",
            value=round(float(np.mean(moving_arr)), 2),
            unit="个/帧",
            description="中距离矩形区域内动态障碍物",
            details={'max': int(np.max(moving_arr)), 'p95': round(float(np.percentile(moving_arr, 95)), 2)}
        ))
    
    def _add_far_roi_results(self, stats_list: List[ROIFrameStats], frame_count: int):
        """添加远距离 ROI 结果"""
        if not stats_list:
            return
        
        total_arr = np.array([s.total_count for s in stats_list])
        static_arr = np.array([s.static_count for s in stats_list])
        moving_arr = np.array([s.moving_count for s in stats_list])
        cfg = self.far_roi_config
        
        self.add_result(KPIResult(
            name="远距离障碍物数",
            value=round(float(np.mean(total_arr)), 2),
            unit="个/帧",
            description=f"前{cfg['front']}m后{cfg['rear']}m左右{cfg['left']}m矩形区域（预警感知区）",
            details={
                'mean': round(float(np.mean(total_arr)), 2),
                'std': round(float(np.std(total_arr)), 2),
                'max': int(np.max(total_arr)),
                'p95': round(float(np.percentile(total_arr, 95)), 2),
                'p99': round(float(np.percentile(total_arr, 99)), 2),
                'config': cfg,
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="远距离静态障碍物数",
            value=round(float(np.mean(static_arr)), 2),
            unit="个/帧",
            description="远距离矩形区域内静态障碍物",
            details={'max': int(np.max(static_arr)), 'p95': round(float(np.percentile(static_arr, 95)), 2)}
        ))
        
        self.add_result(KPIResult(
            name="远距离动态障碍物数",
            value=round(float(np.mean(moving_arr)), 2),
            unit="个/帧",
            description="远距离矩形区域内动态障碍物",
            details={'max': int(np.max(moving_arr)), 'p95': round(float(np.percentile(moving_arr, 95)), 2)}
        ))
    
    def _add_directional_distance_results(self, dir_distances: DirectionalDistances,
                                           all_min_distances: List[float]):
        """添加分方向距离统计结果"""
        
        # 前方障碍物距离
        if dir_distances.front:
            arr = np.array(dir_distances.front)
            self.add_result(KPIResult(
                name="前方障碍物距离P5",
                value=round(float(np.percentile(arr, 5)), 2),
                unit="m",
                description="前方障碍物到自车前保险杠距离的5分位数（box-to-box）",
                details={
                    'min': round(float(np.min(arr)), 2),
                    'p5': round(float(np.percentile(arr, 5)), 2),
                    'p10': round(float(np.percentile(arr, 10)), 2),
                    'p25': round(float(np.percentile(arr, 25)), 2),
                    'median': round(float(np.median(arr)), 2),
                    'sample_count': len(arr),
                    'rate_lt_5m': round(float(np.sum(arr < 5) / len(arr) * 100), 1),
                    'rate_lt_10m': round(float(np.sum(arr < 10) / len(arr) * 100), 1)
                }
            ))
        
        # 侧前方障碍物距离
        if dir_distances.front_side:
            arr = np.array(dir_distances.front_side)
            self.add_result(KPIResult(
                name="侧前方障碍物距离P5",
                value=round(float(np.percentile(arr, 5)), 2),
                unit="m",
                description="侧前方障碍物到自车边界框距离的5分位数（box-to-box）",
                details={
                    'min': round(float(np.min(arr)), 2),
                    'p5': round(float(np.percentile(arr, 5)), 2),
                    'p10': round(float(np.percentile(arr, 10)), 2),
                    'median': round(float(np.median(arr)), 2),
                    'sample_count': len(arr),
                    'rate_lt_3m': round(float(np.sum(arr < 3) / len(arr) * 100), 1)
                }
            ))
        
        # 侧方障碍物距离
        if dir_distances.side:
            arr = np.array(dir_distances.side)
            self.add_result(KPIResult(
                name="侧方障碍物距离P5",
                value=round(float(np.percentile(arr, 5)), 2),
                unit="m",
                description="侧方障碍物到自车侧边距离的5分位数（box-to-box）",
                details={
                    'min': round(float(np.min(arr)), 2),
                    'p5': round(float(np.percentile(arr, 5)), 2),
                    'p10': round(float(np.percentile(arr, 10)), 2),
                    'median': round(float(np.median(arr)), 2),
                    'sample_count': len(arr),
                    'rate_lt_1m': round(float(np.sum(arr < 1) / len(arr) * 100), 1),
                    'rate_lt_2m': round(float(np.sum(arr < 2) / len(arr) * 100), 1)
                }
            ))
            
        # 整体最近距离
        if all_min_distances:
            arr = np.array(all_min_distances)
            self.add_result(KPIResult(
                name="最近障碍物距离P5",
                value=round(float(np.percentile(arr, 5)), 2),
                unit="m",
                description="障碍物边界框到自车边界框的最短距离5分位数（box-to-box）",
                details={
                    'min': round(float(np.min(arr)), 2),
                    'p5': round(float(np.percentile(arr, 5)), 2),
                    'p10': round(float(np.percentile(arr, 10)), 2),
                    'p25': round(float(np.percentile(arr, 25)), 2),
                    'median': round(float(np.median(arr)), 2),
                    'sample_count': len(arr)
                }
            ))
    
    def _add_ttc_results(self, all_ttc: List[float], 
                         ttc_danger_events: List[AnomalyRecord] = None):
        """添加 TTC 统计结果"""
        if not all_ttc:
            return
        
        arr = np.array(all_ttc)
        
        result = KPIResult(
            name="TTC P5",
            value=round(float(np.percentile(arr, 5)), 2),
            unit="s",
            description="前方 TTC 的5分位数（最危险5%情况）",
            details={
                'min': round(float(np.min(arr)), 2),
                'p5': round(float(np.percentile(arr, 5)), 2),
                'p10': round(float(np.percentile(arr, 10)), 2),
                'median': round(float(np.median(arr)), 2),
                'sample_count': len(arr),
                'rate_lt_3s': round(float(np.sum(arr < 3) / len(arr) * 100), 1),
                'rate_lt_5s': round(float(np.sum(arr < 5) / len(arr) * 100), 1),
                'danger_event_count': len(ttc_danger_events) if ttc_danger_events else 0
            }
        )
        
        # 添加 TTC 危险事件（限制数量，避免过多）
        if ttc_danger_events:
            # 按 TTC 值排序，取最危险的前 30 个
            sorted_events = sorted(ttc_danger_events, key=lambda x: x.value)[:30]
            for event in sorted_events:
                result.anomalies.append(event)
        
        self.add_result(result)
    
    def _add_danger_results(self, danger_count: int, warning_count: int, frame_count: int,
                             danger_events: List[AnomalyRecord] = None):
        """添加危险率统计"""
        if frame_count == 0:
            return
        
        danger_rate = danger_count / frame_count * 100
        warning_rate = warning_count / frame_count * 100
        safe_rate = 100 - danger_rate - warning_rate
        
        result = KPIResult(
            name="危险帧率(<1.5m)",
            value=round(danger_rate, 2),
            unit="%",
            description="障碍物边界框距自车<1.5m的帧占比（box-to-box）",
            details={
                'danger_count': danger_count,
                'danger_rate': round(danger_rate, 2),
                'warning_count': warning_count,  # 1.5-3m
                'warning_rate': round(warning_rate, 2),
                'safe_count': frame_count - danger_count - warning_count,
                'safe_rate': round(safe_rate, 2),
                'frame_count': frame_count,
                'danger_event_count': len(danger_events) if danger_events else 0
            }
        )
        
        # 添加危险距离事件（限制数量，取最危险的前 30 个）
        if danger_events:
            sorted_events = sorted(danger_events, key=lambda x: x.value)[:30]
            for event in sorted_events:
                result.anomalies.append(event)
        
        self.add_result(result)
    
    def _generate_visualizations(self, viz_cache: List[FrameVizData],
                                  near_radius: float, mid_roi: Rectangle, 
                                  far_roi: Rectangle):
        """生成可视化图片"""
        os.makedirs(self.viz_output_dir, exist_ok=True)
        
        sample_count = min(self.viz_sample_count, len(viz_cache))
        sampled_frames = random.sample(viz_cache, sample_count)
        sampled_frames.sort(key=lambda x: x.frame_idx)
        
        print(f"    [可视化] 随机抽样 {sample_count}/{len(viz_cache)} 帧")
        
        for frame_data in sampled_frames:
            self._visualize_frame(frame_data, near_radius, mid_roi, far_roi)
        
        print(f"    [可视化] 已生成 {sample_count} 张图片到 {self.viz_output_dir}")
    
    def _generate_danger_distance_scenes(self, viz_cache: List[FrameVizData],
                                          near_radius: float, mid_roi: Rectangle,
                                          far_roi: Rectangle, max_scenes: int = 20):
        """生成距离最近（危险）的帧可视化"""
        if not HAS_MATPLOTLIB or not viz_cache:
            return
        
        # 计算每帧的最近障碍物距离
        frames_with_dist = []
        for frame_data in viz_cache:
            if frame_data.obstacles:
                min_dist = min(obs.distance_to_ego for obs in frame_data.obstacles)
                frames_with_dist.append((min_dist, frame_data))
        
        if not frames_with_dist:
            return
        
        # 按距离排序，取距离最近的帧
        frames_with_dist.sort(key=lambda x: x[0])
        danger_frames = [f for f in frames_with_dist if f[0] < 3.0][:max_scenes]  # 距离<3m的帧
        
        if not danger_frames:
            return
        
        # 创建输出目录
        danger_dir = os.path.join(self.viz_output_dir, 'danger_distance_scenes')
        os.makedirs(danger_dir, exist_ok=True)
        
        for idx, (min_dist, frame_data) in enumerate(danger_frames):
            self._visualize_danger_frame(frame_data, near_radius, mid_roi, far_roi,
                                         min_dist, idx + 1, danger_dir)
        
        print(f"    [危险距离可视化] 已生成 {len(danger_frames)} 个近距离场景图到 {danger_dir}")
    
    def _visualize_danger_frame(self, frame_data: FrameVizData, near_radius: float,
                                 mid_roi: Rectangle, far_roi: Rectangle,
                                 min_dist: float, scene_idx: int, output_dir: str):
        """可视化单个危险距离帧"""
        if not HAS_MATPLOTLIB:
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(14, 12))
        
        # 使用配置字典而不是 Rectangle 对象
        mid_cfg = self.mid_roi_config
        far_cfg = self.far_roi_config
        
        x_min = -(self.ego_rear + far_cfg['rear'] + 5)
        x_max = self.ego_front + far_cfg['front'] + 5
        y_max = max(self.ego_left + far_cfg['left'], near_radius) + 3
        
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(-y_max, y_max)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        ax.set_xlabel('X - Forward (m)', fontsize=12)
        ax.set_ylabel('Y - Left (m)', fontsize=12)
        
        # 绘制 ROI 区域
        near_circle = mpatches.Circle((0, 0), near_radius, fill=False, 
                                       edgecolor='#F44336', linewidth=2, 
                                       linestyle='--', label=f'Near ROI (r={near_radius}m)')
        ax.add_patch(near_circle)
        
        mid_rect = mpatches.Rectangle(
            (-(self.ego_rear + mid_cfg['rear']), -(self.ego_left + mid_cfg['left'])),
            self.ego_front + mid_cfg['front'] + self.ego_rear + mid_cfg['rear'],
            (self.ego_left + mid_cfg['left']) * 2,
            fill=False, edgecolor='#FF9800', linewidth=2, linestyle='--',
            label=f'Mid ROI')
        ax.add_patch(mid_rect)
        
        far_rect = mpatches.Rectangle(
            (-(self.ego_rear + far_cfg['rear']), -(self.ego_left + far_cfg['left'])),
            self.ego_front + far_cfg['front'] + self.ego_rear + far_cfg['rear'],
            (self.ego_left + far_cfg['left']) * 2,
            fill=False, edgecolor='#4CAF50', linewidth=2, linestyle='--',
            label=f'Far ROI')
        ax.add_patch(far_rect)
        
        # 绘制自车
        ego_rect = mpatches.Rectangle(
            (-self.ego_rear, -self.ego_left),
            self.ego_front + self.ego_rear,
            self.ego_left + self.ego_right,
            fill=True, facecolor='#1E88E5', edgecolor='#0D47A1',
            linewidth=2, label='Ego Vehicle', zorder=10)
        ax.add_patch(ego_rect)
        
        # 绘制障碍物，高亮最近的障碍物
        nearest_obs = None
        nearest_dist = float('inf')
        
        for obs in frame_data.obstacles:
            if obs.distance_to_ego < nearest_dist:
                nearest_dist = obs.distance_to_ego
                nearest_obs = obs
        
        for obs in frame_data.obstacles:
            # 根据距离选择颜色
            is_nearest = (obs == nearest_obs)
            if is_nearest:
                color = '#D32F2F'  # 深红 - 最近的障碍物
                edge_color = '#B71C1C'
                linewidth = 3
                zorder = 8
            elif obs.distance_to_ego < 1.5:
                color = '#F44336'  # 红色 - 危险
                edge_color = '#C62828'
                linewidth = 2
                zorder = 7
            elif obs.distance_to_ego < 3.0:
                color = '#FF9800'  # 橙色 - 警告
                edge_color = '#E65100'
                linewidth = 2
                zorder = 6
            else:
                color = '#4CAF50' if obs.is_static else '#2196F3'
                edge_color = '#2E7D32' if obs.is_static else '#1565C0'
                linewidth = 1.5
                zorder = 5
            
            obs_rect = mpatches.Rectangle(
                (obs.position_x - obs.length/2, obs.position_y - obs.width/2),
                obs.length, obs.width,
                fill=True, facecolor=color, edgecolor=edge_color,
                linewidth=linewidth, alpha=0.7, zorder=zorder)
            ax.add_patch(obs_rect)
            
            # 标注最近障碍物的距离
            if is_nearest:
                ax.annotate(
                    f'{obs.distance_to_ego:.2f}m',
                    xy=(obs.position_x, obs.position_y),
                    xytext=(obs.position_x + 3, obs.position_y + 2),
                    fontsize=12, fontweight='bold', color='#D32F2F',
                    arrowprops=dict(arrowstyle='->', color='#D32F2F', lw=2),
                    bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='#D32F2F', alpha=0.9),
                    zorder=15
                )
        
        # 危险等级
        if min_dist < 1.0:
            danger_level = "CRITICAL"
            title_color = '#D32F2F'
        elif min_dist < 1.5:
            danger_level = "DANGER"
            title_color = '#F44336'
        elif min_dist < 2.0:
            danger_level = "WARNING"
            title_color = '#FF9800'
        else:
            danger_level = "CAUTION"
            title_color = '#FFC107'
        
        # 标题
        ax.set_title(
            f'Danger Distance Scene #{scene_idx:02d} - Min Distance: {min_dist:.2f}m ({danger_level})\n'
            f'Frame: {frame_data.frame_idx} | Time: {frame_data.timestamp:.2f}s | Speed: {frame_data.ego_speed*3.6:.1f}km/h',
            fontsize=14, fontweight='bold', color=title_color
        )
        
        # 图例
        ax.legend(loc='upper right', fontsize=9)
        
        # 统计信息
        near_s = frame_data.near_stat
        info_text = (
            f"Near ROI: {near_s.total_count} obs ({near_s.static_count} static, {near_s.moving_count} moving)\n"
            f"Nearest Distance: {min_dist:.2f}m"
        )
        ax.text(0.02, 0.98, info_text, transform=ax.transAxes,
                fontsize=10, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
        
        plt.tight_layout()
        output_path = os.path.join(output_dir, f'danger_scene_{scene_idx:02d}_dist{min_dist:.2f}m.png')
        plt.savefig(output_path, dpi=100, bbox_inches='tight')
        plt.close(fig)
    
    def _visualize_frame(self, frame_data: FrameVizData, near_radius: float,
                          mid_roi: Rectangle, far_roi: Rectangle):
        """可视化单帧"""
        if not HAS_MATPLOTLIB:
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(14, 12))
        
        far_cfg = self.far_roi_config
        x_min = -(self.ego_rear + far_cfg['rear'] + 5)
        x_max = self.ego_front + far_cfg['front'] + 5
        y_max = max(self.ego_left + far_cfg['left'], near_radius) + 3
        
        ax.set_xlim(x_min, x_max)
        ax.set_ylim(-y_max, y_max)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3)
        ax.set_xlabel('X - Forward (m)', fontsize=12)
        ax.set_ylabel('Y - Left (m)', fontsize=12)
        
        near_s = frame_data.near_stat
        mid_s = frame_data.mid_stat
        far_s = frame_data.far_stat
        
        title = (f'ROI Obstacles - Frame {frame_data.frame_idx} '
                 f'(t={frame_data.timestamp:.2f}s, v={frame_data.ego_speed*3.6:.1f}km/h)\n'
                 f'Near: {near_s.total_count} (S{near_s.static_count}/M{near_s.moving_count}) | '
                 f'Mid: {mid_s.total_count} | Far: {far_s.total_count}')
        ax.set_title(title, fontsize=12)
        
        # 画自车
        ego_corners = [
            (-self.ego_rear, -self.ego_right),
            (-self.ego_rear, self.ego_left),
            (self.ego_front, self.ego_left),
            (self.ego_front, -self.ego_right),
        ]
        ego_polygon = mpatches.Polygon(ego_corners, closed=True, 
                                        facecolor='#2196F3', edgecolor='#1565C0',
                                        alpha=0.8, linewidth=2)
        ax.add_patch(ego_polygon)
        ax.arrow(0, 0, 2.5, 0, head_width=0.4, head_length=0.3, 
                 fc='#FFC107', ec='#FFC107', zorder=10)
        ax.plot(0, 0, 'ko', markersize=6, zorder=10)
        
        # 画 ROI 区域
        far_front = self.ego_front + far_cfg['front']
        far_rear = self.ego_rear + far_cfg['rear']
        far_left = self.ego_left + far_cfg['left']
        far_right = self.ego_right + far_cfg['right']
        far_corners = [(-far_rear, -far_right), (-far_rear, far_left),
                       (far_front, far_left), (far_front, -far_right)]
        ax.add_patch(mpatches.Polygon(far_corners, closed=True,
                                       facecolor='#E3F2FD', edgecolor='#42A5F5',
                                       alpha=0.3, linestyle='--', linewidth=1.5))
        
        mid_cfg = self.mid_roi_config
        mid_front = self.ego_front + mid_cfg['front']
        mid_rear = self.ego_rear + mid_cfg['rear']
        mid_left = self.ego_left + mid_cfg['left']
        mid_right = self.ego_right + mid_cfg['right']
        mid_corners = [(-mid_rear, -mid_right), (-mid_rear, mid_left),
                       (mid_front, mid_left), (mid_front, -mid_right)]
        ax.add_patch(mpatches.Polygon(mid_corners, closed=True,
                                       facecolor='#FFF3E0', edgecolor='#FF9800',
                                       alpha=0.4, linestyle='-', linewidth=1.5))
        
        ax.add_patch(mpatches.Circle((0, 0), near_radius, 
                                      facecolor='#FFEBEE', edgecolor='#F44336',
                                      alpha=0.4, linestyle='-', linewidth=2))
        
        # 画障碍物
        for obs in frame_data.obstacles:
            in_near = obs.center_distance <= near_radius
            in_mid = self._check_in_rect(obs, mid_roi) and not in_near
            in_far = self._check_in_rect(obs, far_roi) and not in_near and not in_mid
            
            if in_near:
                edge_color = '#D32F2F'
                alpha = 0.85
            elif in_mid:
                edge_color = '#F57C00'
                alpha = 0.7
            elif in_far:
                edge_color = '#1976D2'
                alpha = 0.6
            else:
                edge_color = '#757575'
                alpha = 0.4
            
            fill_color = self.type_colors.get(obs.type_name, '#808080')
            corners = obs.get_corners()
            
            ax.add_patch(mpatches.Polygon(corners, closed=True,
                                          facecolor=fill_color, edgecolor=edge_color,
                                          alpha=alpha, linewidth=1.5))
        
            # 标注：类型、动静态、box-to-box 距离
            status_str = "S" if obs.is_static else "M"  # S=Static, M=Moving
            dist_str = f"{obs.distance_to_ego:.1f}m"
            ax.annotate(f'{obs.type_name}{status_str}\n{dist_str}', 
                        (obs.position_x, obs.position_y),
                        fontsize=7, ha='center', va='center',
                        color='white' if in_near else 'black',
                        fontweight='bold')
        
        # 图例
        legend_elements = [
            mpatches.Patch(facecolor='#2196F3', edgecolor='#1565C0', alpha=0.8, label='Ego'),
            mpatches.Circle((0, 0), 0.1, facecolor='#FFEBEE', edgecolor='#F44336', alpha=0.4, label=f'Near({near_radius}m)'),
            mpatches.Patch(facecolor='#FFF3E0', edgecolor='#FF9800', alpha=0.4, label='Mid(rect)'),
            mpatches.Patch(facecolor='#E3F2FD', edgecolor='#42A5F5', alpha=0.3, label='Far(rect)'),
            Line2D([0], [0], marker='s', color='w', markerfacecolor='#9966FF', markersize=10, label='Veh'),
            Line2D([0], [0], marker='s', color='w', markerfacecolor='#FF4444', markersize=10, label='Ped'),
            Line2D([0], [0], marker='s', color='w', markerfacecolor='#555555', markersize=10, label='Sta'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=9)
        
        output_path = os.path.join(self.viz_output_dir, f'roi_frame_{frame_data.frame_idx:05d}.png')
        plt.savefig(output_path, dpi=100, bbox_inches='tight', facecolor='white')
        plt.close(fig)
    
    def _check_in_rect(self, obs: ObstacleInfo, roi: Rectangle) -> bool:
        """检查障碍物是否在矩形 ROI 内"""
        obs_box = BoundingBox(
            center_x=obs.position_x, center_y=obs.position_y,
            length=obs.length, width=obs.width,
            yaw=obs.heading_to_ego
        )
        return check_box_in_roi(obs_box, roi)
    
    def _generate_ttc_visualization(self, ttc_data_points: List[TTCDataPoint],
                                      danger_points_with_obs: List[TTCDataPoint] = None):
        """
        生成 TTC 时序图可视化
        
        包含：
        1. TTC 随时间变化的曲线图
        2. 前方障碍物距离随时间变化
        3. 危险事件标记（TTC < 3s）
        
        Args:
            ttc_data_points: 所有 TTC 数据点（用于时序图和分布图）
            danger_points_with_obs: 带有完整障碍物信息的危险点（用于场景图，可选）
        """
        if not HAS_MATPLOTLIB or not ttc_data_points:
            return
        
        os.makedirs(self.viz_output_dir, exist_ok=True)
            
        # 提取数据
        timestamps = np.array([p.timestamp for p in ttc_data_points])
        ttc_values = np.array([p.ttc for p in ttc_data_points])
        front_dists = np.array([p.front_dist for p in ttc_data_points])
        ego_speeds = np.array([p.ego_speed * 3.6 for p in ttc_data_points])  # m/s -> km/h
        is_danger = np.array([p.is_danger for p in ttc_data_points])
        
        # 转换时间为相对时间（从0开始的秒数）
        t_start = timestamps.min()
        rel_times = timestamps - t_start
        
        # 创建图表
        fig, axes = plt.subplots(3, 1, figsize=(16, 12), sharex=True)
        fig.suptitle('TTC (Time To Collision) Analysis', fontsize=14, fontweight='bold')
        
        # === 子图1: TTC 时序图 ===
        ax1 = axes[0]
        # 正常点
        normal_mask = ~is_danger
        ax1.scatter(rel_times[normal_mask], ttc_values[normal_mask], 
                   c='#2196F3', s=8, alpha=0.5, label='TTC (normal)')
        # 危险点
        if np.any(is_danger):
            ax1.scatter(rel_times[is_danger], ttc_values[is_danger], 
                       c='#F44336', s=30, alpha=0.8, marker='x', linewidths=2, label='TTC < 3s (danger)')
        
        # 危险阈值线
        ax1.axhline(y=3.0, color='#F44336', linestyle='--', linewidth=1.5, alpha=0.7, label='Danger threshold (3s)')
        ax1.axhline(y=5.0, color='#FF9800', linestyle='--', linewidth=1, alpha=0.5, label='Warning threshold (5s)')
        
        ax1.set_ylabel('TTC (s)', fontsize=11)
        ax1.set_ylim(0, min(30, ttc_values.max() * 1.1))
        ax1.legend(loc='upper right', fontsize=9)
        ax1.grid(True, alpha=0.3)
        ax1.set_title('Time To Collision', fontsize=11)
        
        # === 子图2: 前方障碍物距离 ===
        ax2 = axes[1]
        ax2.scatter(rel_times[normal_mask], front_dists[normal_mask], 
                   c='#4CAF50', s=8, alpha=0.5, label='Front distance (normal)')
        if np.any(is_danger):
            ax2.scatter(rel_times[is_danger], front_dists[is_danger], 
                       c='#F44336', s=30, alpha=0.8, marker='x', linewidths=2, label='During TTC danger')
        
        ax2.axhline(y=3.0, color='#F44336', linestyle='--', linewidth=1, alpha=0.5, label='3m')
        ax2.axhline(y=5.0, color='#FF9800', linestyle='--', linewidth=1, alpha=0.5, label='5m')
        
        ax2.set_ylabel('Front Distance (m)', fontsize=11)
        ax2.legend(loc='upper right', fontsize=9)
        ax2.grid(True, alpha=0.3)
        ax2.set_title('Front Obstacle Distance', fontsize=11)
        
        # === 子图3: 自车速度 ===
        ax3 = axes[2]
        ax3.plot(rel_times, ego_speeds, c='#9C27B0', linewidth=0.8, alpha=0.7, label='Ego speed')
        if np.any(is_danger):
            ax3.scatter(rel_times[is_danger], ego_speeds[is_danger], 
                       c='#F44336', s=30, alpha=0.8, marker='x', linewidths=2, label='During TTC danger')
        
        ax3.set_ylabel('Ego Speed (km/h)', fontsize=11)
        ax3.set_xlabel('Time (s)', fontsize=11)
        ax3.legend(loc='upper right', fontsize=9)
        ax3.grid(True, alpha=0.3)
        ax3.set_title('Ego Vehicle Speed', fontsize=11)
        
        plt.tight_layout()
        
        # 保存图片
        output_path = os.path.join(self.viz_output_dir, 'ttc_timeline.png')
        plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        # 生成 TTC 分布直方图
        self._generate_ttc_histogram(ttc_values, is_danger)
        
        # 生成 TTC 危险事件场景图（每个危险事件的自车-障碍物相对位置）
        # 优先使用带有完整障碍物信息的 danger_points_with_obs
        if danger_points_with_obs:
            self._generate_ttc_danger_scenes(danger_points_with_obs)
        else:
            danger_points = [p for p in ttc_data_points if p.is_danger]
            if danger_points:
                self._generate_ttc_danger_scenes(danger_points)
        
        print(f"    [TTC可视化] 已生成 TTC 时序图: {output_path}")
    
    def _generate_ttc_histogram(self, ttc_values: np.ndarray, is_danger: np.ndarray):
        """生成 TTC 分布直方图"""
        if not HAS_MATPLOTLIB:
            return
        
        fig, axes = plt.subplots(1, 2, figsize=(14, 5))
        fig.suptitle('TTC Distribution Analysis', fontsize=14, fontweight='bold')
        
        # === 左图: TTC 直方图 ===
        ax1 = axes[0]
        bins = np.arange(0, min(31, ttc_values.max() + 1), 1)
        
        # 区分危险和正常
        normal_ttc = ttc_values[~is_danger]
        danger_ttc = ttc_values[is_danger]
        
        ax1.hist(normal_ttc, bins=bins, color='#2196F3', alpha=0.7, label=f'Normal (n={len(normal_ttc)})', edgecolor='white')
        if len(danger_ttc) > 0:
            ax1.hist(danger_ttc, bins=bins, color='#F44336', alpha=0.7, label=f'Danger <3s (n={len(danger_ttc)})', edgecolor='white')
        
        ax1.axvline(x=3.0, color='#F44336', linestyle='--', linewidth=2, label='Danger threshold (3s)')
        ax1.axvline(x=5.0, color='#FF9800', linestyle='--', linewidth=1.5, label='Warning threshold (5s)')
        
        # 添加统计信息
        p5 = np.percentile(ttc_values, 5)
        p10 = np.percentile(ttc_values, 10)
        median = np.median(ttc_values)
        
        ax1.axvline(x=p5, color='#E91E63', linestyle=':', linewidth=1.5, label=f'P5={p5:.2f}s')
        ax1.axvline(x=p10, color='#9C27B0', linestyle=':', linewidth=1.5, label=f'P10={p10:.2f}s')
        
        ax1.set_xlabel('TTC (s)', fontsize=11)
        ax1.set_ylabel('Count', fontsize=11)
        ax1.set_title('TTC Histogram', fontsize=11)
        ax1.legend(loc='upper right', fontsize=8)
        ax1.grid(True, alpha=0.3)
        
        # === 右图: 累积分布函数 ===
        ax2 = axes[1]
        sorted_ttc = np.sort(ttc_values)
        cdf = np.arange(1, len(sorted_ttc) + 1) / len(sorted_ttc)
        
        ax2.plot(sorted_ttc, cdf * 100, color='#2196F3', linewidth=2)
        ax2.axhline(y=5, color='#E91E63', linestyle=':', linewidth=1.5, label=f'P5 = {p5:.2f}s')
        ax2.axhline(y=10, color='#9C27B0', linestyle=':', linewidth=1.5, label=f'P10 = {p10:.2f}s')
        ax2.axhline(y=50, color='#4CAF50', linestyle=':', linewidth=1.5, label=f'Median = {median:.2f}s')
        
        ax2.axvline(x=3.0, color='#F44336', linestyle='--', linewidth=1.5, alpha=0.7)
        ax2.axvline(x=5.0, color='#FF9800', linestyle='--', linewidth=1, alpha=0.5)
        
        ax2.set_xlabel('TTC (s)', fontsize=11)
        ax2.set_ylabel('Cumulative %', fontsize=11)
        ax2.set_title('TTC Cumulative Distribution Function (CDF)', fontsize=11)
        ax2.legend(loc='lower right', fontsize=9)
        ax2.grid(True, alpha=0.3)
        ax2.set_xlim(0, min(30, sorted_ttc.max() * 1.05))
        ax2.set_ylim(0, 100)
        
        plt.tight_layout()
        
        output_path = os.path.join(self.viz_output_dir, 'ttc_distribution.png')
        plt.savefig(output_path, dpi=150, bbox_inches='tight', facecolor='white')
        plt.close(fig)
        
        print(f"    [TTC可视化] 已生成 TTC 分布图: {output_path}")
    
    def _generate_ttc_danger_scenes(self, danger_points: List[TTCDataPoint], max_scenes: int = 20):
        """
        生成 TTC 危险事件的场景图
        
        每个场景图显示：
        - 自车位置和方向
        - 前方障碍物的位置、大小
        - TTC、距离、速度等关键信息
        """
        if not HAS_MATPLOTLIB or not danger_points:
            return
        
        # 创建 TTC 场景子目录
        scene_dir = os.path.join(self.viz_output_dir, 'ttc_scenes')
        os.makedirs(scene_dir, exist_ok=True)
        
        # 按 TTC 值排序，取最危险的事件
        sorted_points = sorted(danger_points, key=lambda x: x.ttc)[:max_scenes]
        
        for idx, point in enumerate(sorted_points):
            self._visualize_ttc_scene(point, idx, scene_dir)
        
        print(f"    [TTC可视化] 已生成 {len(sorted_points)} 个危险场景图到 {scene_dir}")
    
    def _visualize_ttc_scene(self, point: TTCDataPoint, idx: int, output_dir: str):
        """可视化单个 TTC 危险场景"""
        if not HAS_MATPLOTLIB:
            return
        
        fig, ax = plt.subplots(1, 1, figsize=(12, 8))
        
        # 设置坐标范围
        x_max = max(point.obs_x + point.obs_length + 10, 50)
        y_range = max(abs(point.obs_y) + 5, 10)
        
        ax.set_xlim(-5, x_max)
        ax.set_ylim(-y_range, y_range)
        ax.set_aspect('equal')
        
        # === 绘制自车 ===
        ego_length = self.ego_front + self.ego_rear
        ego_width = self.ego_left + self.ego_right
        ego_x = -self.ego_rear
        ego_y = -ego_width / 2
        
        ego_rect = mpatches.Rectangle(
            (ego_x, ego_y), ego_length, ego_width,
            facecolor='#2196F3', edgecolor='#1565C0', linewidth=2, alpha=0.8
        )
        ax.add_patch(ego_rect)
        
        # 自车方向箭头
        ax.arrow(ego_length/2 - 1, 0, 2, 0, head_width=0.5, head_length=0.3, 
                fc='white', ec='white', linewidth=2)
        ax.text(ego_length/2, -1.5, 'EGO', fontsize=10, ha='center', 
               color='white', fontweight='bold')
        
        # === 绘制障碍物 ===
        obs_x = point.obs_x - point.obs_length / 2
        obs_y = point.obs_y - point.obs_width / 2
        
        # 根据 TTC 危险程度设置颜色
        if point.ttc < 1.5:
            obs_color = '#D32F2F'  # 深红 - 极度危险
            danger_level = "CRITICAL"
        elif point.ttc < 2.0:
            obs_color = '#F44336'  # 红色 - 非常危险
            danger_level = "DANGER"
        else:
            obs_color = '#FF9800'  # 橙色 - 危险
            danger_level = "WARNING"
        
        obs_rect = mpatches.Rectangle(
            (obs_x, obs_y), point.obs_length, point.obs_width,
            facecolor=obs_color, edgecolor='#B71C1C', linewidth=2, alpha=0.8
        )
        ax.add_patch(obs_rect)
        
        # 障碍物标注
        ax.text(point.obs_x, point.obs_y, f'{point.obs_type}', 
               fontsize=9, ha='center', va='center', color='white', fontweight='bold')
        
        # === 绘制距离线 ===
        ax.plot([self.ego_front, point.obs_x - point.obs_length/2], [0, point.obs_y], 
               'r--', linewidth=2, alpha=0.7)
        
        # 距离标注
        mid_x = (self.ego_front + point.obs_x - point.obs_length/2) / 2
        mid_y = point.obs_y / 2
        ax.annotate(f'{point.front_dist:.1f}m', (mid_x, mid_y), fontsize=11, 
                   color='#D32F2F', fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        # === 速度箭头 ===
        # 自车速度
        ego_v_scale = point.ego_speed * 0.5  # 缩放因子
        if ego_v_scale > 0.5:
            ax.arrow(self.ego_front + 1, 0, ego_v_scale, 0, 
                    head_width=0.4, head_length=0.3, fc='#4CAF50', ec='#4CAF50', linewidth=2)
            ax.text(self.ego_front + 1 + ego_v_scale/2, 1.2, 
                   f'Ego: {point.ego_speed*3.6:.1f} km/h', fontsize=9, color='#4CAF50')
            
        # 障碍物速度
        obs_v_scale = point.obs_vx * 0.5
        if abs(obs_v_scale) > 0.3:
            arrow_color = '#FF5722' if obs_v_scale > 0 else '#9C27B0'
            ax.arrow(point.obs_x + point.obs_length/2 + 0.5, point.obs_y, obs_v_scale, 0,
                    head_width=0.4, head_length=0.3, fc=arrow_color, ec=arrow_color, linewidth=2)
            ax.text(point.obs_x + point.obs_length/2 + 1, point.obs_y + 1.5,
                   f'Obs: {point.obs_vx*3.6:.1f} km/h', fontsize=9, color=arrow_color)
        
        # === 信息面板 ===
        info_text = (
            f"═══════ TTC Danger Event ═══════\n"
            f"Frame: {point.frame_idx}  |  Time: {point.timestamp:.2f}s\n"
            f"───────────────────────────────\n"
            f"TTC: {point.ttc:.2f} s  ({danger_level})\n"
            f"Front Distance: {point.front_dist:.2f} m\n"
            f"Ego Speed: {point.ego_speed*3.6:.1f} km/h\n"
            f"Obstacle Speed: {point.obs_vx*3.6:.1f} km/h\n"
            f"Relative Vel: {point.relative_vel*3.6:.1f} km/h\n"
            f"───────────────────────────────\n"
            f"Obstacle Type: {point.obs_type}\n"
            f"Obstacle Size: {point.obs_length:.1f}m × {point.obs_width:.1f}m\n"
            f"Position: ({point.obs_x:.1f}, {point.obs_y:.1f}) m"
        )
        
        # 信息框背景颜色
        info_bg_color = '#FFEBEE' if point.ttc < 2.0 else '#FFF3E0'
        ax.text(0.98, 0.98, info_text, transform=ax.transAxes, fontsize=10,
               verticalalignment='top', horizontalalignment='right',
               fontfamily='monospace',
               bbox=dict(boxstyle='round', facecolor=info_bg_color, edgecolor=obs_color, 
                        alpha=0.95, linewidth=2))
        
        # === 标题和标签 ===
        ax.set_title(f'TTC Danger Scene #{idx+1} - TTC={point.ttc:.2f}s', 
                    fontsize=14, fontweight='bold', color=obs_color)
        ax.set_xlabel('X - Forward (m)', fontsize=11)
        ax.set_ylabel('Y - Left (m)', fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.5, alpha=0.5)
        ax.axvline(x=0, color='gray', linestyle='-', linewidth=0.5, alpha=0.5)
        
        # 图例
        legend_elements = [
            mpatches.Patch(facecolor='#2196F3', edgecolor='#1565C0', label='Ego Vehicle'),
            mpatches.Patch(facecolor=obs_color, edgecolor='#B71C1C', label=f'Obstacle ({point.obs_type})'),
            Line2D([0], [0], color='r', linestyle='--', linewidth=2, label='Distance Line'),
        ]
        ax.legend(handles=legend_elements, loc='lower right', fontsize=9)
        
        plt.tight_layout()
        
        # 保存
        output_path = os.path.join(output_dir, f'ttc_scene_{idx+1:02d}_ttc{point.ttc:.2f}s.png')
        plt.savefig(output_path, dpi=120, bbox_inches='tight', facecolor='white')
        plt.close(fig)
    
    @property
    def supports_streaming(self) -> bool:
        """支持流式收集模式"""
        return True
    
    def _add_empty_results(self):
        """添加空结果"""
        self.add_result(KPIResult(
            name="近距离障碍物数(10m圆)", value=0.0, unit="个/帧",
            description="数据不足，无法统计ROI障碍物"
        ))
        self.add_result(KPIResult(
            name="中距离障碍物数", value=0.0, unit="个/帧",
            description="数据不足，无法统计ROI障碍物"
        ))
        self.add_result(KPIResult(
            name="远距离障碍物数", value=0.0, unit="个/帧",
            description="数据不足，无法统计ROI障碍物"
        ))
    
    # ========== 流式模式支持 ==========
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """
        收集 ROI 障碍物统计数据（流式模式）
        
        由于障碍物数据量大，这里直接计算每帧的统计结果而不是存储原始数据
        可视化数据采样收集（每1000帧采样1帧，最多采样20帧）
        """
        near_radius, mid_roi, far_roi = self._create_roi_regions()
        
        # 可视化采样参数（收集足够多的帧，后续再随机抽样）
        viz_sample_interval = 100  # 每100帧采样1帧
        viz_max_samples = 200  # 最多采样200帧（后续会随机抽样30帧生成图）
        
        for frame in synced_frames:
            fm_msg = frame.messages.get("/function/function_manager")
            obs_msg = frame.messages.get("/perception/fusion/obstacle_list_utm")
            
            if fm_msg is None:
                continue
            
            operator_type = MessageAccessor.get_field(fm_msg, "operator_type")
            if operator_type != self.auto_operator_type:
                continue
            
            # 获取自车速度
            ego_speed = 0.0
            chassis_msg = frame.messages.get("/vehicle/chassis_domain_report")
            if chassis_msg is not None:
                ego_speed = MessageAccessor.get_field(
                    chassis_msg, "motion_system.vehicle_speed", 0.0) or 0.0
                ego_speed = ego_speed / 3.6  # km/h -> m/s
            
            # 获取障碍物列表
            obstacles = self._extract_obstacles(obs_msg)
            
            # 计算三级 ROI 统计
            near_obs = [obs for obs in obstacles if obs.center_distance <= near_radius]
            near_stat = self._compute_roi_stats(near_obs)
            mid_stat = self._compute_roi_stats_rect(obstacles, mid_roi)
            far_stat = self._compute_roi_stats_rect(obstacles, far_roi)
            
            # 计算距离和 TTC
            frame_min_dist = float('inf')
            frame_nearest_track_id = None  # 最近障碍物的 track_id
            frame_nearest_type = None  # 最近障碍物的类型
            frame_ttc = None
            front_dist = None
            frame_relative_vel = None
            frame_ttc_obs = None  # 记录造成最小 TTC 的障碍物
            
            for obs in obstacles:
                dist = obs.distance_to_ego
                if dist < frame_min_dist:
                    frame_min_dist = dist
                    frame_nearest_track_id = obs.track_id  # 记录最近障碍物的 track_id
                    frame_nearest_type = obs.type_name_cn  # 记录最近障碍物的类型
                
                # TTC 计算
                if obs.is_in_collision_path(lateral_margin=0.3):
                    obs_front_dist = obs.front_distance
                    if ego_speed > 0.5 and obs_front_dist > 0:
                        relative_vel = ego_speed - obs.velocity_x
                        if relative_vel > 0.1:
                            ttc = obs_front_dist / relative_vel
                            if ttc < 30:
                                if frame_ttc is None or ttc < frame_ttc:
                                    frame_ttc = ttc
                                    front_dist = obs_front_dist
                                    frame_relative_vel = relative_vel
                                    frame_ttc_obs = obs  # 记录造成最小 TTC 的障碍物
            
            # 收集 TTC 危险事件的完整信息（用于生成危险场景图）
            if frame_ttc is not None and frame_ttc < 3 and frame_ttc_obs is not None:
                global_frame_idx = len(streaming_data.roi_data)
                streaming_data.ttc_danger_points.append(TTCDataPoint(
                    timestamp=frame.timestamp,
                    ttc=frame_ttc,
                    front_dist=front_dist,
                    ego_speed=ego_speed,
                    relative_vel=frame_relative_vel,
                    is_danger=True,
                    obs_x=frame_ttc_obs.position_x,
                    obs_y=frame_ttc_obs.position_y,
                    obs_vx=frame_ttc_obs.velocity_x,
                    obs_length=frame_ttc_obs.length,
                    obs_width=frame_ttc_obs.width,
                    obs_type=frame_ttc_obs.type_name,
                    obs_type_cn=frame_ttc_obs.type_name_cn,
                    track_id=frame_ttc_obs.track_id,
                    frame_idx=global_frame_idx
                ))
            
            # 存储每帧的统计数据: (near_total, near_static, near_moving, 
            #                      mid_total, mid_static, mid_moving,
            #                      far_total, far_static, far_moving,
            #                      min_dist, ttc, front_dist, ego_speed, timestamp, relative_vel, 
            #                      nearest_track_id, nearest_type)
            streaming_data.roi_data.append((
                near_stat.total_count, near_stat.static_count, near_stat.moving_count,
                mid_stat.total_count, mid_stat.static_count, mid_stat.moving_count,
                far_stat.total_count, far_stat.static_count, far_stat.moving_count,
                frame_min_dist if frame_min_dist < float('inf') else None,
                frame_ttc,
                front_dist,
                ego_speed,
                frame.timestamp,
                frame_relative_vel,
                frame_nearest_track_id,  # 最近障碍物的 track_id
                frame_nearest_type  # 最近障碍物的类型
            ))
            
            # 可视化数据采样收集（使用全局帧数判断）
            # 注意：roi_data 已经 append，所以当前帧索引是 len - 1
            global_frame_idx = len(streaming_data.roi_data)
            current_viz_count = len(streaming_data.roi_viz_data)
            # 第1帧、第101帧、第201帧... 采样（每100帧采样1帧）
            should_sample = (global_frame_idx == 1 or global_frame_idx % viz_sample_interval == 1)
            if (self.viz_enabled and HAS_MATPLOTLIB and obstacles and 
                current_viz_count < viz_max_samples and should_sample):
                streaming_data.roi_viz_data.append(FrameVizData(
                    frame_idx=global_frame_idx - 1,
                    timestamp=frame.timestamp,
                    obstacles=obstacles,  # 保存完整障碍物列表用于可视化
                    near_stat=near_stat,
                    mid_stat=mid_stat,
                    far_stat=far_stat,
                    ego_speed=ego_speed
                ))
            
            # 收集危险距离帧（距离 < 3m），最多收集 100 帧
            if (self.viz_enabled and HAS_MATPLOTLIB and obstacles and 
                frame_min_dist < 3.0 and len(streaming_data.danger_distance_frames) < 100):
                streaming_data.danger_distance_frames.append(FrameVizData(
                    frame_idx=global_frame_idx - 1,
                    timestamp=frame.timestamp,
                    obstacles=obstacles,
                    near_stat=near_stat,
                    mid_stat=mid_stat,
                    far_stat=far_stat,
                    ego_speed=ego_speed
                ))
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """
        从收集的数据计算 ROI 障碍物统计KPI（流式模式）
        """
        self.clear_results()
        
        # 获取输出目录（用于可视化）
        output_dir = kwargs.get('output_dir')
        if output_dir:
            self.viz_output_dir = os.path.join(output_dir, 'roi_viz')
        
        if len(streaming_data.roi_data) == 0:
            self._add_empty_results()
            return self.get_results()
        
        # 解构数据
        near_totals = np.array([d[0] for d in streaming_data.roi_data])
        near_statics = np.array([d[1] for d in streaming_data.roi_data])
        near_movings = np.array([d[2] for d in streaming_data.roi_data])
        
        mid_totals = np.array([d[3] for d in streaming_data.roi_data])
        mid_statics = np.array([d[4] for d in streaming_data.roi_data])
        mid_movings = np.array([d[5] for d in streaming_data.roi_data])
        
        far_totals = np.array([d[6] for d in streaming_data.roi_data])
        far_statics = np.array([d[7] for d in streaming_data.roi_data])
        far_movings = np.array([d[8] for d in streaming_data.roi_data])
        
        # TTC 数据
        all_ttc = [d[10] for d in streaming_data.roi_data if d[10] is not None]
        
        frame_count = len(streaming_data.roi_data)
        bag_mapper = BagTimeMapper(streaming_data.bag_infos)
        
        # ========== 近距离 ROI 结果 ==========
        self.add_result(KPIResult(
            name="近距离障碍物数(10m圆)",
            value=round(float(np.mean(near_totals)), 2),
            unit="个/帧",
            description=f"半径{self.near_radius}m圆形区域内的平均障碍物数",
            details={
                'mean': round(float(np.mean(near_totals)), 2),
                'max': int(np.max(near_totals)),
                'p95': round(float(np.percentile(near_totals, 95)), 2),
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="近距离静态障碍物数",
            value=round(float(np.mean(near_statics)), 2),
            unit="个/帧",
            description=f"近距离{self.near_radius}m圆形区域内静态障碍物"
        ))
        
        self.add_result(KPIResult(
            name="近距离动态障碍物数",
            value=round(float(np.mean(near_movings)), 2),
            unit="个/帧",
            description=f"近距离{self.near_radius}m圆形区域内动态障碍物"
        ))
        
        # ========== 中距离 ROI 结果 ==========
        cfg = self.mid_roi_config
        self.add_result(KPIResult(
            name="中距离障碍物数",
            value=round(float(np.mean(mid_totals)), 2),
            unit="个/帧",
            description=f"前{cfg['front']}m后{cfg['rear']}m左右{cfg['left']}m矩形区域",
            details={
                'mean': round(float(np.mean(mid_totals)), 2),
                'max': int(np.max(mid_totals)),
                'p95': round(float(np.percentile(mid_totals, 95)), 2),
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="中距离静态障碍物数",
            value=round(float(np.mean(mid_statics)), 2),
            unit="个/帧",
            description="中距离矩形区域内静态障碍物"
        ))
        
        self.add_result(KPIResult(
            name="中距离动态障碍物数",
            value=round(float(np.mean(mid_movings)), 2),
            unit="个/帧",
            description="中距离矩形区域内动态障碍物"
        ))
        
        # ========== 远距离 ROI 结果 ==========
        cfg = self.far_roi_config
        self.add_result(KPIResult(
            name="远距离障碍物数",
            value=round(float(np.mean(far_totals)), 2),
            unit="个/帧",
            description=f"前{cfg['front']}m后{cfg['rear']}m左右{cfg['left']}m矩形区域",
            details={
                'mean': round(float(np.mean(far_totals)), 2),
                'max': int(np.max(far_totals)),
                'p95': round(float(np.percentile(far_totals, 95)), 2),
                'frame_count': frame_count
            }
        ))
        
        self.add_result(KPIResult(
            name="远距离静态障碍物数",
            value=round(float(np.mean(far_statics)), 2),
            unit="个/帧",
            description="远距离矩形区域内静态障碍物"
        ))
        
        self.add_result(KPIResult(
            name="远距离动态障碍物数",
            value=round(float(np.mean(far_movings)), 2),
            unit="个/帧",
            description="远距离矩形区域内动态障碍物"
        ))
        
        # ========== 距离统计 ==========
        # 获取有 min_dist 的数据（包含时间戳、帧索引、track_id 和类型）
        # roi_data 格式: (..., min_dist[9], ..., timestamp[13], relative_vel[14], nearest_track_id[15], nearest_type[16])
        dist_data = [(d[9], d[13], i, d[15] if len(d) > 15 else None, d[16] if len(d) > 16 else None) 
                     for i, d in enumerate(streaming_data.roi_data) if d[9] is not None]
        
        if dist_data:
            all_min_distances = np.array([d[0] for d in dist_data])
            timestamps = [d[1] for d in dist_data]
            frame_indices = [d[2] for d in dist_data]
            track_ids = [d[3] for d in dist_data]
            obs_types = [d[4] for d in dist_data]
            
            danger_count = np.sum(all_min_distances < 1.5)
            warning_count = np.sum((all_min_distances >= 1.5) & (all_min_distances < 3.0))
            
            # 按障碍物类型统计
            type_stats = {}  # {type_name: {'count': n, 'min': m, 'p5': p, 'danger_count': d}}
            for i, obs_type in enumerate(obs_types):
                if obs_type is None:
                    obs_type = "未知"
                if obs_type not in type_stats:
                    type_stats[obs_type] = {'distances': [], 'danger_count': 0}
                type_stats[obs_type]['distances'].append(all_min_distances[i])
                if all_min_distances[i] < 1.5:
                    type_stats[obs_type]['danger_count'] += 1
            
            # 计算各类型的统计值
            type_details = {}
            for obs_type, stats in type_stats.items():
                dists = np.array(stats['distances'])
                type_details[obs_type] = {
                    'count': len(dists),
                    'min': round(float(np.min(dists)), 2),
                    'p5': round(float(np.percentile(dists, 5)), 2) if len(dists) >= 20 else round(float(np.min(dists)), 2),
                    'danger_count': stats['danger_count'],
                    'danger_rate': round(stats['danger_count'] / len(dists) * 100, 2) if len(dists) > 0 else 0
                }
            
            self.add_result(KPIResult(
                name="最近障碍物距离P5",
                value=round(float(np.percentile(all_min_distances, 5)), 2),
                unit="m",
                description="障碍物边界框到自车边界框的最短距离5分位数",
                details={
                    'min': round(float(np.min(all_min_distances)), 2),
                    'p5': round(float(np.percentile(all_min_distances, 5)), 2),
                    'p10': round(float(np.percentile(all_min_distances, 10)), 2),
                    'median': round(float(np.median(all_min_distances)), 2),
                    'sample_count': len(all_min_distances),
                    'by_type': type_details
                }
            ))
            
            # 危险帧率（包含溯源）- 按 track_id 聚合事件
            danger_rate = danger_count / len(all_min_distances) * 100
            
            # 按类型统计危险帧率
            type_danger_stats = {}
            for obs_type, stats in type_stats.items():
                type_danger_stats[obs_type] = {
                    'danger_count': stats['danger_count'],
                    'total_count': len(stats['distances']),
                    'danger_rate': round(stats['danger_count'] / len(stats['distances']) * 100, 2) if stats['distances'] else 0
                }
            
            danger_result = KPIResult(
                name="危险帧率(<1.5m)",
                value=round(danger_rate, 2),
                unit="%",
                description="障碍物边界框距自车<1.5m的帧占比",
                details={
                    'danger_count': int(danger_count),
                    'warning_count': int(warning_count),
                    'frame_count': len(all_min_distances),
                    'by_type': type_danger_stats
                }
            )
            
            # 按 track_id 聚合危险距离事件
            danger_indices = np.where(all_min_distances < 1.5)[0]
            
            # 按 track_id 分组事件（包含类型信息）
            track_events = {}  # {track_id: [(idx, dist, ts, frame_idx, obs_type), ...]}
            for i in danger_indices:
                tid = track_ids[i]
                obs_type = obs_types[i] if obs_types[i] else "未知"
                if tid is None:
                    tid = -1  # 无 track_id 的情况
                if tid not in track_events:
                    track_events[tid] = []
                track_events[tid].append((i, all_min_distances[i], timestamps[i], frame_indices[i], obs_type))
            
            # 为每个 track_id 创建聚合事件
            aggregated_events = []
            for tid, events in track_events.items():
                # 按时间排序
                events.sort(key=lambda x: x[2])
                
                # 分割成连续的事件段（时间间隔 > 1s 认为是新事件）
                event_groups = []
                current_group = [events[0]]
                for i in range(1, len(events)):
                    time_gap = events[i][2] - events[i-1][2]
                    if time_gap <= 1.0:  # 1秒内认为是连续事件
                        current_group.append(events[i])
                    else:
                        event_groups.append(current_group)
                        current_group = [events[i]]
                event_groups.append(current_group)
                
                # 为每个事件组创建聚合记录
                for group in event_groups:
                    min_dist = min(e[1] for e in group)
                    start_ts = group[0][2]
                    end_ts = group[-1][2]
                    frame_count = len(group)
                    start_frame = group[0][3]
                    # 取出现最多的类型作为该事件的类型
                    type_counts = {}
                    for e in group:
                        t = e[4]
                        type_counts[t] = type_counts.get(t, 0) + 1
                    primary_type = max(type_counts, key=type_counts.get)
                    
                    aggregated_events.append({
                        'track_id': tid,
                        'obs_type': primary_type,
                        'min_dist': min_dist,
                        'start_ts': start_ts,
                        'end_ts': end_ts,
                        'frame_count': frame_count,
                        'start_frame': start_frame,
                        'duration': end_ts - start_ts
                    })
            
            # 按最小距离排序，取最危险的事件
            aggregated_events.sort(key=lambda x: x['min_dist'])
            
            for event in aggregated_events[:30]:  # 最多 30 个事件
                tid_str = f"ID:{event['track_id']}" if event['track_id'] != -1 else "未知ID"
                type_str = event['obs_type']
                duration_str = f"持续{event['duration']*1000:.0f}ms/{event['frame_count']}帧" if event['frame_count'] > 1 else ""
                danger_result.anomalies.append(AnomalyRecord(
                    timestamp=event['start_ts'],
                    frame_idx=event['start_frame'],
                    bag_name=bag_mapper.get_bag_name(event['start_ts']),
                    description=f"障碍物距离危险[{type_str}/{tid_str}]：最近{event['min_dist']:.2f}m（<1.5m）{duration_str}",
                    value=round(event['min_dist'], 2),
                    threshold=1.5
                ))
            self.add_result(danger_result)
        
        # ========== TTC 统计 ==========
        # 获取有 TTC 的数据
        ttc_data = [(d[10], d[11], d[13], i) for i, d in enumerate(streaming_data.roi_data) if d[10] is not None]
        
        if ttc_data:
            all_ttc = np.array([d[0] for d in ttc_data])
            front_dists = [d[1] for d in ttc_data]
            timestamps_ttc = [d[2] for d in ttc_data]
            frame_indices_ttc = [d[3] for d in ttc_data]
            
            # 按障碍物类型统计 TTC
            ttc_danger_points = streaming_data.ttc_danger_points
            type_ttc_stats = {}  # {type_cn: {'count': n, 'min_ttc': m}}
            for pt in ttc_danger_points:
                type_cn = pt.obs_type_cn
                if type_cn not in type_ttc_stats:
                    type_ttc_stats[type_cn] = {'count': 0, 'min_ttc': float('inf')}
                type_ttc_stats[type_cn]['count'] += 1
                type_ttc_stats[type_cn]['min_ttc'] = min(type_ttc_stats[type_cn]['min_ttc'], pt.ttc)
            
            # 格式化类型统计
            type_details = {}
            for type_cn, stats in type_ttc_stats.items():
                type_details[type_cn] = {
                    'danger_count': stats['count'],
                    'min_ttc': round(stats['min_ttc'], 2) if stats['min_ttc'] < float('inf') else None
                }
            
            ttc_result = KPIResult(
                name="TTC P5",
                value=round(float(np.percentile(all_ttc, 5)), 2),
                unit="s",
                description="前方 TTC 的5分位数（最危险5%情况）",
                details={
                    'min': round(float(np.min(all_ttc)), 2),
                    'p5': round(float(np.percentile(all_ttc, 5)), 2),
                    'median': round(float(np.median(all_ttc)), 2),
                    'sample_count': len(all_ttc),
                    'rate_lt_3s': round(float(np.sum(all_ttc < 3) / len(all_ttc) * 100), 1),
                    'by_type': type_details
                }
            )
            
            # 按 track_id 聚合 TTC 危险事件
            if ttc_danger_points:
                # 按 track_id 分组
                track_ttc_events = {}  # {track_id: [TTCDataPoint, ...]}
                for pt in ttc_danger_points:
                    tid = pt.track_id if pt.track_id else -1
                    if tid not in track_ttc_events:
                        track_ttc_events[tid] = []
                    track_ttc_events[tid].append(pt)
                
                # 为每个 track_id 创建聚合事件
                aggregated_ttc_events = []
                for tid, events in track_ttc_events.items():
                    # 按时间排序
                    events.sort(key=lambda x: x.timestamp)
                    
                    # 分割成连续的事件段（时间间隔 > 1s 认为是新事件）
                    event_groups = []
                    current_group = [events[0]]
                    for i in range(1, len(events)):
                        time_gap = events[i].timestamp - events[i-1].timestamp
                        if time_gap <= 1.0:  # 1秒内认为是连续事件
                            current_group.append(events[i])
                        else:
                            event_groups.append(current_group)
                            current_group = [events[i]]
                    event_groups.append(current_group)
                    
                    # 为每个事件组创建聚合记录
                    for group in event_groups:
                        min_ttc = min(e.ttc for e in group)
                        min_ttc_pt = min(group, key=lambda x: x.ttc)
                        start_ts = group[0].timestamp
                        end_ts = group[-1].timestamp
                        frame_count = len(group)
                        start_frame = group[0].frame_idx
                        
                        aggregated_ttc_events.append({
                            'track_id': tid,
                            'obs_type_cn': min_ttc_pt.obs_type_cn,
                            'min_ttc': min_ttc,
                            'front_dist': min_ttc_pt.front_dist,
                            'start_ts': start_ts,
                            'end_ts': end_ts,
                            'frame_count': frame_count,
                            'start_frame': start_frame,
                            'duration': end_ts - start_ts
                        })
                
                # 按最小 TTC 排序，取最危险的事件
                aggregated_ttc_events.sort(key=lambda x: x['min_ttc'])
                
                for event in aggregated_ttc_events[:30]:  # 最多 30 个事件
                    tid_str = f"ID:{event['track_id']}" if event['track_id'] != -1 else "未知ID"
                    type_str = event['obs_type_cn']
                    duration_str = f"持续{event['duration']*1000:.0f}ms/{event['frame_count']}帧" if event['frame_count'] > 1 else ""
                    dist_str = f"距离{event['front_dist']:.2f}m" if event['front_dist'] else ""
                    ttc_result.anomalies.append(AnomalyRecord(
                        timestamp=event['start_ts'],
                        frame_idx=event['start_frame'],
                        bag_name=bag_mapper.get_bag_name(event['start_ts']),
                        description=f"TTC危险[{type_str}/{tid_str}]：TTC={event['min_ttc']:.2f}s（<3s）{dist_str} {duration_str}",
                        value=round(event['min_ttc'], 2),
                        threshold=3.0
                    ))
            
            self.add_result(ttc_result)
            
            # 帧示例图可视化（流式模式）
            if self.viz_enabled and HAS_MATPLOTLIB and streaming_data.roi_viz_data:
                near_radius, mid_roi, far_roi = self._create_roi_regions()
                self._generate_visualizations(streaming_data.roi_viz_data, near_radius, mid_roi, far_roi)
                # 生成危险距离帧可视化（使用专门收集的危险距离帧数据）
                if streaming_data.danger_distance_frames:
                    self._generate_danger_distance_scenes(streaming_data.danger_distance_frames, near_radius, mid_roi, far_roi)
            
            # TTC 可视化（流式模式）
            if self.viz_enabled and HAS_MATPLOTLIB:
                # 获取 relative_vel 数据（兼容旧数据格式）
                relative_vels = []
                for d in streaming_data.roi_data:
                    if d[10] is not None:  # ttc 不为空
                        # 检查是否有 relative_vel (index 14)
                        if len(d) > 14 and d[14] is not None:
                            relative_vels.append(d[14])
                        else:
                            # 旧格式没有 relative_vel，从 ttc 和 front_dist 推算
                            if d[11] is not None and d[10] > 0:
                                relative_vels.append(d[11] / d[10])
                            else:
                                relative_vels.append(0)
                
                # 构建 TTCDataPoint 列表
                ttc_data_points = []
                for i, (ttc_val, front_d, ts, ego_sp) in enumerate(zip(all_ttc, front_dists, timestamps_ttc, 
                                                                        [d[12] for d in streaming_data.roi_data if d[10] is not None])):
                    rel_vel = relative_vels[i] if i < len(relative_vels) else 0
                    ttc_data_points.append(TTCDataPoint(
                        timestamp=ts,
                        ttc=ttc_val,
                        front_dist=front_d if front_d else 0,
                        ego_speed=ego_sp,
                        relative_vel=rel_vel,
                        is_danger=(ttc_val < 3),
                        frame_idx=frame_indices_ttc[i]
                    ))
                
                if ttc_data_points:
                    # 传递收集的危险点（带有完整障碍物信息）用于生成场景图
                    self._generate_ttc_visualization(
                        ttc_data_points, 
                        danger_points_with_obs=streaming_data.ttc_danger_points
                    )
        
        return self.get_results()
