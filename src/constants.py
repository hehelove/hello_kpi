"""
常量定义
避免硬编码的魔法字符串
"""
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field


class Topics:
    """ROS2 Topic 名称常量"""
    # 功能管理
    FUNCTION_MANAGER = "/function/function_manager"
    
    # 定位
    LOCALIZATION = "/localization/localization"
    
    # 控制
    CONTROL_DEBUG = "/control/debug"
    CONTROL = "/control/control"
    
    # 规划
    PLANNING_TRAJECTORY = "/planning/trajectory"
    PLANNING_DEBUG = "/planning/debug"
    
    # 感知
    PERCEPTION_OBSTACLES = "/perception/fusion/obstacle_list_utm"
    
    # 底盘
    CHASSIS_DOMAIN = "/vehicle_io/chassis_domain"
    
    # 地图
    MAP = "/map/map"


class KPINames:
    """KPI 名称常量"""
    MILEAGE = "里程统计"
    TAKEOVER = "接管统计"
    LANE_KEEPING = "车道保持"
    STEERING_SMOOTHNESS = "转向平滑度"
    COMFORT = "舒适度"
    EMERGENCY_EVENTS = "紧急事件检测"
    WEAVING = "画龙检测"
    ROI_OBSTACLES = "ROI障碍物统计"
    LOCALIZATION = "定位统计"
    CURVATURE = "道路曲率"
    SPEEDING = "超速检测"
    
    # 需要里程依赖的 KPI
    MILEAGE_DEPENDENT = [
        TAKEOVER,
        EMERGENCY_EVENTS,
        WEAVING,
        STEERING_SMOOTHNESS,
        SPEEDING
    ]


class KPIResultNames:
    """KPI 结果名称常量"""
    AUTO_MILEAGE = "自动驾驶里程"
    TOTAL_MILEAGE = "总里程"
    TAKEOVER_COUNT = "接管次数"
    TAKEOVER_RATE = "接管率"


class ConfigKeys:
    """配置文件键名常量"""
    TOPICS = "topics"
    BASE = "base"
    SYNC_TOLERANCE = "sync_tolerance"
    CURVATURE_THRESHOLD = "curvature_threshold"
    MAPBOX_TOKEN = "mapbox_token"


class KPIStatus:
    """KPI 计算状态"""
    SUCCESS = "success"
    PARTIAL = "partial"  # 部分成功
    FAILED = "failed"
    SKIPPED = "skipped"


# ============================================================================
# 轻量级消息类 - 只保存需要的字段，大幅降低内存占用
# ============================================================================

@dataclass
class LightMessage:
    """
    轻量级消息基类
    用于替代完整的 ROS 消息，只保留 KPI 计算所需的字段
    """
    _topic: str = ""
    
    def get(self, field_path: str, default: Any = None) -> Any:
        """
        兼容 MessageAccessor.get_field 的访问方式
        支持嵌套路径如 'motion_system.vehicle_speed'
        """
        parts = field_path.split('.')
        obj = self
        for part in parts:
            if hasattr(obj, part):
                obj = getattr(obj, part)
            elif isinstance(obj, dict) and part in obj:
                obj = obj[part]
            else:
                return default
        return obj


@dataclass
class FunctionManagerMsg(LightMessage):
    """功能管理消息 - 轻量版"""
    operator_type: int = 0
    _topic: str = field(default=Topics.FUNCTION_MANAGER, init=False)


@dataclass
class PositionStddev:
    """位置标准差"""
    east: Optional[float] = None
    north: Optional[float] = None


@dataclass
class Position:
    """位置信息"""
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@dataclass
class GlobalLocalization:
    """全局定位信息"""
    position: Position = field(default_factory=Position)
    position_stddev: PositionStddev = field(default_factory=PositionStddev)


@dataclass
class LocalizationStatus:
    """定位状态"""
    common: Optional[int] = None


@dataclass
class LocalizationMsg(LightMessage):
    """定位消息 - 轻量版"""
    global_localization: GlobalLocalization = field(default_factory=GlobalLocalization)
    status: LocalizationStatus = field(default_factory=LocalizationStatus)
    _topic: str = field(default=Topics.LOCALIZATION, init=False)


@dataclass
class MotionSystem:
    """运动系统数据"""
    vehicle_longitudinal_acceleration: Optional[float] = None
    vehicle_lateral_acceleration: Optional[float] = None
    vehicle_speed: Optional[float] = None


@dataclass
class EpsSystem:
    """EPS 系统数据"""
    actual_steering_angle: Optional[float] = None
    actual_steering_angle_velocity: Optional[float] = None


@dataclass
class ChassisDomainMsg(LightMessage):
    """底盘消息 - 轻量版"""
    motion_system: MotionSystem = field(default_factory=MotionSystem)
    eps_system: EpsSystem = field(default_factory=EpsSystem)
    _topic: str = field(default="/vehicle/chassis_domain_report", init=False)


@dataclass
class PathPoint:
    """轨迹点"""
    x: Optional[float] = None
    y: Optional[float] = None
    kappa: Optional[float] = None


@dataclass
class TrajectoryMsg(LightMessage):
    """轨迹消息 - 轻量版"""
    path_point: List[PathPoint] = field(default_factory=list)
    lane_id: List[str] = field(default_factory=list)  # 用于场景检测
    _topic: str = field(default=Topics.PLANNING_TRAJECTORY, init=False)


@dataclass
class ObstaclePosition:
    """障碍物位置"""
    x: Optional[float] = None
    y: Optional[float] = None


@dataclass
class ObstacleVelocity:
    """障碍物速度"""
    x: Optional[float] = None
    y: Optional[float] = None


@dataclass
class TypeHistory:
    """类型历史"""
    type: int = 0


@dataclass
class Obstacle:
    """单个障碍物"""
    track_id: int = 0  # 障碍物跟踪ID
    position: ObstaclePosition = field(default_factory=ObstaclePosition)
    velocity: ObstacleVelocity = field(default_factory=ObstacleVelocity)
    length: Optional[float] = None
    width: Optional[float] = None
    heading_to_ego: Optional[float] = None
    motion_status: int = 0
    type_history: List[TypeHistory] = field(default_factory=list)


@dataclass
class ObstacleListMsg(LightMessage):
    """障碍物列表消息 - 轻量版"""
    obstacles: List[Obstacle] = field(default_factory=list)
    _topic: str = field(default=Topics.PERCEPTION_OBSTACLES, init=False)


@dataclass
class ControlDebugMsg(LightMessage):
    """控制调试消息 - 轻量版（Proto 解析后）"""
    reserved0: bytes = field(default=b'')  # 原始 proto 数据
    _topic: str = field(default=Topics.CONTROL_DEBUG, init=False)


@dataclass
class PlanningDebugMsg(LightMessage):
    """Planning 调试消息 - 轻量版（保留原始 proto 数据）"""
    data: bytes = field(default=b'')  # 原始 proto 数据
    _topic: str = field(default=Topics.PLANNING_DEBUG, init=False)


@dataclass
class MapLane:
    """地图车道信息（用于场景检测）"""
    id: str = ""
    turn: int = 1  # 1=直行, 2=左转, 3=右转, 4=掉头
    junction_id: Optional[str] = None
    lane_type: int = 0


@dataclass
class MapMsg(LightMessage):
    """地图消息 - 轻量版（只保存场景检测需要的 lanes 信息）"""
    lanes: List[MapLane] = field(default_factory=list)
    _topic: str = field(default=Topics.MAP, init=False)


@dataclass
class ChassisControl:
    """底盘控制数据"""
    target_longitudinal_acceleration: Optional[float] = None


@dataclass
class ControlMsg(LightMessage):
    """控制消息 - 轻量版"""
    chassis_control: ChassisControl = field(default_factory=ChassisControl)
    _topic: str = field(default=Topics.CONTROL, init=False)


# ============================================================================
# 字段提取配置 - 定义每个 Topic 需要提取的字段路径
# ============================================================================

TOPIC_REQUIRED_FIELDS: Dict[str, List[str]] = {
    Topics.FUNCTION_MANAGER: [
        "operator_type"
    ],
    Topics.LOCALIZATION: [
        "global_localization.latitude",
        "global_localization.longitude",
        "status.common",
        "global_localization.position_stddev.east",
        "global_localization.position_stddev.north"
    ],
    "/vehicle/chassis_domain_report": [
        "motion_system.vehicle_longitudinal_acceleration",
        "motion_system.vehicle_lateral_acceleration",
        "motion_system.vehicle_speed",
        "motion_system.steering_wheel_angle",
        "motion_system.steering_wheel_speed"
    ],
    Topics.PLANNING_TRAJECTORY: [
        "path_point",  # 数组字段，需要特殊处理
        "lane_id"  # 用于场景检测
    ],
    Topics.CONTROL_DEBUG: [
        "reserved0"  # Proto 原始数据
    ],
    Topics.PERCEPTION_OBSTACLES: [
        "obstacles"  # 数组字段，需要特殊处理
    ],
    Topics.CONTROL: [
        "chassis_control.target_longitudinal_acceleration"
    ],
    Topics.PLANNING_DEBUG: [
        "data"  # 原始 Proto 数据
    ],
    Topics.MAP: [
        "lanes"  # 车道信息（用于场景检测）
    ]
}


def extract_light_message(topic: str, msg: Any) -> Optional[LightMessage]:
    """
    从完整 ROS 消息中提取轻量级消息
    
    Args:
        topic: Topic 名称
        msg: 完整的 ROS 消息对象
        
    Returns:
        轻量级消息对象，失败返回 None
    """
    from .data_loader.bag_reader import MessageAccessor
    
    try:
        if topic == Topics.FUNCTION_MANAGER:
            return FunctionManagerMsg(
                operator_type=MessageAccessor.get_field(msg, "operator_type", 0)
            )
        
        elif topic == Topics.LOCALIZATION:
            return LocalizationMsg(
                global_localization=GlobalLocalization(
                    position=Position(
                        latitude=MessageAccessor.get_field(msg, "global_localization.position.latitude", None),
                        longitude=MessageAccessor.get_field(msg, "global_localization.position.longitude", None)
                    ),
                    position_stddev=PositionStddev(
                        east=MessageAccessor.get_field(msg, "global_localization.position_stddev.east", None),
                        north=MessageAccessor.get_field(msg, "global_localization.position_stddev.north", None)
                    )
                ),
                status=LocalizationStatus(
                    common=MessageAccessor.get_field(msg, "status.common", None)
                )
            )
        
        elif topic == "/vehicle/chassis_domain_report":
            return ChassisDomainMsg(
                motion_system=MotionSystem(
                    vehicle_longitudinal_acceleration=MessageAccessor.get_field(
                        msg, "motion_system.vehicle_longitudinal_acceleration", None),
                    vehicle_lateral_acceleration=MessageAccessor.get_field(
                        msg, "motion_system.vehicle_lateral_acceleration", None),
                    vehicle_speed=MessageAccessor.get_field(
                        msg, "motion_system.vehicle_speed", None)
                ),
                eps_system=EpsSystem(
                    actual_steering_angle=MessageAccessor.get_field(
                        msg, "eps_system.actual_steering_angle", None),
                    actual_steering_angle_velocity=MessageAccessor.get_field(
                        msg, "eps_system.actual_steering_angle_velocity", None)
                )
            )
        
        elif topic == Topics.PLANNING_TRAJECTORY:
            raw_points = MessageAccessor.get_field(msg, "path_point", [])
            path_points = []
            for pt in raw_points:
                path_points.append(PathPoint(
                    x=MessageAccessor.get_field(pt, "x", None),
                    y=MessageAccessor.get_field(pt, "y", None),
                    kappa=MessageAccessor.get_field(pt, "kappa", None)
                ))
            # 提取 lane_id 用于场景检测
            raw_lane_ids = MessageAccessor.get_field(msg, "lane_id", [])
            lane_ids = [str(lid) for lid in raw_lane_ids] if raw_lane_ids else []
            return TrajectoryMsg(path_point=path_points, lane_id=lane_ids)
        
        elif topic == Topics.PERCEPTION_OBSTACLES:
            raw_obstacles = MessageAccessor.get_field(msg, "obstacles", [])
            obstacles = []
            for obs in raw_obstacles:
                type_history = []
                raw_history = MessageAccessor.get_field(obs, "type_history", [])
                for th in raw_history[:1]:  # 只取第一个
                    type_history.append(TypeHistory(
                        type=MessageAccessor.get_field(th, "type", 0)
                    ))
                
                obstacles.append(Obstacle(
                    track_id=MessageAccessor.get_field(obs, "track_id", 0),
                    position=ObstaclePosition(
                        x=MessageAccessor.get_field(obs, "position.x", None),
                        y=MessageAccessor.get_field(obs, "position.y", None)
                    ),
                    velocity=ObstacleVelocity(
                        x=MessageAccessor.get_field(obs, "velocity.x", None),
                        y=MessageAccessor.get_field(obs, "velocity.y", None)
                    ),
                    length=MessageAccessor.get_field(obs, "length", None),
                    width=MessageAccessor.get_field(obs, "width", None),
                    heading_to_ego=MessageAccessor.get_field(obs, "heading_to_ego", None),
                    motion_status=MessageAccessor.get_field(obs, "motion_status", 0),
                    type_history=type_history
                ))
            return ObstacleListMsg(obstacles=obstacles)
        
        elif topic == Topics.CONTROL_DEBUG:
            return ControlDebugMsg(
                reserved0=MessageAccessor.get_field(msg, "reserved0", b'')
            )
        
        elif topic == Topics.CONTROL:
            return ControlMsg(
                chassis_control=ChassisControl(
                    target_longitudinal_acceleration=MessageAccessor.get_field(
                        msg, "chassis_control.target_longitudinal_acceleration", None)
                )
            )
        
        elif topic == Topics.PLANNING_DEBUG:
            return PlanningDebugMsg(
                data=MessageAccessor.get_field(msg, "data", b'')
            )
        
        elif topic == Topics.MAP:
            raw_lanes = MessageAccessor.get_field(msg, "lanes", [])
            lanes = []
            for lane in raw_lanes:
                lanes.append(MapLane(
                    id=str(MessageAccessor.get_field(lane, "id", "")),
                    turn=MessageAccessor.get_field(lane, "turn", 1),
                    junction_id=MessageAccessor.get_field(lane, "junction_id", None) or None,
                    lane_type=MessageAccessor.get_field(lane, "type", 0)
                ))
            return MapMsg(lanes=lanes)
        
        else:
            # 未知 topic，返回 None（保留原消息）
            return None
            
    except Exception as e:
        # 提取失败，返回 None
        return None

