"""
路口场景检测器

基于 /map/map 的 lanes 信息和 /planning/trajectory 的 lane_id 判断当前场景。

场景分类：
- 非路口 (NON_JUNCTION)
- 路口直行 (JUNCTION_STRAIGHT)
- 路口左转 (JUNCTION_LEFT)
- 路口右转 (JUNCTION_RIGHT)
- 路口掉头 (JUNCTION_UTURN)
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple
from enum import Enum
import bisect


class SceneType(Enum):
    """场景类型枚举"""
    NON_JUNCTION = "非路口"
    JUNCTION_STRAIGHT = "路口直行"
    JUNCTION_LEFT = "路口左转"
    JUNCTION_RIGHT = "路口右转"
    JUNCTION_UTURN = "路口掉头"
    UNKNOWN = "未知"


@dataclass
class LaneInfo:
    """车道信息"""
    lane_id: str
    turn: int  # 1=直行, 2=左转, 3=右转, 4=掉头
    junction_id: Optional[str] = None
    lane_type: int = 0
    
    @property
    def in_junction(self) -> bool:
        return bool(self.junction_id)
    
    @property
    def turn_name(self) -> str:
        return {1: "直行", 2: "左转", 3: "右转", 4: "掉头"}.get(self.turn, "未知")
    
    @property
    def scene_type(self) -> SceneType:
        if not self.in_junction:
            return SceneType.NON_JUNCTION
        if self.turn == 1:
            return SceneType.JUNCTION_STRAIGHT
        elif self.turn == 2:
            return SceneType.JUNCTION_LEFT
        elif self.turn == 3:
            return SceneType.JUNCTION_RIGHT
        elif self.turn == 4:
            return SceneType.JUNCTION_UTURN
        return SceneType.UNKNOWN


@dataclass
class SceneInfo:
    """场景检测结果"""
    scene_type: SceneType = SceneType.UNKNOWN
    in_junction: bool = False
    junction_id: Optional[str] = None
    turn_type: Optional[int] = None  # 1=直行, 2=左转, 3=右转, 4=掉头
    turn_name: Optional[str] = None
    lane_id: Optional[str] = None
    
    def to_dict(self) -> Dict:
        return {
            'scene': self.scene_type.value,
            'in_junction': self.in_junction,
            'junction_id': self.junction_id,
            'turn_type': self.turn_type,
            'turn_name': self.turn_name,
            'lane_id': self.lane_id
        }


@dataclass
class JunctionInterval:
    """路口通行区间"""
    junction_id: str
    enter_time: float
    exit_time: float
    turn_type: int  # 1=直行, 2=左转, 3=右转, 4=掉头
    turn_name: str
    lane_id: str
    passed: bool = True  # 是否通过（无异常）
    failure_reasons: List[str] = field(default_factory=list)  # 失败原因
    
    @property
    def duration(self) -> float:
        return self.exit_time - self.enter_time
    
    def to_dict(self) -> Dict:
        from datetime import datetime
        return {
            'junction_id': self.junction_id,
            'enter_time': self.enter_time,
            'enter_time_str': datetime.fromtimestamp(self.enter_time).strftime('%Y-%m-%d %H:%M:%S'),
            'exit_time': self.exit_time,
            'exit_time_str': datetime.fromtimestamp(self.exit_time).strftime('%Y-%m-%d %H:%M:%S'),
            'duration': round(self.duration, 2),
            'turn_type': self.turn_type,
            'turn_name': self.turn_name,
            'lane_id': self.lane_id,
            'passed': self.passed,
            'failure_reasons': self.failure_reasons
        }


class JunctionSceneDetector:
    """
    路口场景检测器
    
    使用方式（时间序列数据）：
    1. 调用 load_map_timeseries() 加载按时间戳存储的地图数据
    2. 调用 detect_at_time() 根据时间戳和 lane_id 判断场景
    
    使用方式（传统方式）：
    1. 调用 load_map() 加载单个地图消息
    2. 调用 detect() 根据 trajectory.lane_id 判断当前场景
    """
    
    TURN_NAMES = {1: "直行", 2: "左转", 3: "右转", 4: "掉头"}
    
    def __init__(self):
        self.lane_info: Dict[str, LaneInfo] = {}
        self._map_loaded = False
        # 时间序列数据: [(timestamp, lanes_dict), ...] 按时间排序
        self._map_timeseries: List[Tuple[float, Dict[str, Dict]]] = []
        self._timeseries_timestamps: List[float] = []  # 用于二分查找
    
    @property
    def is_loaded(self) -> bool:
        return self._map_loaded or len(self._map_timeseries) > 0
    
    def load_map_timeseries(self, map_lanes_data: List[Tuple[float, Dict[str, Dict]]]) -> int:
        """
        加载按时间戳存储的地图数据
        
        Args:
            map_lanes_data: [(timestamp, {lane_id: lane_info_dict, ...}), ...]
            
        Returns:
            时间戳帧数
        """
        self._map_timeseries = sorted(map_lanes_data, key=lambda x: x[0])
        self._timeseries_timestamps = [ts for ts, _ in self._map_timeseries]
        return len(self._map_timeseries)
    
    def detect_at_time(self, timestamp: float, lane_ids: List[str]) -> SceneInfo:
        """
        根据时间戳找到对应的 map 数据，然后判断场景
        
        Args:
            timestamp: 事件发生的时间戳
            lane_ids: trajectory.lane_id 列表
        
        Returns:
            SceneInfo 场景信息
        """
        if not self._map_timeseries:
            return SceneInfo(scene_type=SceneType.UNKNOWN)
        
        # 获取当前 lane_id（过滤空值后第 2 个）
        valid_ids = [lid for lid in lane_ids if lid]
        
        if len(valid_ids) >= 2:
            current_lane_id = valid_ids[1]  # 第二个是当前车道
        elif len(valid_ids) == 1:
            current_lane_id = valid_ids[0]
        else:
            return SceneInfo(scene_type=SceneType.UNKNOWN)
        
        # 二分查找最近的时间戳
        idx = bisect.bisect_right(self._timeseries_timestamps, timestamp)
        if idx == 0:
            lanes_dict = self._map_timeseries[0][1]
        elif idx >= len(self._map_timeseries):
            lanes_dict = self._map_timeseries[-1][1]
        else:
            # 选择更接近的时间戳
            t_before = self._timeseries_timestamps[idx - 1]
            t_after = self._timeseries_timestamps[idx]
            if abs(timestamp - t_before) <= abs(timestamp - t_after):
                lanes_dict = self._map_timeseries[idx - 1][1]
            else:
                lanes_dict = self._map_timeseries[idx][1]
        
        # 在该时刻的 lanes 中查找 lane 信息
        if current_lane_id not in lanes_dict:
            return SceneInfo(scene_type=SceneType.UNKNOWN, lane_id=current_lane_id)
        
        lane_data = lanes_dict[current_lane_id]
        
        # 构建 LaneInfo 并返回结果
        info = LaneInfo(
            lane_id=current_lane_id,
            turn=lane_data.get('turn', 1),
            junction_id=lane_data.get('junction_id'),
            lane_type=lane_data.get('type', 0)
        )
        
        return SceneInfo(
            scene_type=info.scene_type,
            in_junction=info.in_junction,
            junction_id=info.junction_id,
            turn_type=info.turn,
            turn_name=info.turn_name,
            lane_id=current_lane_id
        )
    
    def load_map(self, map_msg: Any) -> int:
        """
        从 /map/map 消息加载地图数据（传统方式）
        
        Args:
            map_msg: hv_map_msgs/msg/Map 消息对象
            
        Returns:
            加载的 lane 数量
        """
        self.lane_info.clear()
        
        if not hasattr(map_msg, 'lanes'):
            print(f"  [WARN] map_msg 没有 lanes 字段")
            return 0
        
        for lane in map_msg.lanes:
            lane_id = str(lane.id)
            junction_id = lane.junction_id if lane.junction_id else None
            
            self.lane_info[lane_id] = LaneInfo(
                lane_id=lane_id,
                turn=lane.turn,
                junction_id=junction_id,
                lane_type=lane.type if hasattr(lane, 'type') else 0
            )
        
        self._map_loaded = True
        return len(self.lane_info)
    
    def load_from_cache(self, lanes_data: List[Dict]) -> int:
        """
        从缓存数据加载（传统方式）
        
        Args:
            lanes_data: [{'lane_id': str, 'turn': int, 'junction_id': str, 'type': int}, ...]
        """
        self.lane_info.clear()
        
        for item in lanes_data:
            lane_id = item['lane_id']
            self.lane_info[lane_id] = LaneInfo(
                lane_id=lane_id,
                turn=item.get('turn', 1),
                junction_id=item.get('junction_id'),
                lane_type=item.get('type', 0)
            )
        
        self._map_loaded = True
        return len(self.lane_info)
    
    def export_for_cache(self) -> List[Dict]:
        """导出用于缓存的数据"""
        return [
            {
                'lane_id': info.lane_id,
                'turn': info.turn,
                'junction_id': info.junction_id,
                'type': info.lane_type
            }
            for info in self.lane_info.values()
        ]
    
    def detect(self, lane_ids: List[str]) -> SceneInfo:
        """
        根据 trajectory.lane_id 判断当前场景（传统方式，需先 load_map）
        
        Args:
            lane_ids: /planning/trajectory.lane_id 列表
                     规则：过滤空值后，第1个=前车道，第2个=当前车道，第3个=下车道
        
        Returns:
            SceneInfo 场景信息
        """
        if not self._map_loaded:
            return SceneInfo(scene_type=SceneType.UNKNOWN)
        
        # 获取当前 lane_id（过滤空值后第 2 个）
        valid_ids = [lid for lid in lane_ids if lid]
        
        if len(valid_ids) >= 2:
            current_lane_id = valid_ids[1]  # 第二个是当前车道
        elif len(valid_ids) == 1:
            current_lane_id = valid_ids[0]
        else:
            return SceneInfo(scene_type=SceneType.UNKNOWN)
        
        # 查找 lane 信息
        if current_lane_id not in self.lane_info:
            return SceneInfo(scene_type=SceneType.UNKNOWN, lane_id=current_lane_id)
        
        info = self.lane_info[current_lane_id]
        
        return SceneInfo(
            scene_type=info.scene_type,
            in_junction=info.in_junction,
            junction_id=info.junction_id,
            turn_type=info.turn,
            turn_name=info.turn_name,
            lane_id=current_lane_id
        )
    
    def detect_by_lane_id(self, lane_id: str) -> SceneInfo:
        """
        根据单个 lane_id 判断场景（传统方式）
        
        Args:
            lane_id: 车道 ID
        """
        if not self._map_loaded or not lane_id:
            return SceneInfo(scene_type=SceneType.UNKNOWN)
        
        if lane_id not in self.lane_info:
            return SceneInfo(scene_type=SceneType.UNKNOWN, lane_id=lane_id)
        
        info = self.lane_info[lane_id]
        
        return SceneInfo(
            scene_type=info.scene_type,
            in_junction=info.in_junction,
            junction_id=info.junction_id,
            turn_type=info.turn,
            turn_name=info.turn_name,
            lane_id=lane_id
        )
    
    def get_scene_stats(self) -> Dict:
        """获取 lane 场景统计"""
        if self._map_timeseries:
            # 时间序列模式：统计所有时刻的 lane
            all_lanes = {}
            for _, lanes_dict in self._map_timeseries:
                for lane_id, lane_data in lanes_dict.items():
                    if lane_id not in all_lanes:
                        all_lanes[lane_id] = lane_data
            
            stats = {
                'total_lanes': len(all_lanes),
                'junction_lanes': 0,
                'non_junction_lanes': 0,
                'by_turn': {1: 0, 2: 0, 3: 0, 4: 0},
                'map_frames': len(self._map_timeseries)
            }
            
            for lane_data in all_lanes.values():
                if lane_data.get('junction_id'):
                    stats['junction_lanes'] += 1
                else:
                    stats['non_junction_lanes'] += 1
                
                turn = lane_data.get('turn', 1)
                if turn in stats['by_turn']:
                    stats['by_turn'][turn] += 1
            
            return stats
        else:
            # 传统模式
            stats = {
                'total_lanes': len(self.lane_info),
                'junction_lanes': 0,
                'non_junction_lanes': 0,
                'by_turn': {1: 0, 2: 0, 3: 0, 4: 0}
            }
            
            for info in self.lane_info.values():
                if info.in_junction:
                    stats['junction_lanes'] += 1
                else:
                    stats['non_junction_lanes'] += 1
                
                if info.turn in stats['by_turn']:
                    stats['by_turn'][info.turn] += 1
            
            return stats
    
    def extract_junction_intervals(
        self,
        trajectory_lane_ids: List[Tuple[List[str], float]]
    ) -> List[JunctionInterval]:
        """
        从 trajectory lane_id 序列中提取所有路口通行区间
        
        Args:
            trajectory_lane_ids: [(lane_ids_list, timestamp), ...] 按时间排序
            
        Returns:
            JunctionInterval 列表
        """
        if not self._map_timeseries or not trajectory_lane_ids:
            return []
        
        intervals = []
        current_junction = None  # 当前正在通行的路口
        
        for lane_ids, timestamp in trajectory_lane_ids:
            scene_info = self.detect_at_time(timestamp, lane_ids)
            
            if scene_info.in_junction:
                # 在路口中
                if current_junction is None:
                    # 新进入路口
                    current_junction = {
                        'junction_id': scene_info.junction_id,
                        'enter_time': timestamp,
                        'turn_type': scene_info.turn_type,
                        'turn_name': scene_info.turn_name or self.TURN_NAMES.get(scene_info.turn_type, "未知"),
                        'lane_id': scene_info.lane_id
                    }
                elif current_junction['junction_id'] != scene_info.junction_id:
                    # 换到了另一个路口，先结束当前路口
                    intervals.append(JunctionInterval(
                        junction_id=current_junction['junction_id'],
                        enter_time=current_junction['enter_time'],
                        exit_time=timestamp,
                        turn_type=current_junction['turn_type'] or 1,
                        turn_name=current_junction['turn_name'],
                        lane_id=current_junction['lane_id'] or ""
                    ))
                    # 开始新路口
                    current_junction = {
                        'junction_id': scene_info.junction_id,
                        'enter_time': timestamp,
                        'turn_type': scene_info.turn_type,
                        'turn_name': scene_info.turn_name or self.TURN_NAMES.get(scene_info.turn_type, "未知"),
                        'lane_id': scene_info.lane_id
                    }
                # 更新转向类型（可能在路口内变化）
                elif scene_info.turn_type and scene_info.turn_type != 1:
                    # 优先记录非直行的转向类型
                    current_junction['turn_type'] = scene_info.turn_type
                    current_junction['turn_name'] = scene_info.turn_name or self.TURN_NAMES.get(scene_info.turn_type, "未知")
            else:
                # 不在路口中
                if current_junction is not None:
                    # 刚离开路口
                    intervals.append(JunctionInterval(
                        junction_id=current_junction['junction_id'],
                        enter_time=current_junction['enter_time'],
                        exit_time=timestamp,
                        turn_type=current_junction['turn_type'] or 1,
                        turn_name=current_junction['turn_name'],
                        lane_id=current_junction['lane_id'] or ""
                    ))
                    current_junction = None
        
        # 处理末尾未结束的路口
        if current_junction is not None and trajectory_lane_ids:
            last_timestamp = trajectory_lane_ids[-1][1]
            intervals.append(JunctionInterval(
                junction_id=current_junction['junction_id'],
                enter_time=current_junction['enter_time'],
                exit_time=last_timestamp,
                turn_type=current_junction['turn_type'] or 1,
                turn_name=current_junction['turn_name'],
                lane_id=current_junction['lane_id'] or ""
            ))
        
        return intervals
    
    def compute_junction_pass_rate(
        self,
        intervals: List[JunctionInterval],
        anomalies: List[Dict],
        failure_types: List[str] = None
    ) -> Dict:
        """
        计算路口通过率
        
        Args:
            intervals: 路口区间列表
            anomalies: 异常列表 [{'timestamp': float, 'description': str, ...}, ...]
            failure_types: 视为失败的异常类型关键词，默认 ['接管', '急刹', '急减速', '顿挫']
            
        Returns:
            统计结果字典
        """
        if failure_types is None:
            failure_types = ['接管', '急刹', '急减速', '顿挫']
        
        # 按转向类型分组统计
        stats = {
            'total': {'total': 0, 'passed': 0, 'failed': 0},
            '直行': {'total': 0, 'passed': 0, 'failed': 0},
            '左转': {'total': 0, 'passed': 0, 'failed': 0},
            '右转': {'total': 0, 'passed': 0, 'failed': 0},
            '掉头': {'total': 0, 'passed': 0, 'failed': 0},
        }
        
        failed_intervals = []
        
        for interval in intervals:
            # 检查该区间内是否有失败类型的异常
            failures = []
            for anomaly in anomalies:
                ts = anomaly.get('timestamp', 0)
                # 检查时间是否在区间内（允许一点误差）
                if interval.enter_time - 0.5 <= ts <= interval.exit_time + 0.5:
                    desc = anomaly.get('description', '')
                    for fail_type in failure_types:
                        if fail_type in desc:
                            failures.append(f"{fail_type}@{anomaly.get('time', '')}")
                            break
            
            interval.passed = len(failures) == 0
            interval.failure_reasons = failures
            
            # 更新统计
            turn_name = interval.turn_name if interval.turn_name in stats else '直行'
            stats['total']['total'] += 1
            stats[turn_name]['total'] += 1
            
            if interval.passed:
                stats['total']['passed'] += 1
                stats[turn_name]['passed'] += 1
            else:
                stats['total']['failed'] += 1
                stats[turn_name]['failed'] += 1
                failed_intervals.append(interval)
        
        # 计算通过率
        result = {
            'summary': {},
            'by_turn': {},
            'failed_intervals': [i.to_dict() for i in failed_intervals],
            'all_intervals': [i.to_dict() for i in intervals]
        }
        
        for key, data in stats.items():
            total = data['total']
            passed = data['passed']
            rate = (passed / total * 100) if total > 0 else 0
            
            if key == 'total':
                result['summary'] = {
                    'total_junctions': total,
                    'passed': passed,
                    'failed': data['failed'],
                    'pass_rate': round(rate, 2)
                }
            else:
                if total > 0:  # 只记录有数据的转向类型
                    result['by_turn'][key] = {
                        'total': total,
                        'passed': passed,
                        'failed': data['failed'],
                        'pass_rate': round(rate, 2)
                    }
        
        return result