"""
ROS2 Bag Reader Module
用于读取ROS2 bag文件并提取指定topic的数据
"""
import os
from typing import Dict, List, Optional, Any, Generator, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import numpy as np
import yaml

from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore, get_types_from_msg
from tqdm import tqdm

# ============================================================================
# Monkey patch: 修复 rosbags 对 'cdr' 编码格式的支持
# rosbags 0.11.0 在 storage_sqlite3.py 第94行有断言 assert typ['encoding'] == 'ros2msg'
# 但 ROS2 bag 文件可能使用 'cdr' 编码（这是 ROS2 的标准序列化格式）
# 我们通过替换 get_types_from_msg 来绕过这个断言
# ============================================================================
try:
    from rosbags.rosbag2 import storage_sqlite3
    from rosbags.typesys.msg import get_types_from_msg as _original_get_types
    
    # 保存原始的 open 方法
    _original_sqlite3_open = storage_sqlite3.Sqlite3Reader.open
    
    def _patched_sqlite3_open(self):
        """Patched open method to support 'cdr' encoding in message_definitions"""
        import sqlite3
        from typing import cast
        from rosbags.interfaces import Connection, MessageDefinition, MessageDefinitionFormat, ConnectionExtRosbag2
        from rosbags.typesys.store import Typestore
        from rosbags.rosbag2.metadata import parse_qos
        
        conn = sqlite3.connect(f'file:{self.path}?immutable=1', uri=True)
        conn.row_factory = lambda _, x: x
        cur = conn.cursor()
        _ = cur.execute(
            'SELECT count(*) FROM sqlite_master WHERE type="table" AND name IN ("messages", "topics")'
        )
        if cur.fetchone()[0] != 2:
            conn.close()
            raise Exception(f'Cannot open database {self.path}')
        
        self.dbconn = conn
        
        cur = conn.cursor()
        if cur.execute('PRAGMA table_info(schema)').fetchall():
            (schema,) = cur.execute('SELECT schema_version FROM schema').fetchone()
        elif any(x[1] == 'offered_qos_profiles' for x in cur.execute('PRAGMA table_info(topics)')):
            schema = 2
        else:
            schema = 1
        
        self.schema = schema
        
        if schema >= 4:
            msgtypes = [
                {'name': x[0], 'encoding': x[1], 'msgdef': x[2], 'digest': x[3]}
                for x in cur.execute(
                    'SELECT topic_type, encoding, encoded_message_definition, type_description_hash '
                    'FROM message_definitions ORDER BY id'
                )
            ]
            # 修复：允许 'cdr' / 空字符串 编码，将其视为 'ros2msg' 处理
            # 某些工具生成的 bag 文件可能 encoding 字段为空
            for typ in msgtypes:
                if typ['encoding'] not in ('ros2msg', 'cdr', ''):
                    raise Exception(f"Unsupported encoding: {typ['encoding']}")
                # 跳过验证：cdr 或空编码没有有效的 msgdef
                if typ['encoding'] in ('cdr', '') or not typ['msgdef']:
                    continue
                try:
                    types = _original_get_types(typ['msgdef'], typ['name'])
                    store = Typestore()
                    store.register(types)
                except Exception:
                    pass  # 忽略解析错误
        else:
            msgtypes = []
        
        self.msgtypes = msgtypes
        
        def get_msgdef(name):
            fmtmap = {'ros2msg': MessageDefinitionFormat.MSG, 'ros2idl': MessageDefinitionFormat.IDL, 'cdr': MessageDefinitionFormat.MSG}
            if msgtype := next((x for x in msgtypes if x['name'] == name), None):
                return MessageDefinition(fmtmap.get(msgtype['encoding'], MessageDefinitionFormat.NONE), msgtype.get('msgdef', ''))
            return MessageDefinition(MessageDefinitionFormat.NONE, '')
        
        # Connection 签名: (id, topic, msgtype, msgdef, digest, msgcount, ext, owner)
        if schema >= 4:
            self.connections = [
                Connection(
                    cid, topic, msgtype,
                    get_msgdef(msgtype),
                    '',  # digest
                    msgcount,
                    ConnectionExtRosbag2(serialization_format, parse_qos(qos)),
                    None,  # owner
                )
                for cid, topic, msgtype, serialization_format, qos, msgcount in cur.execute(
                    'SELECT topics.id, name, type, serialization_format, offered_qos_profiles, count(*) '
                    'FROM topics LEFT JOIN messages ON topics.id = messages.topic_id GROUP BY topics.id ORDER BY topics.id'
                )
            ]
        elif schema >= 2:
            self.connections = [
                Connection(
                    cid, topic, msgtype,
                    get_msgdef(msgtype),
                    '',  # digest
                    msgcount,
                    ConnectionExtRosbag2(serialization_format, parse_qos(qos)),
                    None,  # owner
                )
                for cid, topic, msgtype, serialization_format, qos, msgcount in cur.execute(
                    'SELECT topics.id, name, type, serialization_format, offered_qos_profiles, count(*) '
                    'FROM topics LEFT JOIN messages ON topics.id = messages.topic_id GROUP BY topics.id ORDER BY topics.id'
                )
            ]
        else:
            self.connections = [
                Connection(
                    cid, topic, msgtype,
                    get_msgdef(msgtype),
                    '',  # digest
                    msgcount,
                    ConnectionExtRosbag2(serialization_format, []),
                    None,  # owner
                )
                for cid, topic, msgtype, serialization_format, msgcount in cur.execute(
                    'SELECT topics.id, name, type, serialization_format, count(*) '
                    'FROM topics LEFT JOIN messages ON topics.id = messages.topic_id GROUP BY topics.id ORDER BY topics.id'
                )
            ]
        
        # Get metadata
        rows = list(cur.execute('SELECT timestamp FROM messages ORDER BY timestamp LIMIT 1'))
        start = rows[0][0] if rows else 0
        rows = list(cur.execute('SELECT timestamp FROM messages ORDER BY timestamp DESC LIMIT 1'))
        end = rows[0][0] if rows else 0
        (count,) = cur.execute('SELECT count(*) FROM messages').fetchone()
        
        from rosbags.rosbag2.metadata import ReaderMetadata
        self.metadata = ReaderMetadata(start, end, count, 0, None, None, None, None)
    
    storage_sqlite3.Sqlite3Reader.open = _patched_sqlite3_open
    
except Exception as e:
    pass  # 如果补丁失败，继续使用原始实现

# 尝试导入ROS2序列化模块和自定义消息类型
RCLPY_AVAILABLE = False
HV_MSG_CLASSES = {}

try:
    from rclpy.serialization import deserialize_message
    RCLPY_AVAILABLE = True
except ImportError:
    deserialize_message = None

# 尝试导入自定义ROS2消息类型
try:
    from hv_vehicle_io_msgs.msg import ChassisDomain
    HV_MSG_CLASSES['hv_vehicle_io_msgs/msg/ChassisDomain'] = ChassisDomain
except ImportError:
    pass

try:
    from hv_localization_msgs.msg import Localization
    HV_MSG_CLASSES['hv_localization_msgs/msg/Localization'] = Localization
except ImportError:
    pass

try:
    from hv_perception_msgs.msg import ObstacleList
    HV_MSG_CLASSES['hv_perception_msgs/msg/ObstacleList'] = ObstacleList
except ImportError:
    pass

try:
    from hv_function_manager_msgs.msg import FunctionManager
    HV_MSG_CLASSES['hv_function_manager_msgs/msg/FunctionManager'] = FunctionManager
except ImportError:
    pass

try:
    from hv_control_msgs.msg import ControlDebug ,Control ,ChassisControl
    HV_MSG_CLASSES['hv_control_msgs/msg/ControlDebug'] = ControlDebug
    HV_MSG_CLASSES['hv_control_msgs/msg/Control'] = Control
    HV_MSG_CLASSES['hv_control_msgs/msg/ChassisControl'] = ChassisControl
except ImportError:
    pass

try:
    from hv_planning_msgs.msg import Trajectory, PathPoint
    HV_MSG_CLASSES['hv_planning_msgs/msg/Trajectory'] = Trajectory
    HV_MSG_CLASSES['hv_planning_msgs/msg/PathPoint'] = PathPoint
except ImportError as e:
    print(f"Error importing hv_planning_msgs/msg/Trajectory or hv_planning_msgs/msg/PathPoint: {e}")
    pass

try:
    from hv_planning_msgs.msg import PlanningDebug
    HV_MSG_CLASSES['hv_planning_msgs/msg/PlanningDebug'] = PlanningDebug
except ImportError:
    pass

try:
    from hv_map_msgs.msg import Map
    HV_MSG_CLASSES['hv_map_msgs/msg/Map'] = Map
except ImportError:
    pass

if HV_MSG_CLASSES:
    print(f"已加载 {len(HV_MSG_CLASSES)} 个自定义HV消息类型")


@dataclass
class TopicData:
    """存储单个topic的数据"""
    topic_name: str
    timestamps: List[float] = field(default_factory=list)
    messages: List[Any] = field(default_factory=list)
    
    def append(self, timestamp: float, message: Any):
        self.timestamps.append(timestamp)
        self.messages.append(message)
    
    def extend(self, other: 'TopicData'):
        """合并另一个 TopicData"""
        self.timestamps.extend(other.timestamps)
        self.messages.extend(other.messages)
    
    def clear(self):
        """清空数据释放内存"""
        self.timestamps.clear()
        self.messages.clear()
    
    def to_dict(self) -> Dict:
        return {
            'topic_name': self.topic_name,
            'timestamps': np.array(self.timestamps),
            'messages': self.messages,
            'count': len(self.messages)
        }
    
    def __len__(self) -> int:
        return len(self.messages)


class BagReader:
    """ROS2 Bag文件读取器"""
    
    def __init__(self, bag_path: str):
        """
        初始化Bag读取器
        
        Args:
            bag_path: ROS2 bag文件夹路径 或 单个 .db3 文件路径
        """
        self.bag_path = Path(bag_path)
        if not self.bag_path.exists():
            raise FileNotFoundError(f"Bag path not found: {bag_path}")
        
        # 如果是单个 .db3 文件，需要特殊处理
        self._single_db3_mode = False
        self._single_db3_file = None
        self._single_db3_time_range = None  # (start_ns, end_ns) 用于过滤消息
        if self.bag_path.is_file() and self.bag_path.suffix == '.db3':
            self._single_db3_mode = True
            self._single_db3_file = self.bag_path
            self.bag_path = self.bag_path.parent
            # 获取该 db3 文件的时间范围
            self._single_db3_time_range = self._get_db3_time_range(self._single_db3_file)
        
        # 检查并生成缺失的 metadata.yaml
        self._ensure_metadata_exists()
        
        self._reader: Optional[Reader] = None
        self._topic_types: Dict[str, str] = {}
        self._available_topics: List[str] = []
        self._typestore = None
        self._types_registered = False
    
    def _get_db3_time_range(self, db3_file: Path) -> tuple:
        """
        获取单个 db3 文件的时间范围（纳秒）
        """
        import sqlite3
        try:
            conn = sqlite3.connect(str(db3_file))
            cursor = conn.cursor()
            cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages")
            row = cursor.fetchone()
            conn.close()
            if row[0] is not None and row[1] is not None:
                return (row[0], row[1])
        except Exception as e:
            print(f"  [WARN] 读取 {db3_file.name} 时间范围失败: {e}")
        return None
    
    def _ensure_metadata_exists(self):
        """
        确保 metadata.yaml 存在且有效
        如果目录中只有 .db3 文件而没有 metadata.yaml（或文件为空），则自动生成一个
        会从 db3 文件中读取 topics 信息
        """
        metadata_path = self.bag_path / "metadata.yaml"
        
        # 检查文件是否存在且非空
        if metadata_path.exists():
            if metadata_path.stat().st_size > 0:
                return  # 已存在且非空，无需生成
            else:
                print(f"  [WARN] metadata.yaml 存在但为空，将重新生成...")
        
        # 查找目录中的 .db3 文件
        db3_files = sorted(self.bag_path.glob("*.db3"))
        if not db3_files:
            raise FileNotFoundError(f"No .db3 files found in {self.bag_path}")
        
        print(f"  [INFO] 未找到 metadata.yaml，正在自动生成...")
        print(f"  [INFO] 检测到 {len(db3_files)} 个 .db3 文件")
        
        # 生成相对路径列表
        relative_paths = [f.name for f in db3_files]
        
        # 从 db3 文件读取 topics 和统计信息
        import sqlite3
        topics_info = {}  # topic_name -> {type, count, qos}
        total_message_count = 0
        min_timestamp = None
        max_timestamp = None
        
        for db3_file in db3_files:
            try:
                conn = sqlite3.connect(str(db3_file))
                cursor = conn.cursor()
                
                # 读取 topics
                cursor.execute("SELECT id, name, type, serialization_format, offered_qos_profiles FROM topics")
                topic_rows = cursor.fetchall()
                topic_id_map = {}
                
                for row in topic_rows:
                    topic_id, name, msg_type, serialization, qos = row
                    topic_id_map[topic_id] = name
                    if name not in topics_info:
                        topics_info[name] = {
                            'type': msg_type,
                            'serialization_format': serialization,
                            'offered_qos_profiles': qos,
                            'message_count': 0
                        }
                
                # 统计每个 topic 的消息数量
                cursor.execute("SELECT topic_id, COUNT(*) FROM messages GROUP BY topic_id")
                for topic_id, count in cursor.fetchall():
                    if topic_id in topic_id_map:
                        topics_info[topic_id_map[topic_id]]['message_count'] += count
                        total_message_count += count
                
                # 获取时间范围
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages")
                row = cursor.fetchone()
                if row[0] is not None:
                    if min_timestamp is None or row[0] < min_timestamp:
                        min_timestamp = row[0]
                    if max_timestamp is None or row[1] > max_timestamp:
                        max_timestamp = row[1]
                
                conn.close()
            except Exception as e:
                print(f"  [WARN] 读取 {db3_file.name} 信息失败: {e}")
        
        # 构建 topics_with_message_count 列表
        topics_with_message_count = []
        for name, info in sorted(topics_info.items()):
            topics_with_message_count.append({
                'topic_metadata': {
                    'name': name,
                    'type': info['type'],
                    'serialization_format': info['serialization_format'],
                    'offered_qos_profiles': info['offered_qos_profiles']
                },
                'message_count': info['message_count']
            })
        
        # 计算持续时间
        duration_ns = (max_timestamp - min_timestamp) if (min_timestamp and max_timestamp) else 0
        starting_time_ns = min_timestamp if min_timestamp else 0
        
        # 创建完整的 metadata.yaml 结构
        # 注意：版本号需要与 db3 文件的 schema_version 匹配
        metadata = {
            'rosbag2_bagfile_information': {
                'version': 3,
                'storage_identifier': 'sqlite3',
                'relative_file_paths': relative_paths,
                'duration': {
                    'nanoseconds': duration_ns
                },
                'starting_time': {
                    'nanoseconds_since_epoch': starting_time_ns
                },
                'message_count': total_message_count,
                'topics_with_message_count': topics_with_message_count,
                'compression_format': '',
                'compression_mode': ''
            }
        }
        
        # 写入 metadata.yaml
        with open(metadata_path, 'w', encoding='utf-8') as f:
            yaml.dump(metadata, f, default_flow_style=False, allow_unicode=True)
        
        print(f"  [INFO] 已生成 metadata.yaml: {metadata_path}")
        print(f"  [INFO] 包含 {len(topics_info)} 个 topics, {total_message_count:,} 条消息")
    
    def _should_include_message(self, timestamp_ns: int) -> bool:
        """
        检查消息是否应该被包含（用于单 db3 文件模式的时间过滤）
        """
        if not self._single_db3_mode or self._single_db3_time_range is None:
            return True
        start_ns, end_ns = self._single_db3_time_range
        return start_ns <= timestamp_ns <= end_ns
    
    def _deserialize_message(self, rawdata: bytes, msgtype: str) -> Any:
        """
        反序列化消息，优先使用rclpy原生方式
        
        Args:
            rawdata: 原始字节数据
            msgtype: 消息类型字符串
            
        Returns:
            反序列化后的消息对象
        """
        # 方法1: 使用rclpy原生反序列化 (如果消息类型可用)
        if RCLPY_AVAILABLE and msgtype in HV_MSG_CLASSES:
            msg_class = HV_MSG_CLASSES[msgtype]
            return deserialize_message(rawdata, msg_class)
        
        # 方法2: 使用rosbags typestore反序列化
        if self._typestore:
            return self._typestore.deserialize_cdr(rawdata, msgtype)
        
        raise ValueError(f"无法反序列化消息类型: {msgtype}")
    
    def _init_typestore(self, reader: Reader):
        """
        初始化类型存储并注册bag中的自定义消息类型
        """
        if self._types_registered:
            return
        
        # 创建基础typestore
        try:
            self._typestore = get_typestore(Stores.ROS2_HUMBLE)
        except:
            try:
                self._typestore = get_typestore(Stores.ROS2_FOXY)
            except:
                self._typestore = get_typestore(Stores.LATEST)
        
        # 从bag的connections中读取消息类型定义并注册
        custom_types = {}
        for conn in reader.connections:
            if conn.msgdef:
                try:
                    # 解析消息定义并注册
                    types = get_types_from_msg(conn.msgdef, conn.msgtype)
                    custom_types.update(types)
                except Exception as e:
                    # 忽略解析错误，继续处理其他类型
                    pass
        
        if custom_types:
            try:
                self._typestore.register(custom_types)
                print(f"  已注册 {len(custom_types)} 个自定义消息类型")
            except Exception as e:
                print(f"  Warning: 注册自定义类型时出错: {e}")
        
        self._types_registered = True
    
    def get_available_topics(self) -> List[str]:
        """获取bag中所有可用的topic列表"""
        with Reader(self.bag_path) as reader:
            self._init_typestore(reader)
            self._available_topics = [conn.topic for conn in reader.connections]
            self._topic_types = {conn.topic: conn.msgtype for conn in reader.connections}
        return self._available_topics
    
    def get_topic_info(self) -> Dict[str, Dict]:
        """获取所有topic的详细信息"""
        topic_info = {}
        with Reader(self.bag_path) as reader:
            self._init_typestore(reader)
            for conn in reader.connections:
                topic_info[conn.topic] = {
                    'type': conn.msgtype,
                    'count': conn.msgcount if hasattr(conn, 'msgcount') else 'unknown'
                }
        return topic_info
    
    def read_topics(self, topics: List[str], 
                    progress: bool = True,
                    time_range: Optional[Tuple[float, float]] = None,
                    sample_interval: Optional[float] = None,
                    light_mode: bool = False) -> Dict[str, TopicData]:
        """
        读取指定的多个topic数据
        
        Args:
            topics: 要读取的topic列表
            progress: 是否显示进度条
            time_range: 可选的时间范围过滤 (start_sec, end_sec)，只读取该范围内的消息
            sample_interval: 可选的采样间隔（秒），用于降采样减少内存占用
            light_mode: 轻量模式，只保留 KPI 计算需要的字段，大幅降低内存占用
            
        Returns:
            Dict[topic_name, TopicData]
        """
        # 导入轻量级消息转换函数
        if light_mode:
            from ..constants import extract_light_message
        
        result = {topic: TopicData(topic_name=topic) for topic in topics}
        
        # 用于采样的上次时间戳记录
        last_sample_time = {topic: -float('inf') for topic in topics}
        
        with Reader(self.bag_path) as reader:
            # 初始化类型存储
            self._init_typestore(reader)
            
            # 建立topic到connection的映射
            topic_connections = {}
            for conn in reader.connections:
                if conn.topic in topics:
                    topic_connections[conn.topic] = conn
            
            # 检查请求的topic是否存在
            missing_topics = set(topics) - set(topic_connections.keys())
            if missing_topics:
                print(f"Warning: Topics not found in bag: {missing_topics}")
            
            # 读取消息
            connections = [topic_connections[t] for t in topics if t in topic_connections]
            
            iterator = reader.messages(connections=connections)
            if progress:
                # 估算消息总数
                total = sum(conn.msgcount for conn in connections if hasattr(conn, 'msgcount'))
                iterator = tqdm(iterator, total=total if total > 0 else None, 
                              desc="Reading bag")
            
            skipped_time_range = 0
            skipped_sample = 0
            light_converted = 0
            
            for connection, timestamp, rawdata in iterator:
                # 单 db3 文件模式：按时间范围过滤
                if not self._should_include_message(timestamp):
                    continue
                
                # 转换时间戳为秒
                ts_sec = timestamp / 1e9
                
                # 时间范围过滤
                if time_range is not None:
                    if ts_sec < time_range[0] or ts_sec > time_range[1]:
                        skipped_time_range += 1
                        continue
                
                # 采样间隔过滤
                topic = connection.topic
                if sample_interval is not None:
                    if ts_sec - last_sample_time[topic] < sample_interval:
                        skipped_sample += 1
                        continue
                    last_sample_time[topic] = ts_sec
                    
                try:
                    msg = self._deserialize_message(rawdata, connection.msgtype)
                    
                    # 轻量模式：转换为轻量级消息
                    if light_mode:
                        light_msg = extract_light_message(topic, msg)
                        if light_msg is not None:
                            msg = light_msg
                            light_converted += 1
                    
                    result[topic].append(ts_sec, msg)
                except Exception as e:
                    print(f"Error deserializing message on {topic}: {e}")
                    continue
            
            # 打印过滤统计
            if progress:
                if skipped_time_range > 0:
                    print(f"  [过滤] 时间范围外跳过: {skipped_time_range} 条")
                if skipped_sample > 0:
                    print(f"  [过滤] 采样间隔跳过: {skipped_sample} 条")
                if light_mode and light_converted > 0:
                    print(f"  [轻量] 转换消息: {light_converted} 条")
        
        return result
    
    def read_topic_generator(self, topic: str) -> Generator:
        """
        以生成器方式读取单个topic (节省内存)
        
        Args:
            topic: topic名称
            
        Yields:
            (timestamp, message) 元组
        """
        with Reader(self.bag_path) as reader:
            # 初始化类型存储
            self._init_typestore(reader)
            
            conn = None
            for c in reader.connections:
                if c.topic == topic:
                    conn = c
                    break
            
            if conn is None:
                raise ValueError(f"Topic {topic} not found in bag")
            
            for connection, timestamp, rawdata in reader.messages(connections=[conn]):
                # 单 db3 文件模式：按时间范围过滤
                if not self._should_include_message(timestamp):
                    continue
                    
                try:
                    msg = self._deserialize_message(rawdata, connection.msgtype)
                    yield timestamp / 1e9, msg
                except Exception as e:
                    print(f"Error: {e}")
                    continue
    
    def get_time_range(self) -> tuple:
        """获取bag的时间范围"""
        # 单 db3 文件模式：返回该文件的时间范围
        if self._single_db3_mode and self._single_db3_time_range:
            start_ns, end_ns = self._single_db3_time_range
            return start_ns / 1e9, end_ns / 1e9
        
        try:
            with Reader(self.bag_path) as reader:
                start_time = reader.start_time / 1e9
                end_time = reader.end_time / 1e9
            return start_time, end_time
        except Exception as e:
            # 备选方案：直接从 db3 文件读取
            print(f"  [WARN] rosbags 读取失败，使用备选方案: {e}")
            return self._get_time_range_from_db3()
    
    def _get_time_range_from_db3(self) -> tuple:
        """直接从 db3 文件读取时间范围（备选方案）"""
        import sqlite3
        
        min_ts = None
        max_ts = None
        
        db3_files = sorted(self.bag_path.glob("*.db3"))
        for db3_file in db3_files:
            try:
                conn = sqlite3.connect(str(db3_file))
                cursor = conn.cursor()
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages")
                row = cursor.fetchone()
                conn.close()
                
                if row[0] is not None:
                    if min_ts is None or row[0] < min_ts:
                        min_ts = row[0]
                    if max_ts is None or row[1] > max_ts:
                        max_ts = row[1]
            except Exception as e:
                print(f"  [WARN] 读取 {db3_file.name} 时间范围失败: {e}")
        
        if min_ts is not None and max_ts is not None:
            return min_ts / 1e9, max_ts / 1e9
        return 0.0, 0.0
    
    def get_duration(self) -> float:
        """获取bag的时长(秒)"""
        start, end = self.get_time_range()
        return end - start
    
    def get_db3_file_infos(self) -> list:
        """
        获取每个 .db3 文件的时间范围信息
        用于异常溯源时精确定位到具体的 .db3 文件
        
        Returns:
            list of dict, 每个字典包含 path, start_time, end_time
        """
        import sqlite3
        
        db3_infos = []
        db3_files = sorted(self.bag_path.glob("*.db3"))
        
        for db3_file in db3_files:
            try:
                conn = sqlite3.connect(str(db3_file))
                cursor = conn.cursor()
                cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM messages")
                row = cursor.fetchone()
                conn.close()
                
                if row[0] is not None and row[1] is not None:
                    db3_infos.append({
                        'path': db3_file,
                        'start_time': row[0] / 1e9,
                        'end_time': row[1] / 1e9
                    })
            except Exception as e:
                print(f"  [WARN] 读取 {db3_file.name} 时间范围失败: {e}")
        
        # 按开始时间排序
        db3_infos.sort(key=lambda x: x['start_time'])
        return db3_infos


class MessageAccessor:
    """
    消息字段访问工具类
    支持点号分隔的嵌套字段访问
    """
    
    @staticmethod
    def get_field(msg: Any, field_path: str, default=None) -> Any:
        """
        获取消息中的嵌套字段值
        
        Args:
            msg: ROS消息对象
            field_path: 字段路径, 如 "header.global_timestamp"
            default: 默认值
            
        Returns:
            字段值或默认值
        """
        try:
            parts = field_path.split('.')
            value = msg
            for part in parts:
                # 处理数组索引
                if '[' in part:
                    name, idx = part.rstrip(']').split('[')
                    value = getattr(value, name)[int(idx)]
                else:
                    value = getattr(value, part)
            return value
        except (AttributeError, IndexError, TypeError):
            return default
    
    @staticmethod
    def get_timestamp(msg: Any) -> Optional[float]:
        """获取消息的global_timestamp"""
        return MessageAccessor.get_field(msg, "header.global_timestamp")


if __name__ == "__main__":
    # 简单测试
    import sys
    if len(sys.argv) > 1:
        bag_path = sys.argv[1]
        reader = BagReader(bag_path)
        print("Available topics:")
        for topic, info in reader.get_topic_info().items():
            print(f"  {topic}: {info}")
        print(f"\nTime range: {reader.get_time_range()}")
        print(f"Duration: {reader.get_duration():.2f}s")

