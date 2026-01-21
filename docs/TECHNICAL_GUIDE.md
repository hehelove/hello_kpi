# Hello KPI 技术指南

本文档详细介绍 Hello KPI 系统的完整分析流程、核心算法原理和使用的 Python 库。

---

## 目录

1. [系统架构概述](#1-系统架构概述)
2. [数据加载（Bag 解析）](#2-数据加载bag-解析)
3. [时间同步](#3-时间同步)
4. [高频数据收集](#4-高频数据收集)
5. [信号处理](#5-信号处理)
6. [KPI 计算框架](#6-kpi-计算框架)
7. [持久化缓存](#7-持久化缓存)
8. [报告生成](#8-报告生成)
9. [完整流程图](#9-完整流程图)
10. [关键库总结](#10-关键库总结)

---

## 1. 系统架构概述

整个系统采用**流水线架构**，数据依次经过：

```
ROS2 Bag → 数据加载 → 时间同步 → 高频数据收集 → KPI计算 → 报告生成
                ↓
           持久化缓存
```

### 1.1 核心依赖库

| 库 | 版本 | 用途 |
|---|---|---|
| `rosbags` | ≥0.9.12 | ROS2 bag 文件解析 |
| `numpy` | ≥1.24.0 | 数值计算、数组操作 |
| `scipy` | ≥1.11.0 | 信号处理（滤波、差分） |
| `pandas` | ≥2.0.0 | 数据分析（可选） |
| `pyproj` | ≥3.6.0 | 地理坐标转换 |
| `protobuf` | ≥4.24.0 | 解析嵌入的 proto 数据 |
| `pyyaml` | ≥6.0.1 | 配置文件解析 |
| `tabulate` | ≥0.9.0 | 报告表格格式化 |

### 1.2 目录结构

```
src/
├── main.py                    # 主程序入口
├── constants.py               # 常量定义
├── data_loader/
│   ├── bag_reader.py          # 单 bag 读取
│   ├── multi_bag_reader.py    # 多 bag 批量读取
│   └── frame_store.py         # 数据缓存层
├── sync/
│   └── time_sync.py           # 时间同步
├── kpi/
│   ├── base_kpi.py            # KPI 基类
│   ├── mileage.py             # 里程统计
│   ├── takeover.py            # 接管统计
│   ├── emergency.py           # 紧急事件检测
│   ├── comfort.py             # 舒适性
│   ├── steering.py            # 转向平顺性
│   ├── weaving.py             # 画龙检测
│   └── ...                    # 其他 KPI
├── utils/
│   ├── signal.py              # 信号处理
│   └── geo.py                 # 地理坐标处理
├── output/
│   └── reporter.py            # 报告生成
└── proto/
    ├── control_debug_parser.py    # 控制调试数据解析
    └── planning_debug_parser.py   # 规划调试数据解析
```

---

## 2. 数据加载（Bag 解析）

### 2.1 ROS2 Bag 文件格式

ROS2 bag 使用 **SQLite3 数据库**（`.db3` 文件）存储消息：
- 包含 `metadata.yaml` 描述 topic 信息
- 消息以 **CDR 序列化格式** 存储

### 2.2 rosbags 库工作原理

```python
from rosbags.rosbag2 import Reader
from rosbags.typesys import Stores, get_typestore

# 获取类型存储（包含标准 ROS2 消息定义）
typestore = get_typestore(Stores.ROS2_HUMBLE)

with Reader(bag_path) as reader:
    for connection, timestamp, rawdata in reader.messages():
        # connection.topic: topic 名称
        # timestamp: 纳秒级时间戳
        # rawdata: CDR 序列化的二进制数据
        
        # 反序列化为 Python 对象
        msg = typestore.deserialize_cdr(rawdata, connection.msgtype)
```

**关键点**：
1. `rosbags` 支持**惰性读取**，不会一次性加载所有数据
2. 消息类型通过 `typestore` 动态解析
3. 自定义消息（如 `hv_*_msgs`）需要额外注册

### 2.3 消息反序列化双重策略

```python
def _deserialize_message(self, rawdata: bytes, msgtype: str) -> Any:
    """
    反序列化消息，双重策略：
    
    1. rclpy 原生反序列化（优先）
       - 需要 source ROS2 环境
       - 支持自定义消息类型 (hv_*_msgs)
       - 性能更好，类型更完整
       
    2. rosbags typestore 反序列化（回退）
       - 纯 Python 实现，无需 ROS2 环境
       - 自动从 bag 元数据解析消息定义
       - 通用性更强
    """
    # 方法1: rclpy 原生
    if RCLPY_AVAILABLE and msgtype in HV_MSG_CLASSES:
        msg_class = HV_MSG_CLASSES[msgtype]
        return deserialize_message(rawdata, msg_class)
    
    # 方法2: rosbags typestore
    if self._typestore:
        return self._typestore.deserialize_cdr(rawdata, msgtype)
```

### 2.4 CDR 序列化格式

- CDR (Common Data Representation) 是 DDS 标准的序列化格式
- 二进制编码，支持大小端转换
- 结构化数据按字段顺序编码

---

## 3. 时间同步

### 3.1 问题背景

ROS2 中各 topic 异步发布，时间戳不对齐：

```
Topic A:  |--o--o--o--o--|  (10Hz)
Topic B:  |---o---o---o---|  (7Hz)
Topic C:  |----o----o----|  (5Hz)
          时间 →
```

需要将多个 topic 对齐到统一的时间基准。

### 3.2 同步算法原理

**核心思想**：以 `/function/function_manager`（10Hz）为基准，为每个基准帧在其他 topic 中找最近的消息。

### 3.3 二分查找算法

使用 Python 标准库 `bisect` 实现 O(log n) 时间复杂度的查找：

```python
from bisect import bisect_left

def _find_nearest_by_index(self, topic: str, target_time: float):
    """
    二分查找最近的消息
    
    原理：
    1. bisect_left 找到 target_time 应该插入的位置
    2. 比较插入点前后两个元素，取更近的那个
    3. 检查时间差是否在容差范围内（默认 300ms）
    """
    timestamps = index.global_timestamps  # 已排序
    
    # 二分查找插入点
    pos = bisect_left(timestamps, target_time)
    
    # 检查前后两个位置
    candidates = []
    if pos > 0:
        candidates.append(pos - 1)
    if pos < len(timestamps):
        candidates.append(pos)
    
    # 取最近的
    best_pos = min(candidates, key=lambda p: abs(timestamps[p] - target_time))
    time_diff = abs(timestamps[best_pos] - target_time)
    
    if time_diff <= self.tolerance:  # 默认 0.3s
        return message, time_diff
    return None
```

### 3.4 同步结果数据结构

```python
@dataclass
class SyncedFrame:
    """同步后的一帧数据"""
    timestamp: float              # 基准时间戳（秒）
    messages: Dict[str, Any]      # topic -> message 映射
    valid: bool = True            # 是否所有必需 topic 都找到了数据
```

---

## 4. 高频数据收集

### 4.1 为什么需要高频数据？

同步帧固定为 **10Hz**，但某些 KPI 需要更高频率的原始数据：

| KPI | 需要的数据 | 数据频率 |
|-----|---------|--------|
| 横向猛打 | 方向盘转角速度/加速度 | 50Hz |
| 顿挫 | 纵向加速度 jerk | 50-100Hz |
| 画龙 | 方向盘振荡 | 50Hz |

### 4.2 StreamingData 数据结构

```python
@dataclass
class StreamingData:
    """
    流式处理的中间数据容器
    
    数据分类：
    1. 低频数据（10Hz 同步帧）：里程、接管、定位等
    2. 高频数据（原始频率）：转向、舒适性、画龙等
    """
    # ========== 低频数据 ==========
    positions: List[tuple]           # [(lat, lon, is_auto, timestamp), ...]
    auto_states: List[tuple]         # [(is_auto, timestamp), ...]
    accelerations: List[tuple]       # [(lon_acc, lat_acc, speed, timestamp), ...]
    lateral_errors: List[tuple]      # [(lateral_error, kappa, timestamp), ...]
    steering_data: List[tuple]       # [(steering_angle, speed, timestamp), ...]
    
    # ========== 高频数据 ==========
    # 底盘高频 (~50Hz): [(steering_angle, steering_vel, speed, lat_acc, timestamp), ...]
    chassis_highfreq: List[tuple]
    
    # 控制高频 (~100Hz): [(lon_acc, timestamp), ...]
    control_highfreq: List[tuple]
    
    # 高频数据对应的自动驾驶状态（从 func 插值）
    chassis_auto_states: List[tuple]  # [(is_auto, timestamp), ...]
    control_auto_states: List[tuple]  # [(is_auto, timestamp), ...]
```

### 4.3 自动驾驶状态插值

高频数据（50Hz）需要与低频状态（10Hz）对齐，使用 **最近邻插值**：

```python
# 使用 numpy searchsorted 进行二分查找
idx = np.searchsorted(func_timestamps, current_timestamp)

# 选择更近的时间点
if idx == 0:
    is_auto = func_auto_states[0]
elif idx >= len(func_timestamps):
    is_auto = func_auto_states[-1]
else:
    # 比较前后两个点的距离
    if current_timestamp - func_timestamps[idx-1] < func_timestamps[idx] - current_timestamp:
        is_auto = func_auto_states[idx-1]
    else:
        is_auto = func_auto_states[idx]
```

---

## 5. 信号处理

信号处理是 KPI 计算的基础，主要使用 **scipy.signal** 库。

### 5.1 Butterworth 低通滤波

**原理**：滤除高频噪声，保留有效信号。

```python
from scipy.signal import butter, filtfilt

def butter_lowpass_filter(data, fs, cutoff=10.0, order=2):
    """
    Butterworth 低通滤波
    
    原理：
    1. 计算奈奎斯特频率：nyq = fs / 2
    2. 归一化截止频率：normal_cutoff = cutoff / nyq
    3. 设计滤波器系数 (b, a)
    4. 使用 filtfilt 进行零相位滤波（正向+反向）
    
    参数：
    - data: 输入信号
    - fs: 采样频率 (Hz)
    - cutoff: 截止频率 (Hz)，保留 0~cutoff 的信号
    - order: 滤波器阶数，越高衰减越陡
    """
    nyq = 0.5 * fs
    normal_cutoff = min(cutoff / nyq, 0.99)  # 不能超过奈奎斯特频率
    
    # 设计 Butterworth 滤波器
    b, a = butter(order, normal_cutoff, btype='low', analog=False)
    
    # filtfilt: 零相位滤波，无延迟
    y = filtfilt(b, a, data)
    return y
```

**为什么使用 `filtfilt` 而不是 `lfilter`**：
- `lfilter`：单向滤波，有相位延迟
- `filtfilt`：双向滤波（正向+反向），零相位延迟，保持信号时间对齐

### 5.2 Savitzky-Golay 平滑滤波

**原理**：在滑动窗口内进行多项式拟合，比简单移动平均更好地保留峰值。

```python
from scipy.signal import savgol_filter

def savgol_smooth(data, fs, window_sec=0.1, polyorder=3):
    """
    Savitzky-Golay 平滑滤波
    
    原理：
    1. 在每个窗口内用 polyorder 阶多项式拟合
    2. 用拟合值替换窗口中心点
    3. 相比移动平均，更好地保留峰值和边缘
    
    参数：
    - window_sec: 窗口时长（秒）
    - polyorder: 多项式阶数（必须 < 窗口长度）
    """
    window_len = int(max(5, round(window_sec * fs)))
    if window_len % 2 == 0:
        window_len += 1  # 必须为奇数
    
    return savgol_filter(data, window_len, polyorder)
```

### 5.3 导数计算（中心差分）

```python
def compute_derivative(timestamps, values):
    """
    中心差分求导（比前向/后向差分更稳定）
    
    原理：
    - 中心差分：f'(x) ≈ (f(x+h) - f(x-h)) / (2h)
    - 边界用单边差分
    
    优点：
    - 二阶精度，误差为 O(h²)
    - 对噪声不那么敏感
    """
    n = len(values)
    dt = np.median(np.diff(timestamps))  # 中位数采样间隔
    
    derivative = np.zeros(n)
    
    # 中心差分
    derivative[1:-1] = (values[2:] - values[:-2]) / (2.0 * dt)
    
    # 边界：单边差分
    derivative[0] = (values[1] - values[0]) / dt
    derivative[-1] = (values[-1] - values[-2]) / dt
    
    return derivative
```

### 5.4 完整的 Jerk 计算流程

Jerk（冲击度）= 加速度的变化率 = d(acceleration)/dt

```python
def compute_jerk_robust(timestamps, accelerations, 
                        target_fs=None, filter_method='butter', 
                        cutoff=None, max_dt=1.0):
    """
    行业标准 Jerk 计算方法
    
    流程：
    1. 重采样到均匀时间网格（如果需要）
    2. 低通滤波（消除高频噪声）
    3. 中心差分求导
    4. 异常值处理（bag 间隔导致的尖峰）
    5. 计算统计量（RMS、P95、P99 等）
    """
    # 1. 估计原始采样率
    dt_raw = np.median(np.diff(timestamps))
    fs_raw = 1.0 / dt_raw
    
    # 2. 重采样（可选）
    if target_fs:
        t_rs, a_rs = resample_uniform(timestamps, accelerations, 1.0/target_fs)
    
    # 3. 自动选择截止频率
    if cutoff is None:
        cutoff = min(0.2 * fs, 20.0)  # 推荐：采样率的 20%，最大 20Hz
    
    # 4. 滤波
    a_filt = butter_lowpass_filter(a_rs, fs, cutoff=cutoff, order=2)
    
    # 5. 中心差分求导
    jerk = compute_derivative(t_rs, a_filt)
    
    # 6. 处理 bag 间隔（时间跳跃处置零）
    gap_indices = np.where(np.diff(timestamps) > max_dt)[0]
    for gap_idx in gap_indices:
        # 清除间隔附近的 jerk 值
        jerk[max(0, gap_idx-2):min(len(jerk), gap_idx+3)] = 0
    
    # 7. 过滤物理不合理的极端值
    MAX_PHYSICAL_JERK = 15.0  # m/s³
    jerk[np.abs(jerk) > MAX_PHYSICAL_JERK] = np.nan
    
    # 8. 计算统计量
    stats = {
        'j_max': np.nanmax(np.abs(jerk)),
        'j_rms': np.sqrt(np.nanmean(jerk**2)),
        'j_p95': np.nanpercentile(np.abs(jerk), 95),
        'j_p99': np.nanpercentile(np.abs(jerk), 99),
    }
    
    return jerk, stats
```

---

## 6. KPI 计算框架

### 6.1 BaseKPI 抽象类

```python
class BaseKPI(ABC):
    """
    KPI计算基类
    
    支持两种计算模式：
    1. 普通模式：直接调用 compute(synced_frames)
    2. 流式模式：先调用 collect() 收集数据，最后调用 compute_from_collected()
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """KPI名称"""
        pass
    
    @property
    def required_topics(self) -> List[str]:
        """需要的topic列表"""
        return []
    
    @property
    def dependencies(self) -> List[str]:
        """依赖的其他 KPI 名称列表"""
        return []
    
    @property
    def supports_streaming(self) -> bool:
        """是否支持流式收集模式"""
        return False
    
    @abstractmethod
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        """计算KPI（普通模式）"""
        pass
    
    def collect(self, synced_frames: List, streaming_data: StreamingData, **kwargs):
        """收集中间数据（流式模式）"""
        pass
    
    def compute_from_collected(self, streaming_data: StreamingData, **kwargs) -> List[KPIResult]:
        """从收集的数据计算（流式模式）"""
        pass
```

### 6.2 KPI 依赖管理与拓扑排序

系统使用 **Kahn's 算法** 进行拓扑排序，确保 KPI 按依赖关系顺序计算：

```python
def _sort_kpis_by_dependency(self) -> tuple:
    """
    使用拓扑排序按依赖关系对 KPI 进行排序
    
    Returns:
        (independent_kpis, dependent_kpis_ordered)
    """
    from collections import deque
    
    # 构建依赖图和入度
    in_degree = {kpi.name: 0 for kpi in self.kpi_calculators}
    graph = {kpi.name: [] for kpi in self.kpi_calculators}
    
    for kpi in self.kpi_calculators:
        for dep in kpi.dependencies:
            if dep in kpi_map:
                graph[dep].append(kpi.name)
                in_degree[kpi.name] += 1
    
    # Kahn's algorithm
    queue = deque([name for name, deg in in_degree.items() if deg == 0])
    sorted_order = []
    
    while queue:
        current = queue.popleft()
        sorted_order.append(current)
        
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    return independent_kpis, dependent_kpis_ordered
```

### 6.3 时间跳跃处理

当自动驾驶状态过滤后，时间戳可能不连续（接管段被移除）。事件检测需要处理时间跳跃：

```python
def _detect_events(self, timestamps, values, threshold):
    """
    检测事件，处理时间跳跃
    
    当相邻时间戳差距超过 max_gap 时，强制结束当前事件，
    防止事件跨越接管段或 bag 切换。
    """
    max_gap = 0.5  # 0.5 秒
    
    for i in range(len(values)):
        # 检查时间跳跃
        if i > 0 and (timestamps[i] - timestamps[i-1]) > max_gap:
            if in_event:
                # 强制结束事件
                finalize_event()
                in_event = False
        
        # 正常的事件检测逻辑
        ...
```

### 6.4 KPI 计算示例（横向猛打）

```python
class EmergencyEventsKPI(BaseKPI):
    
    def compute_from_collected(self, streaming_data, **kwargs):
        # 1. 检查是否有高频数据
        use_chassis_highfreq = len(streaming_data.chassis_highfreq) >= 100
        
        if use_chassis_highfreq:
            # 2. 过滤只保留自动驾驶状态的数据
            auto_indices = [i for i, (is_auto, _) in enumerate(streaming_data.chassis_auto_states)
                           if is_auto]
            
            # 3. 提取数据
            steering_angles = np.array([streaming_data.chassis_highfreq[i][0] for i in auto_indices])
            timestamps = np.array([streaming_data.chassis_highfreq[i][4] for i in auto_indices])
            
            # 4. 估计采样率
            actual_fs = 1.0 / np.median(np.diff(timestamps))
            
            # 5. 低通滤波（去噪）
            steering_angles_filtered = butter_lowpass_filter(
                steering_angles, cutoff=10.0, fs=actual_fs, order=2)
            
            # 6. 差分计算转角速度
            ts_rate, steering_velocities = compute_derivative(timestamps, steering_angles_filtered)
            
            # 7. 对转角速度滤波
            steering_velocities = butter_lowpass_filter(
                steering_velocities, cutoff=15.0, fs=actual_fs, order=2)
        
        # 8. 检测事件（考虑时间跳跃）
        lateral_jerk_events = self._detect_lateral_jerk(timestamps, steering_velocities, speeds)
        
        return results
```

---

## 7. 持久化缓存

### 7.1 缓存原理

**问题**：每次分析都要解析 bag 文件，耗时较长。  
**解决**：首次分析后缓存数据，后续直接加载。

### 7.2 缓存架构

```
.frame_cache/
├── manifest.json          # 缓存元数据
└── bags/
    ├── {bag_hash}/
    │   ├── frames.db      # SQLite 数据库
    │   └── highfreq.db    # 高频数据
    └── ...
```

### 7.3 缓存有效性校验

```python
def _compute_bag_hash(self, bag_path: str) -> str:
    """
    计算 bag 目录的哈希值
    
    基于：
    - .db3 文件名
    - 文件大小
    - 修改时间
    
    任何变化都会导致哈希不同，触发重新解析
    """
    hash_input = []
    for f in bag_path.glob("**/*.db3"):
        stat = f.stat()
        hash_input.append(f"{f.name}:{stat.st_size}:{int(stat.st_mtime)}")
    
    return hashlib.md5("|".join(hash_input).encode()).hexdigest()[:16]
```

### 7.4 高频数据存储（SQLite）

```sql
-- 底盘高频数据表
CREATE TABLE chassis_highfreq (
    id INTEGER PRIMARY KEY,
    steering_angle REAL,
    steering_vel REAL,
    speed REAL,
    lat_acc REAL,
    timestamp REAL,
    is_auto INTEGER
);

-- 控制高频数据表
CREATE TABLE control_highfreq (
    id INTEGER PRIMARY KEY,
    lon_acc REAL,
    timestamp REAL,
    is_auto INTEGER
);
```

---

## 8. 报告生成

### 8.1 支持的输出格式

| 格式 | 文件 | 用途 |
|------|------|------|
| JSON | `kpi_report_trace.json` | 程序化处理，包含完整异常溯源 |
| Markdown | `kpi_report.md` | 人类可读，适合 GitHub/Confluence |
| Word | `kpi_report.docx` | 正式报告（需要 `python-docx`）|

### 8.2 报告结构

```json
{
  "metadata": {
    "bag_path": "/path/to/bags",
    "duration": 1234.5,
    "bag_count": 10,
    "analysis_time": "2026-01-16T12:00:00"
  },
  "data_quality": {
    "topic_coverage": {...},
    "sampling_rate_issues": [...]
  },
  "results": [
    {
      "kpi_category": "里程统计",
      "name": "自动驾驶里程",
      "value": 15.23,
      "unit": "km",
      "description": "...",
      "anomalies": [
        {
          "timestamp": 1768377423.12,
          "bag_name": "20260114-155706",
          "description": "异常描述",
          "value": 123.4,
          "threshold": 100
        }
      ]
    }
  ]
}
```

---

## 9. 完整流程图

```
┌─────────────────────────────────────────────────────────────────────┐
│                        KPI 分析完整流程                              │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 1. 数据加载 (bag_reader.py)                                         │
│    ├─ rosbags.Reader 打开 .db3 文件                                 │
│    ├─ CDR 反序列化 (typestore / rclpy)                              │
│    └─ 输出: Dict[topic, TopicData]                                  │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 2. 时间同步 (time_sync.py)                                          │
│    ├─ 预计算 global_timestamp 索引                                   │
│    ├─ 二分查找对齐 (bisect_left)                                    │
│    └─ 输出: List[SyncedFrame] @ 10Hz                                │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 3. 高频数据收集 (main.py)                                           │
│    ├─ 遍历 /vehicle/chassis_domain_report @ ~50Hz                   │
│    ├─ 遍历 /control/control @ ~100Hz                                │
│    ├─ 插值自动驾驶状态 (np.searchsorted)                            │
│    └─ 输出: StreamingData.chassis_highfreq, control_highfreq        │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 4. 信号处理 (signal.py)                                             │
│    ├─ Butterworth 低通滤波 (scipy.signal.butter + filtfilt)         │
│    ├─ 中心差分求导 (numpy)                                          │
│    ├─ 异常值处理（时间跳跃、物理极值）                               │
│    └─ 统计量计算 (RMS, P95, P99)                                    │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 5. KPI 计算 (kpi/*.py)                                              │
│    ├─ 拓扑排序确定计算顺序                                           │
│    ├─ collect() → 收集低频同步数据                                   │
│    ├─ compute_from_collected() → 使用高频数据计算                    │
│    └─ 事件检测（考虑时间跳跃 max_gap=0.5s）                          │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────────┐
│ 6. 报告生成 (reporter.py)                                           │
│    ├─ JSON (完整数据 + 异常溯源)                                     │
│    ├─ Markdown (人类可读)                                           │
│    └─ Word (正式报告，可选)                                          │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 10. 关键库总结

| 库 | 模块 | 功能 |
|---|---|---|
| `rosbags` | Reader, typestore | ROS2 bag 解析、消息反序列化 |
| `numpy` | 几乎所有模块 | 数组操作、数值计算 |
| `scipy.signal` | signal.py | Butterworth 滤波、Savgol 平滑 |
| `bisect` | time_sync.py | 二分查找 |
| `sqlite3` | frame_store.py | 缓存存储 |
| `hashlib` | frame_store.py | 文件哈希校验 |
| `pyproj` | geo.py | 地理坐标转换 (WGS84 ↔ UTM) |
| `protobuf` | proto/*.py | 解析嵌入的 protobuf 数据 |
| `tabulate` | reporter.py | 表格格式化 |
| `tqdm` | bag_reader.py | 进度条显示 |
| `collections.deque` | main.py | 拓扑排序队列 |

---

## 附录：常用命令

```bash
# 分析单个 bag
python -m src.main /path/to/bag

# 分析目录下所有 bag
python -m src.main /path/to/bags --multi-bag

# 使用缓存加速
python -m src.main /path/to/bags --cache

# 只计算指定 KPI
python -m src.main /path/to/bags --kpi "里程统计"

# 清除缓存
python -m src.main /path/to/bags --clear-cache
```
