# Hello KPI - 自动驾驶 KPI 分析系统

一个基于 ROS2 bag 数据的自动驾驶关键性能指标（KPI）分析工具，支持里程统计、接管检测、舒适性评估、安全性分析等多维度指标计算。

## 目录

- [系统架构](#系统架构)
- [数据处理流程](#数据处理流程)
- [KPI 指标详解](#kpi-指标详解)
- [配置说明](#配置说明)
- [快速开始](#快速开始)
- [扩展开发](#扩展开发)

---

## 系统架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           KPIAnalyzer (主程序)                           │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐             │
│  │  BagReader     │  │ TimeSynchronizer│  │  KPIReporter   │             │
│  │  数据加载      │─▶│  时间同步       │─▶│  报告生成      │             │
│  └────────────────┘  └────────────────┘  └────────────────┘             │
│          │                   │                                          │
│          ▼                   ▼                                          │
│  ┌──────────────────────────────────────────────────────────────┐      │
│  │                    KPI 计算器矩阵                              │      │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │      │
│  │  │ Mileage  │ │ Takeover │ │ Comfort  │ │ Emergency│        │      │
│  │  │ 里程统计  │ │ 接管统计  │ │ 舒适性    │ │ 紧急事件  │        │      │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘        │      │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐        │      │
│  │  │ Steering │ │LaneKeep  │ │ Weaving  │ │   ROI    │        │      │
│  │  │ 转向平滑  │ │ 车道保持  │ │ 画龙检测  │ │ 障碍物    │        │      │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘        │      │
│  │  ┌──────────┐ ┌──────────┐                                   │      │
│  │  │Curvature │ │Localization│                                 │      │
│  │  │ 道路曲率  │ │ 定位置信度 │                                 │      │
│  │  └──────────┘ └──────────┘                                   │      │
│  └──────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────┘
```

### 模块说明

| 模块 | 路径 | 功能 |
|------|------|------|
| `data_loader` | `src/data_loader/` | ROS2 bag 文件读取、多 bag 管理、FrameStore 数据层 |
| `sync` | `src/sync/` | 多 Topic 时间同步（基于 global_timestamp） |
| `kpi` | `src/kpi/` | KPI 计算器实现 |
| `utils` | `src/utils/` | 信号处理、几何计算、地理坐标转换 |
| `output` | `src/output/` | 报告生成（JSON/CSV/MD/HTML/Word） |
| `proto` | `src/proto/` | Protobuf 解析（控制调试数据） |

### 数据处理模式

系统支持两种数据处理模式，根据数据量自动选择：

```
┌────────────────────────────────────────────────────────────────────────┐
│                         自动模式选择                                    │
│                                                                        │
│   输入数据                                                              │
│      │                                                                 │
│      ▼                                                                 │
│   ┌─────────────────┐                                                  │
│   │ 估算帧数         │                                                  │
│   │ estimate_frame  │                                                  │
│   └────────┬────────┘                                                  │
│            │                                                           │
│            ▼                                                           │
│   ┌────────────────────────────────┐                                   │
│   │ 多文件 OR 帧数 > 200,000？      │                                   │
│   └────────┬───────────────────────┘                                   │
│            │                                                           │
│     是     │      否                                                   │
│     ▼      │      ▼                                                    │
│ ┌──────────┴──┐  ┌──────────────┐                                      │
│ │FrameStore  │  │ 普通模式      │                                      │
│ │模式(磁盘)   │  │ (全内存)      │                                      │
│ └─────────────┘  └──────────────┘                                      │
└────────────────────────────────────────────────────────────────────────┘
```

| 模式 | 适用场景 | 特点 |
|------|---------|------|
| **普通模式** | 单文件、小数据量 (<20万帧) | 全内存处理，速度快 |
| **FrameStore 模式** | 多文件、大数据量 (>20万帧) | SQLite 磁盘缓存，防 OOM |

---

## 数据处理流程

### 1. 数据加载 (BagReader)

```
ROS2 Bag (.db3)
     │
     ▼
┌─────────────────┐
│ 1. 读取 metadata │  - 解析 metadata.yaml（自动生成如果不存在）
│ 2. 提取时间范围   │  - 获取 bag 起止时间
│ 3. 读取消息      │  - 按 topic 分类存储消息
│ 4. 轻量化转换    │  - 可选：转换为 LightMessage 减少内存
└─────────────────┘
     │
     ▼
TopicData: { topic_name, timestamps[], messages[] }
```

**轻量模式 (Light Mode)：**

为降低内存占用，消息可转换为仅包含 KPI 计算必需字段的轻量版本：

```python
# 原始 ROS 消息（含大量冗余字段）
OriginalMsg:
  - header, timestamp, frame_id, ...
  - position (x, y, z, lat, lon, alt, ...)
  - velocity, acceleration, orientation, ...
  - 其他数十个字段

# 轻量消息（仅保留 KPI 必需字段）
LightLocalizationMsg:
  - global_timestamp
  - lat, lon, altitude
  - status, stddev_east, stddev_north
  - heading, speed
```

| 消息类型 | 保留字段 | 内存节省 |
|---------|---------|---------|
| Localization | 位置、状态、标准差、航向、速度 | ~70% |
| ChassisDomain | 加速度、速度、转角、转角速度 | ~80% |
| FunctionManager | operator_type, chassis_state | ~90% |
| Trajectory | path_point (kappa, s, x, y) | ~60% |

**支持的 Topic：**

| Topic | 频率 | 用途 |
|-------|------|------|
| `/function/function_manager` | 10Hz | 驾驶状态判断（自动/手动） |
| `/localization/localization` | 50Hz | 位置、姿态、定位状态 |
| `/vehicle/chassis_domain_report` | 50Hz | 车辆动力学（加速度、速度、转角） |
| `/control/debug` | 50Hz | 控制调试数据（横向误差） |
| `/perception/fusion/obstacle_list_utm` | 10Hz | 障碍物感知数据 |
| `/planning/trajectory` | 10Hz | 规划轨迹（曲率 kappa） |

### 2. 时间同步 (TimeSynchronizer)

**优化策略：**
- 预计算每个 Topic 的 global_timestamp 数组
- 使用 NumPy 向量化 + 二分查找（`bisect_left`）
- 并行构建 Topic 索引

```
基准 Topic (/function/function_manager, 10Hz)
     │
     ▼ 对每个基准帧
┌─────────────────────────────────────────┐
│ 二分查找其他 Topic 中最近的消息           │
│ 条件：|Δt| ≤ tolerance (默认 300ms)      │
└─────────────────────────────────────────┘
     │
     ▼
SyncedFrame: { timestamp, messages: {topic: msg} }
```

### 3. FrameStore 数据层

**透明数据层**：KPI 计算代码无需关心数据存储位置（内存或磁盘）。

```
┌─────────────────────────────────────────────────────────────────┐
│                      FrameStore (抽象层)                         │
│                                                                 │
│   提供统一接口：                                                  │
│   - append_batch(frames, bag_name)  添加帧                      │
│   - __iter__()                      迭代所有帧                   │
│   - __len__()                       总帧数                      │
│   - get_bag_infos()                 获取 bag 信息               │
│   - finalize()                      完成写入                    │
│   - cleanup()                       清理资源                    │
│                                                                 │
│  ┌─────────────────────┐     ┌─────────────────────┐           │
│  │  MemoryFrameStore   │     │   DiskFrameStore    │           │
│  │  (内存模式)          │     │   (磁盘模式)         │           │
│  │                     │     │                     │           │
│  │  - List[SyncedFrame]│     │  - SQLite 数据库     │           │
│  │  - 直接内存存储      │     │  - pickle + zlib    │           │
│  │  - 快速访问         │     │  - 分批加载          │           │
│  └─────────────────────┘     └─────────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

**DiskFrameStore 存储结构：**

```sql
-- 帧数据表
CREATE TABLE frames (
    id INTEGER PRIMARY KEY,
    timestamp REAL NOT NULL,
    bag_name TEXT,
    data BLOB NOT NULL  -- pickle + zlib 压缩的 SyncedFrame
);

-- Bag 信息表
CREATE TABLE bag_infos (
    id INTEGER PRIMARY KEY,
    name TEXT,
    frame_count INTEGER,
    start_timestamp REAL,
    end_timestamp REAL
);
```

**自动模式选择：**

```python
def create_frame_store(estimated_frames: int, cache_dir: str) -> FrameStore:
    """
    根据预估帧数自动选择存储模式
    
    - < 200,000 帧：MemoryFrameStore（内存模式）
    - >= 200,000 帧：DiskFrameStore（磁盘模式）
    """
    if estimated_frames < 200_000:
        return MemoryFrameStore()
    else:
        return DiskFrameStore(cache_dir)
```

### 4. 持久化缓存 (FrameStoreCache)

**缓存机制**：首次分析后将处理结果持久化，再次分析相同数据时直接加载。

```
┌─────────────────────────────────────────────────────────────────┐
│                     FrameStoreCache                             │
│                                                                 │
│   .frame_cache/                                                 │
│   ├── frames_<hash>.db     # SQLite 帧数据                      │
│   └── frames_<hash>.json   # 元数据                             │
│                                                                 │
│   元数据示例：                                                    │
│   {                                                             │
│     "bag_path": "/data/rosbag/test",                           │
│     "bag_hash": "5d19afd9ad83e35c",                            │
│     "frame_count": 156533,                                      │
│     "created_at": "2025-12-19T12:00:00",                       │
│     "bag_infos": [                                              │
│       {"name": "kpi_0", "frame_count": 52000, ...},            │
│       {"name": "kpi_1", "frame_count": 52000, ...},            │
│       ...                                                       │
│     ]                                                           │
│   }                                                             │
└─────────────────────────────────────────────────────────────────┘
```

**缓存有效性检查：**

```python
def is_valid(self, bag_path: str) -> bool:
    """
    检查缓存是否有效
    
    1. 元数据文件存在
    2. 数据库文件存在
    3. bag_hash 匹配（基于文件内容哈希）
    """
```

**命令行控制：**

```bash
# 禁用缓存
python run_analysis.py /data/bag --no-cache

# 清理缓存后重新分析
python run_analysis.py /data/bag --clear-cache

# 指定缓存目录
python run_analysis.py /data/bag --cache-dir /tmp/my_cache
```

### 5. KPI 计算

每个 KPI 计算器实现 `BaseKPI` 接口：

```python
class BaseKPI(ABC):
    @property
    def name(self) -> str: ...           # KPI 名称
    @property
    def required_topics(self) -> List[str]: ...  # 依赖的 Topic
    @property
    def dependencies(self) -> List[str]: ...     # 依赖的其他 KPI
    
    def compute(self, synced_frames, **kwargs) -> List[KPIResult]: ...
```

**计算顺序：**
1. **独立 KPI**（无依赖）：里程统计、定位置信度、道路曲率
2. **依赖 KPI**：接管统计、舒适性、紧急事件等（依赖里程数据）

### 4. 报告生成

输出格式：`JSON` | `CSV` | `Markdown` | `HTML` | `Word`

---

## KPI 指标详解

### 1. 里程统计 (MileageKPI)

**目的：** 统计总里程、自动驾驶里程及时间分布。

**数据源：**
- `/function/function_manager.operator_type` - 驾驶状态（2=自动）
- `/localization/localization.global_localization.position` - 经纬度

**计算逻辑：**

```
1. 提取所有帧的 (lat, lon, is_auto, timestamp)
2. 过滤无效位置：
   - lat=0 或 lon=0
   - 超出范围 [-90,90], [-180,180]
3. 分段计算：
   - 驾驶状态变化时切分
   - 时间间隔 > 1s 时切分（处理 bag 间隔）
4. 里程计算：Haversine 公式
   d = 2R × arcsin(√(sin²(Δlat/2) + cos(lat1)×cos(lat2)×sin²(Δlon/2)))
5. 过滤异常速度 > 200 km/h 的段落
```

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 总里程 | km | 所有行驶里程（保留3位小数） |
| 自动驾驶里程 | km | operator_type=2 状态下的里程 |
| 非自动驾驶里程 | km | 手动驾驶里程 |
| 自动驾驶时间 | min | 累计自动驾驶时长 |
| 自动驾驶平均速度 | km/h | 自动驾驶里程 / 自动驾驶时间 |

---

### 2. 接管统计 (TakeoverKPI)

**目的：** 统计人工接管自动驾驶的次数和频率。

**数据源：**
- `/function/function_manager.operator_type`

**接管定义：**
```
operator_type: 2(自动) → 非2(手动)
```

**有效接管条件：**
```
自动驾驶持续时间 ≥ min_auto_duration (默认 0.3s)
```

**计算逻辑：**

```
1. 提取驾驶状态序列 [is_auto, timestamp]
2. 检测状态跳变点：
   - 从 True 变为 False 即为接管
3. 计算接管前自动驾驶持续时间
4. 按 min_auto_duration 划分有效/无效接管
5. 统计：
   - 有效接管次数
   - 每百公里接管次数 = 有效次数 / 自动里程 × 100
   - 平均接管里程 = 自动里程 / 有效次数
   - 平均接管间隔
```

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 总接管次数 | 次 | 有效接管次数（自动驾驶≥0.3s） |
| 每百公里接管次数 | 次/百公里 | 归一化频率 |
| 平均接管里程 | km/次 | 每多少公里接管一次 |
| 平均接管间隔 | min | 两次接管的时间间隔 |

---

### 3. 车道保持精度 (LaneKeepingKPI)

**目的：** 评估横向控制精度，按直道/弯道分类统计。

**数据源：**
- `/control/debug.reserved0` - 需 Protobuf 解析，获取 `lateral_error`
- `/planning/trajectory.path_point[].kappa` - 道路曲率
- `/localization/localization` - 定位状态和标准差

**定位可信度过滤：**
```python
loc_valid = (status in [3, 7]) and (max(stddev_east, stddev_north) ≤ 0.2m)
```

**直道/弯道判定：**
```python
road_type = "straight" if |kappa| < 0.003 else "curve"
```

**kappa 获取逻辑：**
```
1. 获取自车 UTM 坐标
2. 从规划轨迹提取路径点
3. 向量化计算所有点到自车的距离
4. 找最近点（距离 < 5m）
5. 返回该点的 kappa 值
```

**连续帧事件合并：**
```
如果横向偏差超限的帧连续（间隔≤3帧），合并为单次事件
```

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 全路段横向偏差均值 | cm | 所有帧 |lateral_error| 的平均值 |
| 横向偏差P95 | cm | 95分位数，比均值更能反映极端情况 |
| 直道保持精度 | cm | 曲率<0.003 路段的平均偏差 |
| 弯道保持精度 | cm | 曲率≥0.003 路段的平均偏差 |
| 横向偏差超限率 | % | |偏差| > 20cm 的帧占比 |

---

### 4. 转向平滑度 (SteeringSmoothnessKPI)

**目的：** 评估方向盘操作平稳性。

**数据源：**
- `/vehicle/chassis_domain_report.eps_system.actual_steering_angle` - 转角 (°)
- `/vehicle/chassis_domain_report.eps_system.actual_steering_angle_velocity` - 转角速度 (°/s)
- `/localization/localization` - 定位可信度

**转角加速度计算：**
```python
steering_acc = d(steering_velocity) / dt  # 中心差分
```

**速度自适应阈值（转角速度）：**

| 车速 (km/h) | 阈值 (°/s) |
|-------------|-----------|
| 0-10 | 200 |
| 10-20 | 180 |
| 20-30 | 160 |
| 30-40 | 140 |
| 40-50 | 120 |
| 50-60 | 100 |
| 60-70 | 80 |
| 70+ | 60 |

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 转向平滑度(转角速度均值) | °/s | 转角速度绝对值平均 |
| 转角速度P95 | °/s | 95分位数 |
| 转角加速度均值 | °/s² | 转角加速度绝对值平均 |
| 转角速度超限率 | % | 超过速度自适应阈值的比例 |
| 转角加速度超限率 | % | 超过阈值的比例 |

---

### 5. 舒适性检测 (ComfortKPI)

**目的：** 评估驾乘舒适性，核心指标为 Jerk（加速度变化率）。

**数据源：**
- `/vehicle/chassis_domain_report.motion_system.vehicle_longitudinal_acceleration` - 纵向加速度
- `/vehicle/chassis_domain_report.motion_system.vehicle_lateral_acceleration` - 横向加速度

**Jerk 计算（行业标准方法）：**

```
1. 数据对齐：确保纵向/横向加速度使用相同时间戳
2. 检测采样率：计算 dt 中位数 → fs = 1/dt
3. 低通滤波（可选）：
   - Butterworth (默认): cutoff=10Hz, order=2
   - Savitzky-Golay: window=0.1s, order=3
4. 中心差分求导：
   jerk[i] = (acc[i+1] - acc[i-1]) / (2*dt)
5. 物理极值过滤：|jerk| > 15 m/s³ 标记为异常
6. 多尺度统计：
   - 瞬时：peak, P95, P99
   - 窗口RMS(1s)：滑动窗口计算 √(mean(jerk²))
   - 零交叉率：jerk 符号翻转频率
```

**阈值配置：**

| 参数 | 值 | 说明 |
|------|-----|------|
| lon_jerk_threshold | 5.0 m/s³ | 纵向瞬时阈值 |
| lat_jerk_threshold | 3.0 m/s³ | 横向瞬时阈值 |
| lon_rms_threshold | 2.0 m/s³ | 纵向1s RMS阈值 |
| lat_rms_threshold | 1.5 m/s³ | 横向1s RMS阈值 |

**Jerk 超限事件合并：**
```
1. 找出纵向/横向超限的帧索引
2. 合并同时超限的帧为"横纵向Jerk"事件
3. 连续帧（间隔≤3）合并为单次事件
```

**舒适性综合评分（0-100）：**

```python
score = 100
# 纵向RMS扣分（最多30分）
if lon_rms_1s_max > 2.0: score -= min(30, (lon_rms_1s_max - 2.0) * 10)
# 横向RMS扣分（最多25分）
if lat_rms_1s_max > 1.5: score -= min(25, (lat_rms_1s_max - 1.5) * 12)
# 峰值扣分（最多各15分）
# 高频振荡扣分（零交叉率>20Hz，最多15分）

评级：A(≥90) | B(≥75) | C(≥60) | D(<60)
```

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 纵向Jerk均值 | m/s³ | 纵向Jerk绝对值平均 |
| 纵向Jerk P95 | m/s³ | 95分位数 |
| 纵向Jerk RMS(1s)最大 | m/s³ | 1秒窗口RMS最大值 |
| 横向Jerk均值 | m/s³ | 横向Jerk绝对值平均 |
| 横向Jerk P95 | m/s³ | 95分位数 |
| Jerk超限事件 | 次 | 横纵向超限事件总数 |
| 舒适性综合评分 | 分 | 0-100综合评分 |

---

### 6. 紧急事件检测 (EmergencyEventsKPI)

**目的：** 检测急减速、顿挫、横向猛打等安全相关事件。

#### 6.1 急减速检测

**条件：**
```
acceleration < -3.0 m/s² 且持续 ≥ 100ms
```

**实现：**
```python
detections = SignalProcessor.detect_threshold_events(
    timestamps, accelerations,
    threshold=-3.0,
    min_duration=0.1,
    compare='less'
)
```

#### 6.2 顿挫检测

**条件：**
```
|jerk| > 2.5 m/s³ 且持续 ≥ 200ms
```

**实现：**
```python
# 先计算 jerk
ts_jerk, jerks = SignalProcessor.compute_jerk(timestamps, accelerations)
# 再检测事件
detections = SignalProcessor.detect_threshold_events(
    ts_jerk, jerks,
    threshold=2.5,
    min_duration=0.2,
    compare='abs_greater'
)
```

#### 6.3 横向猛打检测

**条件：**
```
|steering_velocity| > speed_threshold[speed]
```

使用与转向平滑度相同的速度自适应阈值。

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 急减速次数 | 次 | 满足条件的事件数 |
| 急减速频率 | 次/百公里 | 归一化频率 |
| 顿挫次数 | 次 | 高Jerk事件数 |
| 顿挫频率 | 次/百公里 | 归一化频率 |
| 横向猛打次数 | 次 | 转角速度超限事件数 |

---

### 7. 画龙检测 (WeavingKPI)

**目的：** 检测横向控制异常（lateral oscillation）。

**判定条件：**
```
1. 方向盘角速度 |δ̇| > 25 °/s
2. 方向盘角速度符号翻转 ≥ 3 次（1秒窗口内）
3. 持续时间 ≥ 0.5s
```

**符号翻转检测（优化版）：**
```python
# 使用前缀和 O(n) 计算
signs = np.sign(steering_velocities)
sign_changes = np.diff(signs) != 0
prefix_flips = np.cumsum(sign_changes)
# 窗口内翻转次数 = prefix_flips[i] - prefix_flips[i-window]
```

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 画龙次数 | 次 | 检测到的画龙事件数 |
| 最大横向偏移 | cm | 自动驾驶期间最大横向误差 |
| 画龙频率 | 次/百公里 | 归一化频率 |
| 平均画龙里程 | km/次 | 每多少公里画龙一次 |

---

### 8. ROI 障碍物统计 (ROIObstaclesKPI)

**目的：** 统计不同距离区域内的障碍物分布情况。

**三级 ROI 设计：**

```
┌─────────────────────────────────────────────────────────────┐
│                       远距离 (Far)                          │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    中距离 (Mid)                      │   │
│  │  ┌───────────────────────────────────────────────┐  │   │
│  │  │           近距离 (Near) - 圆形               │  │   │
│  │  │               ●                               │  │   │
│  │  │             (自车)                            │  │   │
│  │  └───────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

| ROI | 形状 | 范围 | 用途 |
|-----|------|------|------|
| Near | 圆形 | 半径 10m | 紧急反应区 |
| Mid | 矩形 | 前25m/后10m/左右各5m | 主动避障区 |
| Far | 矩形 | 前60m/后25m/左右各7m | 预警感知区 |

**距离计算（Box-to-Box）：**
```python
def distance_to_ego(obstacle):
    corners = obstacle.get_corners()  # 四个角点
    min_dist = min(point_to_ego_box_distance(c) for c in corners)
    return min_dist
```

**方向分类：**
- **前方 (Front)**: 障碍物中心在自车前方，y偏移小
- **侧前方 (Front-Side)**: 前方但有明显侧向偏移
- **侧方 (Side)**: 主要在自车侧面

**输出指标（每个 ROI）：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 平均障碍物数 | 个/帧 | 所有帧的平均值 |
| 动态障碍物均值 | 个/帧 | motion_status=1 |
| 静态障碍物均值 | 个/帧 | motion_status=3 |
| 零障碍物帧占比 | % | 该ROI内无障碍物的比例 |
| 各方向距离P5 | m | 最危险5%情况的距离 |

---

### 9. 道路曲率统计 (CurvatureKPI)

**目的：** 统计道路曲率分布，判断直道/弯道比例。

**数据源：**
- `/localization/localization.global_localization.position` - 自车经纬度
- `/planning/trajectory.path_point[]` - 规划轨迹点（含 kappa）

**计算流程：**
```
1. 经纬度 → UTM 坐标转换
2. 从轨迹提取路径点（x, y, kappa）
3. 向量化查找最近点：
   dist_sq = (xs - ego_x)² + (ys - ego_y)²
   nearest_idx = argmin(dist_sq)
4. 过滤匹配距离 > 5m 的帧
5. 统计 |kappa| 的 mean/std/max/min/median
6. 按阈值 0.003 分类直道/弯道
```

**地图可视化：**
- 使用 Mapbox 生成交互式 HTML 地图
- 绿色点：直道（κ < 0.003）
- 红色点：弯道（κ ≥ 0.003）

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 道路平均曲率 | 1/m | |kappa| 的平均值 |
| 直道比例 | % | κ < 0.003 的比例 |
| 弯道比例 | % | κ ≥ 0.003 的比例 |
| 轨迹匹配平均距离 | m | 自车到最近轨迹点的平均距离 |

---

### 10. 定位置信度 (LocalizationKPI)

**目的：** 评估定位系统可靠性。

**数据源：**
- `/localization/localization.status.common` - 定位状态码
- `/localization/localization.global_localization.position_stddev` - 位置标准差

**状态判定逻辑：**

```python
if status == 2:
    return "等待初始化"
elif status in [3, 7]:  # 有效状态
    max_stddev = max(stddev_east, stddev_north)
    if max_stddev > 1.0:
        return "定位偏移"
    elif max_stddev > 0.2:
        return "可能偏移"
    else:
        return "正常"
else:
    return "未知状态"
```

**连续异常事件合并：**
- 相同状态 + 连续帧（间隔≤3）→ 合并为单次事件

**输出指标：**

| 指标 | 单位 | 说明 |
|------|------|------|
| 定位正常率 | % | 状态为"正常"的占比 |
| 定位偏移率 | % | stddev > 1.0m 的占比 |
| 可能偏移率 | % | 0.2m < stddev ≤ 1.0m 的占比 |
| 平均位置标准差 | m | max(east, north) 的平均值 |

---

## 配置说明

配置文件：`config/kpi_config.yaml`

### 基础配置

```yaml
base:
  sync_tolerance: 0.3   # 时间同步容差 (秒)
  base_frequency: 10    # 基准频率 (Hz)
  utm_zone: 51          # UTM区域 (上海)
  hemisphere: 'N'       # 北半球

ego_vehicle:
  front_length: 3.79    # 后轴到车头距离 (m)
  rear_length: 0.883    # 后轴到车尾距离 (m)
  left_width: 0.945     # 后轴到左侧距离 (m)
  right_width: 0.945    # 后轴到右侧距离 (m)
```

### KPI 阈值配置

```yaml
kpi:
  comfort:
    filter_method: butter     # 滤波方法
    cutoff_hz: 10.0          # 截止频率
    lon_jerk_threshold: 5.0  # 纵向Jerk阈值
    lat_jerk_threshold: 3.0  # 横向Jerk阈值

  hard_braking:
    acceleration_threshold: -3.0  # 急减速阈值
    min_duration: 0.1            # 最小持续时间

  roi:
    near_radius: 10.0
    mid_roi:
      front: 25.0
      rear: 10.0
      left: 5.0
      right: 5.0
```

---

## 快速开始

### 安装

```bash
cd /path/to/hello_kpi
pip install -r requirements.txt
```

### 运行分析

```bash
# 基本用法（自动选择处理模式）
python run_analysis.py /path/to/ros2bag

# 指定输出目录
python run_analysis.py /path/to/ros2bag -o ./my_output

# 使用自定义配置
python run_analysis.py /path/to/ros2bag -c ./my_config.yaml

# 详细输出
python run_analysis.py /path/to/ros2bag -v
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `bag_path` | ROS2 bag 路径（文件或目录） | 必需 |
| `-o, --output` | 输出目录 | `./output` |
| `-c, --config` | 配置文件路径 | `config/kpi_config.yaml` |
| `-v, --verbose` | 详细输出 | False |
| `--no-cache` | 禁用持久化缓存 | False |
| `--clear-cache` | 清理缓存后重新分析 | False |
| `--cache-dir` | 缓存目录 | `.frame_cache` |
| `--force-disk` | 强制使用磁盘模式 | False |

### 处理模式说明

```bash
# 自动模式（推荐）
# - 单文件 + 小数据量：内存模式
# - 多文件 或 大数据量：FrameStore 模式 + 持久化缓存
python run_analysis.py /data/rosbag/test

# 强制使用磁盘模式（适用于内存受限环境）
python run_analysis.py /data/rosbag/test --force-disk

# 禁用缓存（每次重新解析 bag）
python run_analysis.py /data/rosbag/test --no-cache

# 清理旧缓存（数据有更新时使用）
python run_analysis.py /data/rosbag/test --clear-cache
```

### Python API

```python
from src.main import KPIAnalyzer

# 基本用法
analyzer = KPIAnalyzer(config_path="config/kpi_config.yaml")
results = analyzer.analyze(
    bag_path="/path/to/ros2bag",
    output_dir="./output"
)

# 禁用缓存
analyzer = KPIAnalyzer(
    config_path="config/kpi_config.yaml",
    use_cache=False
)

# 指定缓存目录
analyzer = KPIAnalyzer(
    config_path="config/kpi_config.yaml",
    cache_dir="/tmp/my_cache"
)

# 结果结构
# results['results']      - 各KPI计算结果
# results['report_files'] - 生成的报告文件
# results['timing_stats'] - 耗时统计
```

### 缓存管理

```python
from src.data_loader.frame_store import FrameStoreCache

# 获取缓存统计
cache = FrameStoreCache("/path/to/cache_dir")
stats = cache.get_stats()
print(f"缓存条目: {stats['entries']}")
print(f"总大小: {stats['total_size_mb']:.1f} MB")

# 清理特定 bag 的缓存
cache.clear("/data/rosbag/test")

# 清理所有缓存
cache.clear()
```

---

## 扩展开发

### 新增 KPI 计算器

1. **创建 KPI 类**

```python
# src/kpi/my_kpi.py
from .base_kpi import BaseKPI, KPIResult

class MyKPI(BaseKPI):
    @property
    def name(self) -> str:
        return "我的KPI"
    
    @property
    def required_topics(self) -> List[str]:
        return ["/function/function_manager", "/my/topic"]
    
    @property
    def dependencies(self) -> List[str]:
        return ["里程统计"]  # 依赖其他KPI的结果
    
    def compute(self, synced_frames: List, **kwargs) -> List[KPIResult]:
        self.clear_results()
        
        for frame in synced_frames:
            msg = frame.messages.get("/my/topic")
            if msg is None:
                continue
            # 计算逻辑...
        
        self.add_result(KPIResult(
            name="指标名称",
            value=123.45,
            unit="单位",
            description="描述"
        ))
        
        return self.get_results()
```

2. **注册 KPI**

在 `src/main.py` 中添加：

```python
from src.kpi.my_kpi import MyKPI

# 在 _init_kpi_calculators 方法中
self.kpi_calculators.append(MyKPI(self.config))
```

3. **添加配置**

```yaml
# config/kpi_config.yaml
kpi:
  my_kpi:
    threshold: 1.0
    enabled: true
  ```

---

## 项目结构

```
hello_kpi/
├── config/
│   └── kpi_config.yaml       # 配置文件
├── src/
│   ├── main.py               # 主程序（KPIAnalyzer）
│   ├── constants.py          # 常量定义（Topics、LightMessage）
│   ├── data_loader/          # 数据加载
│   │   ├── bag_reader.py     # Bag 读取器
│   │   ├── multi_bag_reader.py  # 多 Bag 管理
│   │   └── frame_store.py    # FrameStore 数据层 + 缓存
│   ├── sync/
│   │   └── time_sync.py      # 时间同步（优化版）
│   ├── kpi/
│   │   ├── base_kpi.py       # KPI 基类 + StreamingData
│   │   ├── mileage.py        # 里程统计
│   │   ├── takeover.py       # 接管统计
│   │   ├── lane_keeping.py   # 车道保持
│   │   ├── steering.py       # 转向平滑度
│   │   ├── comfort.py        # 舒适性
│   │   ├── emergency.py      # 紧急事件
│   │   ├── weaving.py        # 画龙检测
│   │   ├── roi_obstacles.py  # ROI障碍物
│   │   ├── curvature.py      # 道路曲率
│   │   └── localization.py   # 定位置信度
│   ├── utils/
│   │   ├── signal.py         # 信号处理（Jerk、滤波）
│   │   ├── geo.py            # 地理坐标转换（UTM）
│   │   └── geometry.py       # 几何计算
│   ├── output/
│   │   └── reporter.py       # 报告生成（多格式）
│   └── proto/
│       └── control_debug_parser.py  # Protobuf 解析
├── tests/                     # 测试脚本
│   └── test_trajectory_info.py  # 轨迹信息测试
├── scripts/                   # 工具脚本
│   ├── merge_db3.py          # Bag 合并
│   └── monitor/              # 定位监控
├── .frame_cache/             # 持久化缓存目录（自动生成）
│   ├── frames_<hash>.db      # SQLite 帧数据
│   └── frames_<hash>.json    # 缓存元数据
├── requirements.txt
├── run_analysis.py           # 快速启动
└── README.md
```

### 核心文件说明

| 文件 | 说明 |
|------|------|
| `src/main.py` | 主入口，包含 `KPIAnalyzer` 类，协调数据加载、同步、计算、报告生成 |
| `src/constants.py` | Topic 常量、轻量消息类 (`LightMessage`)、字段映射 |
| `src/data_loader/frame_store.py` | `FrameStore` 抽象层、`MemoryFrameStore`、`DiskFrameStore`、`FrameStoreCache` |
| `src/kpi/base_kpi.py` | `BaseKPI` 抽象类、`KPIResult` 数据类、`BagMapper` 事件溯源、`StreamingData` |
| `src/sync/time_sync.py` | `TimeSynchronizer` 多 Topic 时间对齐、`SyncedFrame` 数据结构 |

---

## 报告输出顺序

报告按以下类别顺序组织：

1. **里程统计** - 总里程、自动驾驶里程/时间
2. **道路曲率** - 平均曲率、直道/弯道比例
3. **接管统计** - 接管次数、频率
4. **定位置信度** - 正常率、偏移率
5. **安全性** - 急减速、横向猛打、顿挫
6. **车道保持精度** - 横向偏差统计
7. **舒适性** - Jerk统计、综合评分
8. **画龙检测** - 画龙次数、频率
9. **转向平滑度** - 转角速度/加速度统计
10. **ROI障碍物** - 各区域障碍物统计

---

## 性能优化

### 大数据处理策略

| 优化项 | 策略 | 效果 |
|-------|------|------|
| **轻量模式** | 消息转换为仅含必需字段的 LightMessage | 内存减少 60-90% |
| **磁盘缓存** | DiskFrameStore 使用 SQLite + zlib 压缩 | 防止 OOM |
| **分批加载** | 迭代时每批 10,000 帧 + 定期 GC | 内存占用平稳 |
| **持久化缓存** | FrameStoreCache 跨次运行复用 | 二次分析提速 10x+ |
| **并行索引** | Topic 索引构建并行化 | 同步速度提升 2-3x |

### 内存占用参考

| 数据量 | 普通模式 | FrameStore 模式 |
|--------|---------|----------------|
| 10 万帧 | ~1 GB | ~200 MB |
| 50 万帧 | ~5 GB (可能 OOM) | ~400 MB |
| 100 万帧 | OOM | ~600 MB |

### 处理时间参考

| 数据量 | 首次分析 | 缓存命中 |
|--------|---------|---------|
| 10 万帧 | ~30s | ~5s |
| 50 万帧 | ~150s | ~20s |
| 100 万帧 | ~300s | ~40s |

---

## 故障排除

### 常见问题

**Q: 运行报错 `ModuleNotFoundError: No module named 'rclpy'`**

A: 需要先 source ROS2 环境：
```bash
source /opt/ros/humble/setup.bash
source ~/your_ws/install/setup.bash
```

**Q: 分析中途 OOM (Out of Memory)**

A: 尝试以下方案：
1. 使用 `--force-disk` 强制磁盘模式
2. 分割大 bag 文件后逐个处理
3. 增加系统 swap 空间

**Q: 缓存不生效**

A: 检查以下情况：
1. bag 文件内容是否有变化（会导致 hash 不匹配）
2. 缓存目录是否有写权限
3. 使用 `--clear-cache` 清理旧缓存重试

**Q: KPI 计算结果与预期不符**

A: 检查：
1. 定位数据是否有效（status 为 3 或 7）
2. 时间同步容差是否合适（默认 300ms）
3. bag 文件是否包含所需 topic

---

## License

MIT
