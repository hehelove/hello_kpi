# Hello KPI - 自动驾驶 KPI 分析系统

一个基于 ROS2 bag 数据的自动驾驶关键性能指标（KPI）分析工具，支持里程统计、接管检测、舒适性评估、安全性分析等多维度指标计算。

## 目录

- [系统架构](#系统架构)
- [数据处理流程详解](#数据处理流程详解)
- [KPI 指标详解](#kpi-指标详解)
- [配置说明](#配置说明)
- [快速开始](#快速开始)
- [扩展开发](#扩展开发)

---

## 系统架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           KPIAnalyzer (主程序)                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         数据读取层                                    │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                     │   │
│  │  │ BagReader  │  │MultiBagReader│ │FrameStore │                     │   │
│  │  │ 单bag读取   │  │ 多bag管理   │  │ 缓存管理   │                     │   │
│  │  └────────────┘  └────────────┘  └────────────┘                     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         数据同步层                                    │   │
│  │  ┌──────────────────┐     ┌──────────────────┐                      │   │
│  │  │ 高频数据收集      │     │ 低频时间同步       │                      │   │
│  │  │ (~50Hz 原始)     │     │ (10Hz 基准帧)     │                      │   │
│  │  └──────────────────┘     └──────────────────┘                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         KPI 计算层                                    │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │   │
│  │  │ Mileage  │ │ Takeover │ │ Comfort  │ │ Emergency│ │ Steering │  │   │
│  │  │ 里程统计  │ │ 接管统计  │ │ 舒适性    │ │ 紧急事件  │ │ 转向平滑  │  │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘  │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │   │
│  │  │LaneKeep  │ │ Weaving  │ │   ROI    │ │Curvature │ │Localization│ │   │
│  │  │ 车道保持  │ │ 画龙检测  │ │ 障碍物    │ │ 道路曲率  │ │ 定位置信度 │ │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                    │                                        │
│                                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         报告输出层                                    │   │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐            │   │
│  │  │  JSON  │ │  CSV   │ │Markdown│ │  HTML  │ │  Word  │            │   │
│  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘            │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
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

---

## 数据处理流程详解

### 完整数据流

```
ROS2 Bag (.db3)
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 阶段1: 数据读取                                                          │
│                                                                         │
│   BagReader/MultiBagReader                                              │
│        │                                                                │
│        ├─► metadata.yaml 解析 (bag 时间范围)                             │
│        │                                                                │
│        └─► 按 topic 读取消息 → TopicData { timestamps[], messages[] }    │
│                                                                         │
│   支持的 Topic:                                                          │
│   ┌─────────────────────────────┬────────┬──────────────────────────┐  │
│   │ Topic                       │ 频率   │ 用途                      │  │
│   ├─────────────────────────────┼────────┼──────────────────────────┤  │
│   │ /function/function_manager  │ 10Hz   │ 驾驶状态（自动/手动）       │  │
│   │ /localization/localization  │ 50Hz   │ 位置、姿态、定位状态        │  │
│   │ /vehicle/chassis_domain_report│ 50Hz │ 转角、转角速度、加速度、速度 │  │
│   │ /control/control            │ 50Hz   │ 目标纵向加速度             │  │
│   │ /control/debug              │ 50Hz   │ 横向误差、曲率              │  │
│   │ /perception/obstacle_list   │ 10Hz   │ 障碍物感知                  │  │
│   │ /planning/trajectory        │ 10Hz   │ 规划轨迹（曲率 kappa）      │  │
│   └─────────────────────────────┴────────┴──────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 阶段2: 数据同步与收集                                                    │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │ 2a. 高频数据收集 (50Hz，保留原始频率)                             │  │
│   │                                                                 │  │
│   │   /vehicle/chassis_domain_report (50Hz)                         │  │
│   │        │                                                        │  │
│   │        └─► chassis_highfreq: [(steering_angle,                  │  │
│   │                                steering_vel,                     │  │
│   │                                speed,                            │  │
│   │                                lat_acc,                          │  │
│   │                                timestamp), ...]                  │  │
│   │                                                                 │  │
│   │   /control/control (50Hz)                                       │  │
│   │        │                                                        │  │
│   │        └─► control_highfreq: [(lon_acc, timestamp), ...]        │  │
│   │                                                                 │  │
│   │   自动驾驶状态插值:                                               │  │
│   │   ┌─────────────────────────────────────────────────────────┐  │  │
│   │   │ func (10Hz):    ●─────●─────●─────●─────●                │  │  │
│   │   │                  │     │     │     │     │                │  │  │
│   │   │ chassis (50Hz): ●●●●●●●●●●●●●●●●●●●●●●●●●●●●●●           │  │  │
│   │   │                  ↑                                       │  │  │
│   │   │            每个高频点从最近的 func 点插值获取 is_auto       │  │  │
│   │   └─────────────────────────────────────────────────────────┘  │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │ 2b. 低频时间同步 (10Hz，多 Topic 对齐)                           │  │
│   │                                                                 │  │
│   │   基准 Topic: /function/function_manager (10Hz)                 │  │
│   │        │                                                        │  │
│   │        ▼ 对每个基准帧                                            │  │
│   │   ┌─────────────────────────────────────────────────────────┐  │  │
│   │   │ 二分查找其他 Topic 中最近的消息                           │  │  │
│   │   │ 条件：|Δt| ≤ tolerance (默认 300ms)                      │  │  │
│   │   └─────────────────────────────────────────────────────────┘  │  │
│   │        │                                                        │  │
│   │        ▼                                                        │  │
│   │   SyncedFrame: {                                                │  │
│   │       timestamp: float,                                         │  │
│   │       messages: {                                               │  │
│   │           "/function/function_manager": msg,                    │  │
│   │           "/localization/localization": msg,                    │  │
│   │           "/vehicle/chassis_domain_report": msg,                │  │
│   │           ...                                                   │  │
│   │       }                                                         │  │
│   │   }                                                             │  │
│   └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 阶段3: 数据缓存 (可选)                                                   │
│                                                                         │
│   DiskFrameStore (SQLite)                                               │
│                                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │ 表结构:                                                          │  │
│   │                                                                 │  │
│   │ frames (10Hz 同步帧)                                            │  │
│   │ ├─ id, timestamp, bag_name, data (pickle+zlib)                  │  │
│   │                                                                 │  │
│   │ chassis_highfreq (50Hz 底盘高频)                                │  │
│   │ ├─ id, timestamp, bag_name                                      │  │
│   │ ├─ steering_angle, steering_vel, speed, lat_acc, is_auto        │  │
│   │                                                                 │  │
│   │ control_highfreq (50Hz 控制高频)                                │  │
│   │ ├─ id, timestamp, bag_name, lon_acc, is_auto                    │  │
│   │                                                                 │  │
│   │ bag_infos (bag 元信息)                                          │  │
│   │ ├─ id, name, frame_count, start_timestamp, end_timestamp        │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   缓存文件: .frame_cache/frames_<hash>.db + .json                       │
│   缓存版本: 3.0 (支持高频数据)                                           │
└─────────────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 阶段4: KPI 计算                                                          │
│                                                                         │
│   数据源选择策略:                                                         │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                                                                 │  │
│   │   高频 KPI (转向、画龙、舒适性、紧急事件):                         │  │
│   │       优先使用 chassis_highfreq / control_highfreq (50Hz)        │  │
│   │       回退到 synced_frames (10Hz) - 兼容旧缓存                    │  │
│   │                                                                 │  │
│   │   低频 KPI (里程、接管、定位、ROI):                                │  │
│   │       使用 synced_frames (10Hz)                                  │  │
│   │                                                                 │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   信号处理流程 (以横向猛打为例):                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │                                                                 │  │
│   │   steering_angle (50Hz 原始)                                    │  │
│   │        │                                                        │  │
│   │        ▼ Butterworth 低通滤波 (cutoff=10Hz)                      │  │
│   │        │                                                        │  │
│   │   steering_angle_filtered                                       │  │
│   │        │                                                        │  │
│   │        ▼ 差分计算: dθ/dt                                         │  │
│   │        │                                                        │  │
│   │   steering_velocity                                             │  │
│   │        │                                                        │  │
│   │        ▼ Butterworth 低通滤波 (cutoff=15Hz)                      │  │
│   │        │                                                        │  │
│   │   steering_velocity_filtered                                    │  │
│   │        │                                                        │  │
│   │        ▼ 阈值检测 + 持续时间过滤                                  │  │
│   │        │                                                        │  │
│   │   横向猛打事件列表                                                │  │
│   │                                                                 │  │
│   └─────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│   KPI 计算顺序:                                                          │
│   ┌─────────────────────────────────────────────────────────────────┐  │
│   │ 1. 独立 KPI (无依赖):                                            │  │
│   │    里程统计 → 定位置信度 → 道路曲率 → ROI障碍物 → 舒适性 →         │  │
│   │    转向平滑度 → 画龙检测 → 紧急事件                               │  │
│   │                                                                 │  │
│   │ 2. 依赖 KPI:                                                     │  │
│   │    接管统计 (依赖里程) → 车道保持 → 超速检测                       │  │
│   └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ 阶段5: 报告生成                                                          │
│                                                                         │
│   KPIReporter                                                           │
│        │                                                                │
│        ├─► kpi_report.json    (结构化数据)                              │
│        ├─► kpi_report.csv     (表格数据)                                │
│        ├─► kpi_report.md      (Markdown 报告)                           │
│        ├─► kpi_report.html    (HTML 报告)                               │
│        ├─► kpi_report.docx    (Word 报告)                               │
│        └─► kpi_report_trace.json (异常事件溯源)                         │
│                                                                         │
│   Trace 报告结构:                                                        │
│   {                                                                     │
│     "metadata": {...},                                                  │
│     "traces": [                                                         │
│       {                                                                 │
│         "kpi_category": "舒适性",                                       │
│         "indicator": "顿挫次数",                                        │
│         "value": 30,                                                    │
│         "anomalies": [                                                  │
│           {                                                             │
│             "timestamp": 1768377126.59,                                 │
│             "bag_name": "20260114-155157",                              │
│             "description": "顿挫 #1: |2.88|>2.5m/s³，持续213ms",        │
│             "value": {"peak": 2.88, "series": [[0.0, 2.1], [20, 2.88]]} │
│           }                                                             │
│         ]                                                               │
│       }                                                                 │
│     ]                                                                   │
│   }                                                                     │
└─────────────────────────────────────────────────────────────────────────┘
```

### 详细步骤说明

#### Step 1: Bag 文件发现与读取

```python
# MultiBagReader 发现 bag 文件
multi_reader = MultiBagReader(bag_path)
bag_infos = multi_reader.discover_bags()  # 返回按时间排序的 BagInfo 列表

# 每个 BagInfo 包含:
# - path: bag 目录路径
# - start_time: 起始时间戳
# - end_time: 结束时间戳
# - duration: 持续时间

# 读取指定 topic 的数据
topic_data = reader.read_single_bag_data(bag_path, topics_to_read)
# topic_data[topic_name] = TopicData(messages=[], timestamps=[])
```

#### Step 2: 高频数据收集

```python
def _collect_highfreq_data(topic_data, streaming_data):
    """
    从原始 topic 数据收集高频数据（不经过同步下采样）
    
    高频数据用于需要精确信号分析的 KPI:
    - 底盘数据 (~50Hz): 转向平滑度、画龙检测、横向猛打
    - 控制数据 (~50Hz): 舒适性(jerk)、急减速、顿挫
    """
    
    # 1. 获取 func 自动驾驶状态时间序列（用于插值）
    func_timestamps, func_auto_states = extract_func_states(topic_data)
    
    # 2. 收集底盘高频数据
    for msg, ts in chassis_topic:
        steering_angle = msg.eps_system.actual_steering_angle
        steering_vel = msg.eps_system.actual_steering_angle_velocity
        speed = msg.motion_system.vehicle_speed
        lat_acc = msg.motion_system.vehicle_lateral_acceleration
        
        # 插值获取该时刻的自动驾驶状态
        is_auto = interpolate_auto_state(ts, func_timestamps, func_auto_states)
        
        streaming_data.chassis_highfreq.append(
            (steering_angle, steering_vel, speed, lat_acc, ts)
        )
        streaming_data.chassis_auto_states.append((is_auto, ts))
    
    # 3. 收集控制高频数据
    for msg, ts in control_topic:
        lon_acc = msg.chassis_control.target_longitudinal_acceleration
        is_auto = interpolate_auto_state(ts, func_timestamps, func_auto_states)
        
        streaming_data.control_highfreq.append((lon_acc, ts))
        streaming_data.control_auto_states.append((is_auto, ts))
```

#### Step 3: 低频时间同步

```python
def synchronize(topic_data, base_topic, tolerance=0.3):
    """
    以 base_topic 为基准，同步所有其他 topic
    
    同步逻辑:
    - 帧数量 = base_topic 消息数量
    - 对每个 base_topic 帧，二分查找其他 topic 中时间最近的消息
    - 如果时间差 > tolerance，该 topic 在该帧为空
    """
    synced_frames = []
    
    for base_ts in base_topic.timestamps:
        frame = SyncedFrame(timestamp=base_ts)
        
        for topic_name, topic in topic_data.items():
            # 二分查找最近的消息
            idx = bisect_left(topic.timestamps, base_ts)
            
            # 检查前后两个点，选择更近的
            best_idx = find_nearest(topic.timestamps, base_ts, idx)
            
            if abs(topic.timestamps[best_idx] - base_ts) <= tolerance:
                frame.messages[topic_name] = topic.messages[best_idx]
        
        synced_frames.append(frame)
    
    return synced_frames
```

#### Step 4: 信号处理（滤波与差分）

```python
def _apply_lowpass_filter(data, cutoff_hz, fs, order=2):
    """
    Butterworth 低通滤波
    
    Args:
        data: 输入信号
        cutoff_hz: 截止频率 (Hz)
        fs: 采样率 (Hz)
        order: 滤波器阶数 (默认2阶)
    
    注意:
        cutoff_hz 必须 < Nyquist (fs/2)
        否则跳过滤波
    """
    nyquist = fs / 2.0
    normal_cutoff = cutoff_hz / nyquist
    
    if normal_cutoff >= 1.0:
        return data  # 无法滤波
    
    b, a = signal.butter(order, normal_cutoff, btype='low')
    return signal.filtfilt(b, a, data)

def compute_derivative(timestamps, values):
    """
    计算一阶导数（差分）
    
    derivative[i] = (values[i+1] - values[i]) / (timestamps[i+1] - timestamps[i])
    
    注意:
        过滤时间间隔 > 1s 的点（bag 间隔）
    """
    dt = np.diff(timestamps)
    dv = np.diff(values)
    
    valid_mask = (dt > 0) & (dt <= 1.0)
    return timestamps[1:][valid_mask], (dv / dt)[valid_mask]
```

#### Step 5: KPI 计算示例（横向猛打）

```python
def compute_lateral_jerk(streaming_data):
    """
    横向猛打检测
    
    数据流:
    1. 从 chassis_highfreq 获取转角和速度 (50Hz)
    2. 滤波 + 差分得到转角速度
    3. 再次滤波 + 差分得到转角加速度
    4. 阈值检测 + 持续时间过滤
    5. 事件合并
    """
    
    # 1. 筛选自动驾驶状态的数据
    auto_indices = [i for i, (is_auto, _) in enumerate(chassis_auto_states) if is_auto]
    steering_angles = [chassis_highfreq[i][0] for i in auto_indices]
    speeds = [chassis_highfreq[i][2] for i in auto_indices]
    timestamps = [chassis_highfreq[i][4] for i in auto_indices]
    
    # 2. 估计实际采样率
    actual_fs = 1.0 / np.median(np.diff(timestamps))  # 约 50Hz
    
    # 3. 转角滤波
    angle_cutoff = min(10.0, actual_fs / 2 - 0.1)  # 10Hz 或 Nyquist-0.1
    steering_filtered = lowpass_filter(steering_angles, angle_cutoff, actual_fs)
    
    # 4. 差分计算转角速度
    ts_vel, steering_vel = compute_derivative(timestamps, steering_filtered)
    
    # 5. 转角速度滤波
    rate_cutoff = min(15.0, actual_fs / 2 - 0.1)  # 15Hz 或 Nyquist-0.1
    steering_vel_filtered = lowpass_filter(steering_vel, rate_cutoff, actual_fs)
    
    # 6. 差分计算转角加速度
    ts_acc, steering_acc = compute_derivative(ts_vel, steering_vel_filtered)
    
    # 7. 检测事件
    # 转角速度超限检测
    vel_events = detect_threshold_events(
        steering_vel_filtered, 
        threshold=get_threshold(speed),  # 速度自适应阈值
        min_duration=0.1  # 100ms
    )
    
    # 转角加速度超限检测
    acc_events = detect_threshold_events(
        steering_acc,
        threshold=get_acc_threshold(speed),  # 速度自适应阈值
        min_duration=0.1  # 100ms
    )
    
    # 8. 合并相邻事件 (gap < 300ms)
    all_events = merge_events(vel_events + acc_events, merge_gap=0.3)
    
    return all_events
```

---

## KPI 指标详解

### 数据源与计算方式汇总

| KPI | Topic | 字段 | 频率 | 计算方式 |
|-----|-------|------|------|---------|
| 里程统计 | `/localization`, `/function` | `position.lat/lon`, `operator_type` | 10Hz | Haversine 距离累加 |
| 接管统计 | `/function` | `operator_type` | 10Hz | 状态跳变检测 |
| 定位置信度 | `/localization` | `status`, `position_stddev` | 10Hz | 状态分布统计 |
| 车道保持 | `/control/debug` | `lateral_error` | 10Hz | 横向误差统计 |
| 转向平滑度 | `/vehicle/chassis` | `steering_angle` | **50Hz** | 滤波+差分→RMS |
| 舒适性 | `/control/control` | `target_lon_acceleration` | **50Hz** | 差分→Jerk统计 |
| 紧急事件 | `/vehicle/chassis`, `/control` | `steering_angle`, `lon_acc` | **50Hz** | 滤波+差分→阈值检测 |
| 画龙检测 | `/vehicle/chassis` | `steering_angle` | **50Hz** | 滤波+差分→有效过零点 |
| ROI障碍物 | `/perception/obstacle` | `obstacles[]` | 10Hz | 区域统计 |
| 道路曲率 | `/planning/trajectory` | `path_point.kappa` | 10Hz | 曲率分布统计 |

### 1. 里程统计 (MileageKPI)

**数据源**:
- `/localization/localization.global_localization.position.latitude/longitude`
- `/function/function_manager.operator_type`

**计算逻辑**:
```
1. 提取帧序列: [(lat, lon, is_auto, timestamp), ...]
2. 过滤无效位置: lat=0 或超出 [-90,90]
3. 分段: 状态变化或时间间隔>1s 时切分
4. Haversine 公式计算距离:
   d = 2R × arcsin(√(sin²(Δlat/2) + cos(lat1)×cos(lat2)×sin²(Δlon/2)))
5. 累加并按 is_auto 分类
```

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 总里程 | km | 所有行驶里程 |
| 自动驾驶里程 | km | operator_type=2 状态下的里程 |
| 自动驾驶时间 | min | 累计自动驾驶时长 |
| 自动驾驶平均速度 | km/h | 自动驾驶里程 / 时间 |

---

### 2. 接管统计 (TakeoverKPI)

**接管定义**: `operator_type: 2(自动) → 非2(手动)`

**有效接管条件**: 自动驾驶持续时间 ≥ 1.0s

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 总接管次数 | 次 | 有效接管次数 |
| 每百公里接管次数 | 次/百公里 | 归一化频率 |
| 平均接管里程 | km/次 | 自动里程 / 接管次数 |
| 平均接管间隔 | min | 两次接管的时间间隔 |

---

### 3. 舒适性 (ComfortKPI) + 紧急事件 (EmergencyEventsKPI)

#### 3.1 顿挫检测

**数据源**: `/control/control.chassis_control.target_longitudinal_acceleration`

**为什么用目标加速度**: 实际底盘加速度噪声太大，目标加速度更平滑

**计算流程**:
```
target_lon_acc (50Hz)
      │
      ▼ 差分: d(acc)/dt
      │
    jerk
      │
      ▼ 逐点判断: |jerk| > threshold(speed)?
      │
      ├─ 低速 (<10km/h): 1.5 m/s³
      ├─ 中速 (10-40km/h): 2.0 m/s³
      └─ 高速 (>40km/h): 2.5 m/s³
      │
      ▼ 持续时间 >= 200ms?
      │
      ▼ 相邻事件合并 (gap < 400ms)
      │
    顿挫事件
```

#### 3.2 急减速检测

**条件**: `acceleration < -threshold(speed)` 且持续 ≥ 100ms

**速度自适应阈值**:
| 速度 | 阈值 |
|------|------|
| 低速 | -2.5 m/s² |
| 中速 | -3.0 m/s² |
| 高速 | -3.5 m/s² |

#### 3.3 横向猛打检测

**数据源**: `/vehicle/chassis_domain_report.eps_system.actual_steering_angle`

**计算流程**:
```
steering_angle (50Hz)
      │
      ▼ Butterworth 低通滤波 (10Hz)
      │
      ▼ 差分: dθ/dt
      │
steering_velocity
      │
      ▼ Butterworth 低通滤波 (15Hz)
      │
      ▼ 差分: d(dθ/dt)/dt
      │
steering_acceleration
      │
      ▼ 检测:
      ├─ |steering_vel| > threshold(speed)?  (转角速度超限)
      └─ |steering_acc| > threshold(speed)?  (转角加速度超限)
      │
      ▼ 持续时间 >= 100ms?
      │
      ▼ 相邻事件合并 (gap < 300ms)
```

**速度自适应阈值 (转角速度)**:
| 速度 | 阈值 |
|------|------|
| 0-10 km/h | 200 °/s |
| 10-20 km/h | 180 °/s |
| 20-30 km/h | 160 °/s |
| 30-40 km/h | 140 °/s |
| 40-50 km/h | 120 °/s |
| 50-60 km/h | 100 °/s |
| 60-70 km/h | 80 °/s |
| 70+ km/h | 60 °/s |

**速度自适应阈值 (转角加速度)**:
| 速度 | 阈值 |
|------|------|
| 0-10 km/h | 600 °/s² |
| 10-30 km/h | 400 °/s² |
| 30-60 km/h | 250 °/s² |
| 60-80 km/h | 200 °/s² |
| 80+ km/h | 150 °/s² |

---

### 4. 转向平滑度 (SteeringSmoothnessKPI)

**数据源**: `/vehicle/chassis_domain_report.eps_system.actual_steering_angle`

**计算流程**:
```
steering_angle (50Hz)
      │
      ▼ Butterworth 低通滤波 (10Hz)
      │
      ▼ 差分: dθ/dt
      │
steering_velocity
      │
      ▼ Butterworth 低通滤波 (15Hz)
      │
      ▼ 1s 滑动窗口 RMS: √(mean(vel²))
      │
      ▼ 按速度区间分段统计
```

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 转角速度RMS均值 | °/s | 滑动窗口内转角速度RMS的平均值 |
| 转角速度RMS最大 | °/s | 最大值 |
| 转角速度P95 | °/s | 95分位数 |
| 转角速度符号翻转率 | 次/秒 | 转角速度符号变化频率 |
| 转角速度RMS(0-10km/h) | °/s | 分速度区间统计 |

---

### 5. 画龙检测 (WeavingKPI)

**画龙定义**: 行驶时方向盘发生周期性横向振荡

**检测条件** (同时满足):
```
┌─────────────────────────────────────────────────────────────┐
│  ① 场景过滤                                                  │
│     • 90% 以上的点速度 >= 5 km/h                            │
│                                                             │
│  ② 振荡检测（有效过零点）                                     │
│     • 窗口(5s)内有效过零点 >= 4次（2个完整周期）              │
│     • 有效过零：过零前后0.5s内峰值都 >= 10°/s                │
│                                                             │
│  ③ 幅度要求                                                  │
│     • 转角峰谷差 >= 40°                                      │
│     • 转角速度 RMS >= 10°/s                                  │
│                                                             │
│  ④ 排除正常转弯                                              │
│     • 单向变化比例 > 80% → 是转弯不是振荡                    │
│                                                             │
│  ⑤ 过滤变道                                                  │
│     • 净横向位移 >= 0.8m 且 累积/净位移比 < 1.5 → 是变道     │
│                                                             │
│  ⑥ 持续性                                                    │
│     • 持续时间 >= 2 秒                                       │
│     • 振荡周期 >= 2                                          │
└─────────────────────────────────────────────────────────────┘
```

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 画龙次数 | 次 | 检测到的画龙事件数 |
| 画龙频率 | 次/百公里 | 归一化频率 |
| 平均画龙里程 | km/次 | 每多少公里画龙一次 |

---

### 6. 车道保持精度 (LaneKeepingKPI)

**数据源**: `/control/debug` (需 Protobuf 解析)

**字段**: `lateral_error` (横向误差, 米)

**直道/弯道判定**: 基于 `/planning/trajectory.path_point.kappa`
- 直道: |kappa| < 0.003
- 弯道: |kappa| >= 0.003

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 全路段横向偏差均值 | cm | 所有帧的平均值 |
| 横向偏差P95 | cm | 95分位数 |
| 直道保持精度 | cm | 直道的平均偏差 |
| 弯道保持精度 | cm | 弯道的平均偏差 |
| 横向偏差超限率 | % | |偏差| > 20cm 的比例 |

---

### 7. 定位置信度 (LocalizationKPI)

**数据源**: 
- `/localization/localization.status.common`
- `/localization/localization.global_localization.position_stddev`

**状态判定**:
```python
if status in [3, 7]:  # 有效状态
    max_stddev = max(stddev_east, stddev_north)
    if max_stddev > 1.0:
        return "定位偏移"
    elif max_stddev > 0.2:
        return "可能偏移"
    else:
        return "正常"
```

**输出指标**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 定位正常率 | % | 状态为"正常"的占比 |
| 定位偏移率 | % | stddev > 1.0m 的占比 |
| 可能偏移率 | % | 0.2m < stddev ≤ 1.0m 的占比 |
| 平均位置标准差 | m | max(east, north) 的平均值 |

---

### 8. ROI 障碍物统计 (ROIObstaclesKPI)

**三级 ROI 设计**:
```
┌─────────────────────────────────────────────────────────────┐
│                       远距离 (Far)                          │
│                    前60m/后25m/左右各7m                      │
│  ┌─────────────────────────────────────────────────────┐   │
│  │                    中距离 (Mid)                      │   │
│  │                 前25m/后10m/左右各5m                 │   │
│  │  ┌───────────────────────────────────────────────┐  │   │
│  │  │           近距离 (Near) - 圆形半径10m        │  │   │
│  │  │               ●(自车)                        │  │   │
│  │  └───────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

**输出指标 (每个 ROI)**:
| 指标 | 单位 | 说明 |
|------|------|------|
| 平均障碍物数 | 个/帧 | 所有帧的平均值 |
| 动态障碍物均值 | 个/帧 | motion_status=1 |
| 静态障碍物均值 | 个/帧 | motion_status=3 |
| 零障碍物帧占比 | % | 该ROI内无障碍物的比例 |

---

### 9. 道路曲率统计 (CurvatureKPI)

**数据源**: `/planning/trajectory.path_point[].kappa`

**曲率分类**:
| 类型 | 曲率范围 | 典型半径 |
|------|---------|---------|
| 直道 | < 0.003 | > 333m |
| 小弯道 | [0.003, 0.01) | 100-333m |
| 中弯道 | [0.01, 0.05) | 20-100m |
| 大弯道 | >= 0.05 | < 20m |

**地图可视化**: 使用 Mapbox 生成交互式 HTML 地图，不同曲率用不同颜色标记

---

## 配置说明

配置文件：`config/kpi_config.yaml`

### 基础配置

```yaml
base:
  sync_tolerance: 0.3   # 时间同步容差 (秒)
  base_frequency: 10    # 基准频率 (Hz)

ego_vehicle:
  front_length: 3.79    # 后轴到车头距离 (m)
  rear_length: 0.883    # 后轴到车尾距离 (m)
  left_width: 0.945     # 后轴到左侧距离 (m)
  right_width: 0.945    # 后轴到右侧距离 (m)
```

### KPI 阈值配置

```yaml
kpi:
  # 转向平滑度 / 横向猛打
  steering:
    min_duration: 0.1       # 最小持续时间 (秒)
    merge_gap: 0.3          # 事件合并间隔 (秒)
    angle_filter_cutoff: 10.0   # 转角滤波截止频率 (Hz)
    rate_filter_cutoff: 15.0    # 转角速度滤波截止频率 (Hz)
    speed_thresholds:       # 转角速度阈值
      0: 200
      10: 180
      # ...
    steering_acc_thresholds:  # 转角加速度阈值
      0: 600
      10: 400
      # ...

  # 顿挫
  jerk_event:
    jerk_threshold: 2.5     # m/s³
    min_duration: 0.2       # 200ms
    merge_gap: 0.4          # 400ms 合并

  # 画龙
  weaving:
    min_speed: 5.0                    # km/h
    min_steering_amplitude: 40.0      # 转角峰谷差 (°)
    min_steering_vel_rms: 10.0        # 转角速度RMS (°/s)
    min_zero_crossings: 4             # 有效过零点数
    event_merge_gap: 0.5              # 事件合并间隔 (秒)
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
# 流式模式（推荐，支持大数据）
python run_analysis.py /path/to/ros2bag -s

# 指定输出目录
python run_analysis.py /path/to/ros2bag -s -o ./my_output

# 使用自定义配置
python run_analysis.py /path/to/ros2bag -s -c ./my_config.yaml

# 禁用缓存
python run_analysis.py /path/to/ros2bag -s --no-cache

# 清理缓存后重新分析
python run_analysis.py /path/to/ros2bag -s --clear-cache
```

### 命令行参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `bag_path` | ROS2 bag 路径 | 必需 |
| `-s, --streaming` | 流式模式（推荐） | False |
| `-o, --output` | 输出目录 | `./output` |
| `-c, --config` | 配置文件路径 | `config/kpi_config.yaml` |
| `--no-cache` | 禁用持久化缓存 | False |
| `--clear-cache` | 清理缓存后重新分析 | False |
| `--cache-dir` | 缓存目录 | `.frame_cache` |

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
│   │   └── time_sync.py      # 时间同步
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
│   │   ├── localization.py   # 定位置信度
│   │   └── speeding.py       # 超速检测
│   ├── utils/
│   │   ├── signal.py         # 信号处理（滤波、Jerk）
│   │   ├── geo.py            # 地理坐标转换（UTM）
│   │   └── map_visualizer.py # 地图可视化
│   ├── output/
│   │   └── reporter.py       # 报告生成（多格式）
│   └── proto/
│       └── control_debug_parser.py  # Protobuf 解析
├── .frame_cache/             # 持久化缓存目录（自动生成）
├── requirements.txt
├── run_analysis.py           # 快速启动
└── README.md
```

---

## 性能优化

### 处理策略

| 优化项 | 策略 | 效果 |
|-------|------|------|
| **流式处理** | 边读取边计算，不全量加载 | 内存占用稳定 |
| **高频数据直接使用** | 50Hz 数据不下采样 | 信号精度提升 |
| **磁盘缓存** | SQLite + zlib 压缩 | 二次分析提速 10x+ |
| **分批加载** | 迭代时每批 5000 帧 + GC | 防止 OOM |

### 内存占用参考

| 数据量 | 流式模式 |
|--------|---------|
| 10 万帧 | ~300 MB |
| 50 万帧 | ~500 MB |
| 100 万帧 | ~700 MB |

---

## License

MIT
