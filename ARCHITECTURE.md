# Hello KPI 架构梳理与优化建议

本文基于仓库现有实现与 README 中的流程说明整理，覆盖模块职责、核心数据流、运行模式与关键配置，并给出面向 Linux + ROS2 的优化建议清单。

## 1. 项目定位与职责

Hello KPI 是一个基于 ROS2 bag 数据的 KPI 分析工具，目标是对自动驾驶数据进行离线统计与异常溯源，输出多格式报告（JSON/CSV/MD/HTML/Word）。

## 2. 目录结构与模块职责

- `run_analysis.py`: 入口脚本，直接调用 `src/main.py` 的 `main()`。
- `src/main.py`: 主流程编排与模式选择（普通/FrameStore/流式）。
- `config/kpi_config.yaml`: KPI 阈值、同步容差、车辆几何等参数。
- `src/data_loader/`: ROS2 bag 数据读取与缓存管理。
  - `bag_reader.py`: 单 bag 读取。
  - `multi_bag_reader.py`: 多 bag 管理与合并。
  - `frame_store.py`: 10Hz 同步帧存储与缓存（内存/磁盘）。
- `src/sync/time_sync.py`: 以基准 topic 做多 topic 时间对齐。
- `src/kpi/`: KPI 计算器集合（里程、接管、舒适性、紧急事件等）。
  - `base_kpi.py`: KPI 基类、结果模型、异常溯源结构、StreamingData。
- `src/utils/`: 信号处理、几何/地图相关工具。
- `src/proto/`: 控制调试信息解析。
- `src/output/`: 报告生成（README 中说明，代码在 `src/output/`）。
- `tests/`: 单元测试入口（KPI 和轨迹信息）。

## 3. 核心数据流（高频 + 低频并行）

### 3.1 数据读取

1) `KPIAnalyzer` 选择模式后初始化 KPI 列表，并收集所需 topics。  
2) `BagReader/MultiBagReader` 按 topic 读取消息并给出时间序列。  
3) 支持单 bag、多 bag、单目录多 db3 的结构识别。

### 3.2 高频数据收集（~50Hz）

从 `/vehicle/chassis_domain_report` 与 `/control/control` 直接收集高频数据，不经过时间同步下采样，用于转向平滑度、画龙、舒适性、紧急事件等高频 KPI。

### 3.3 低频时间同步（10Hz）

以 `/function/function_manager` 为基准，将其它 topic 对齐到 10Hz 同步帧，容差默认 300ms，用于里程、接管、定位、ROI 等低频 KPI。

### 3.4 KPI 计算与依赖

- KPI 以 `BaseKPI` 规范化接口实现。
- 无依赖 KPI 可先并行/顺序计算；有依赖 KPI（如接管依赖里程）在第二阶段计算。
- 支持异常溯源：KPI 结果中包含 `AnomalyRecord`，可映射到 bag 文件名与时间段。

### 3.5 报告输出

`KPIReporter` 统一生成结构化与可读性报告，并输出 trace 用于事件溯源。

## 4. 运行模式概览

### 4.1 默认模式（自动选择）

`analyze()` 根据数据量与文件结构选择：

- 多文件或大数据量（>20万帧） → `analyze_with_frame_store`  
- 其它场景 → 直接内存同步 + KPI 计算

### 4.2 FrameStore 模式（推荐）

将同步帧写入 `FrameStore`，支持内存/磁盘，避免 OOM；并可持久化缓存，二次分析加速。

### 4.3 真流式模式

按 bag 逐个读取与同步，立刻调用 `collect()` 收集中间数据，适合超大规模数据集。  
限制：不支持需要完整帧集合的 KPI（会给出警告）。

## 5. 配置要点

- `base.sync_tolerance`: 多 topic 对齐容差（秒）。
- `base.base_frequency`: 基准同步频率（Hz）。
- `kpi.*`: 各 KPI 阈值、持续时间、合并窗口等。
- `ego_vehicle.*`: 车辆尺寸参数（ROI/几何相关 KPI）。

## 6. 关键扩展点

- 新增 KPI：在 `src/kpi/` 继承 `BaseKPI` 并加入 `KPIAnalyzer._init_kpi_calculators`。
- 新增 topic：在 KPI 的 `required_topics` 中声明，并确保 data loader 支持读取。
- 扩展输出：在 `src/output/` 中添加 reporter 输出格式。

## 7. 优化建议（面向稳定性与性能）

### 7.1 数据一致性与健壮性

1) **topic 完整性检查**：在读取阶段输出必需 topic 缺失清单，并在报告元数据中记录，避免静默降级导致误判。
2) **时间同步质量监控**：统计各 topic 在同步帧中的“有效覆盖率”（匹配成功比例），低覆盖率时给出显式告警。
3) **异常溯源补全**：当 `bag_infos` 缺失或时间范围不连续时，记录 gap 信息到 trace 元数据，便于解释异常分布。

### 7.2 性能与资源

4) **采样率自适应上限**：高频 KPI 依赖实际采样率估计，建议在采样率过低时降低阈值或提示“采样率不足”，避免误检。
5) **缓存键版本化**：在缓存元数据中加入 `config` 与 `code version` 指纹（如 Git commit/配置摘要），防止配置变更导致缓存污染。
6) **并行计算策略**：独立 KPI 可以并行执行，但要限定最大线程数并避免重度占用内存的 KPI 同时运行。

### 7.3 可维护性

7) **统一 KPI 依赖声明**：使用显式依赖图（拓扑排序）替代当前简单排序策略，避免新增 KPI 时出现隐含依赖。
8) **配置校验**：对关键阈值（如 cutoff、merge_gap、min_duration）做范围检查，避免无效配置导致静默失效。
9) **测试覆盖**：补充采样率偏低、topic 缺失、跨 bag 时间间隔等边界条件的单元测试。

## 8. 风险与注意事项

- ROS2 bag 时间戳不连续会影响差分与过滤结果，需要在 KPI 中明确处理“时间跳变”。
- 高/低频数据混用时要保证时间对齐基准一致，避免在不同频率间混算指标。
- 磁盘缓存场景应关注 IO 与 CPU 竞争，避免与其他分析任务同时运行造成干扰。
