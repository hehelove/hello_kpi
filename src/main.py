"""
Hello KPI - 自动驾驶KPI分析主程序
"""
import argparse
import sys
import os
import logging
import time
from pathlib import Path
from typing import Optional, List, Dict, Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import yaml

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.data_loader import BagReader
from src.data_loader.multi_bag_reader import MultiBagReader
from src.data_loader.frame_store import (
    FrameStore, MemoryFrameStore, DiskFrameStore,
    create_frame_store, estimate_frame_count, BagInfo as FrameStoreBagInfo,
    FrameStoreCache
)
from src.sync import TimeSynchronizer
from src.kpi import (
    BaseKPI, KPIResult,
    MileageKPI, TakeoverKPI, LaneKeepingKPI,
    SteeringSmoothnessKPI, ComfortKPI, EmergencyEventsKPI,
    WeavingKPI, ROIObstaclesKPI, LocalizationKPI, CurvatureKPI,
    SpeedingKPI
)
from src.kpi.base_kpi import merge_kpi_results, StreamingData
from src.output import KPIReporter
from src.proto import ControlDebugParser
from src.constants import KPINames, KPIStatus, Topics, ConfigKeys

# 配置日志
logger = logging.getLogger(__name__)


@dataclass
class SimpleBagInfo:
    """简化的 Bag 信息（用于单 bag 模式的异常溯源）"""
    path: Path
    start_time: float
    end_time: float


@dataclass
class KPIComputeResult:
    """KPI 计算结果包装"""
    kpi_name: str
    status: str = KPIStatus.SUCCESS
    results: List[KPIResult] = field(default_factory=list)
    error: Optional[str] = None
    duration_ms: float = 0.0  # 计算耗时（毫秒）
    
    def to_dict(self) -> Dict:
        return {
            'kpi_name': self.kpi_name,
            'status': self.status,
            'results': [r.to_dict() for r in self.results],
            'error': self.error,
            'duration_ms': self.duration_ms
        }


@dataclass
class TimingStats:
    """耗时统计"""
    step_timings: Dict[str, float] = field(default_factory=dict)  # 各步骤耗时（秒）
    kpi_timings: Dict[str, float] = field(default_factory=dict)   # 各 KPI 耗时（秒）
    total_time: float = 0.0  # 总耗时（秒）
    
    def add_step(self, step_name: str, duration: float):
        """添加步骤耗时"""
        self.step_timings[step_name] = duration
    
    def add_kpi(self, kpi_name: str, duration: float):
        """添加 KPI 耗时"""
        self.kpi_timings[kpi_name] = duration
    
    def to_dict(self) -> Dict:
        return {
            'total_time_s': round(self.total_time, 3),
            'step_timings': {k: round(v, 3) for k, v in self.step_timings.items()},
            'kpi_timings': {k: round(v, 3) for k, v in self.kpi_timings.items()}
        }
    
    def print_summary(self):
        """打印耗时统计摘要"""
        print(f"\n{'='*60}")
        print("耗时统计")
        print(f"{'='*60}")
        
        # 步骤耗时
        print("\n[步骤耗时]")
        for step, duration in self.step_timings.items():
            pct = (duration / self.total_time * 100) if self.total_time > 0 else 0
            print(f"  {step}: {duration:.2f}s ({pct:.1f}%)")
        
        # KPI 耗时（按耗时排序）
        if self.kpi_timings:
            print("\n[KPI 耗时] (按耗时排序)")
            sorted_kpis = sorted(self.kpi_timings.items(), key=lambda x: x[1], reverse=True)
            kpi_total = sum(self.kpi_timings.values())
            for kpi_name, duration in sorted_kpis:
                pct = (duration / kpi_total * 100) if kpi_total > 0 else 0
                print(f"  {kpi_name}: {duration:.3f}s ({pct:.1f}%)")
        
        # 总耗时
        print(f"\n[总耗时] {self.total_time:.2f}s ({self.total_time/60:.2f}min)")


class KPIAnalyzer:
    """KPI分析器"""
    
    # 默认进度回调
    ProgressCallback = Callable[[int, int, str], None]
    
    def __init__(self, config_path: Optional[str] = None, 
                 progress_callback: Optional[ProgressCallback] = None,
                 max_workers: int = 8,
                 use_cache: bool = True,
                 cache_dir: str = ".frame_cache"):
        """
        初始化分析器
        
        Args:
            config_path: 配置文件路径
            progress_callback: 进度回调函数 (step, total, message)
            max_workers: 并行计算 KPI 的最大线程数
            use_cache: 是否启用持久化缓存（缓存 synced_frames）
            cache_dir: 缓存目录
        """
        # 加载配置
        if config_path is None:
            config_path = PROJECT_ROOT / "config" / "kpi_config.yaml"
        
        self.config = self._load_config(config_path)
        self.max_workers = max_workers
        self.progress_callback = progress_callback or self._default_progress
        self.use_cache = use_cache
        self.frame_cache = FrameStoreCache(cache_dir) if use_cache else None
        
        # 初始化组件（延迟初始化）
        self.bag_reader = None
        self.synchronizer: Optional[TimeSynchronizer] = None
        self.reporter: Optional[KPIReporter] = None
        self.control_parser: Optional[ControlDebugParser] = None
        
        # KPI计算器列表
        self.kpi_calculators: List[BaseKPI] = []
        
        # 分析结果
        self._topic_data: Dict = {}
        self._synced_frames: List = []
        self._parsed_debug_data: Dict = {}
        self._kpi_results: Dict[str, KPIComputeResult] = {}
        self._current_bag_path: str = ""
        
        # 多 bag 支持
        self._is_multi_bag: bool = False
        self._bag_infos: List = []  # 各 bag 的信息
        
        # 耗时统计
        self._timing_stats: TimingStats = TimingStats()
    
    def _default_progress(self, step: int, total: int, message: str):
        """默认进度输出"""
        print(f"[{step}/{total}] {message}")
    
    def _load_config(self, config_path) -> Dict:
        """加载配置文件"""
        config_path = Path(config_path)
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                logger.info(f"配置文件已加载: {config_path}")
                return config or {}
        else:
            logger.warning(f"配置文件未找到: {config_path}")
            return {}
    
    def _init_kpi_calculators(self):
        """初始化KPI计算器"""
        self.kpi_calculators = [
            MileageKPI(self.config),
            TakeoverKPI(self.config),
            LaneKeepingKPI(self.config),
            SteeringSmoothnessKPI(self.config),
            ComfortKPI(self.config),
            EmergencyEventsKPI(self.config),
            WeavingKPI(self.config),
            ROIObstaclesKPI(self.config),
            LocalizationKPI(self.config),
            CurvatureKPI(self.config),
            SpeedingKPI(self.config)
        ]
    
    def _get_required_topics(self) -> List[str]:
        """获取所有需要的topic"""
        topics = set()
        for kpi in self.kpi_calculators:
            topics.update(kpi.required_topics)
        return list(topics)
    
    def _sort_kpis_by_dependency(self) -> tuple:
        """
        按依赖关系对 KPI 进行分组
        
        Returns:
            (independent_kpis, dependent_kpis_ordered)
            - independent_kpis: 无依赖的 KPI（可并行计算）
            - dependent_kpis_ordered: 有依赖的 KPI（按依赖顺序）
        """
        independent = []
        dependent = []
        
        for kpi in self.kpi_calculators:
            if not kpi.dependencies:
                independent.append(kpi)
            else:
                dependent.append(kpi)
        
        # 对有依赖的 KPI 按依赖排序（简单实现：里程统计优先）
        # 更复杂的场景可以使用拓扑排序
        dependent.sort(key=lambda k: 0 if KPINames.MILEAGE in k.dependencies else 1)
        
        return independent, dependent
    
    # ========== 拆分后的方法 ==========
    
    def _step1_init_components(self, bag_path: str, output_dir: str, multi_bag: bool):
        """
        步骤1: 初始化组件
        """
        self.progress_callback(1, 6, "初始化...")
        self._init_kpi_calculators()
        
        bag_path_obj = Path(bag_path)
        is_multi_bag = False
        bag_list = []
        
        # 检测是单个bag还是多bag目录
        # 条件：显式指定 multi_bag、或没有 metadata.yaml（说明是多bag子目录结构）
        if multi_bag or (bag_path_obj.is_dir() and not (bag_path_obj / "metadata.yaml").exists()):
            print(f"  检测到多bag模式，扫描目录...")
            self.bag_reader = MultiBagReader(bag_path)
            is_multi_bag = True
            bag_list = self.bag_reader.get_bag_list()
            print(f"  找到 {len(bag_list)} 个bag文件")
            
            # 检测 bag 之间是否有间隔（用于日志提示）
            self._bag_infos = self.bag_reader.get_bag_infos()
            has_gaps = self.bag_reader.has_gaps(max_gap_seconds=1.0)
            
            if has_gaps:
                gaps = self.bag_reader.get_gaps()
                print(f"  ⚠️  检测到 bag 之间有间隔（KPI 计算会自动过滤间隔点）")
                for i1, i2, gap in gaps:
                    if gap > 1.0:
                        print(f"      bag[{i1+1}] -> bag[{i2+1}]: 间隔 {gap:.1f}s")
        else:
            self.bag_reader = BagReader(bag_path)
            bag_list = [bag_path]
        
        self._is_multi_bag = is_multi_bag
        self.reporter = KPIReporter(output_dir)
        
        return is_multi_bag, bag_list
    
    def _step2_read_bag_info(self, bag_path: str, is_multi_bag: bool, bag_list: List[str]):
        """
        步骤2: 读取Bag信息
        """
        self.progress_callback(2, 6, "读取Bag信息...")
        
        bag_info = self.bag_reader.get_topic_info()
        start_time, end_time = self.bag_reader.get_time_range()
        duration = self.bag_reader.get_duration()
        
        print(f"  - 时间范围: {start_time:.2f} - {end_time:.2f}")
        print(f"  - 持续时间: {duration:.2f}秒 ({duration/3600:.2f}小时)")
        print(f"  - Topic数量: {len(bag_info)}")
        if is_multi_bag:
            print(f"  - Bag文件数: {len(bag_list)}")
        else:
            # 单 bag 模式：为每个 .db3 文件创建独立的 BagInfo（用于异常溯源精确定位）
            db3_infos = self.bag_reader.get_db3_file_infos()
            if db3_infos and len(db3_infos) > 1:
                # 多个 db3 文件：为每个创建独立 BagInfo
                self._bag_infos = [
                    SimpleBagInfo(
                        path=info['path'],
                        start_time=info['start_time'],
                        end_time=info['end_time']
                    )
                    for info in db3_infos
                ]
                print(f"  - DB3文件数: {len(db3_infos)} 个（用于异常溯源）")
            elif db3_infos:
                # 单个 db3 文件
                self._bag_infos = [SimpleBagInfo(
                    path=db3_infos[0]['path'],
                    start_time=start_time,
                    end_time=end_time
                )]
            else:
                # 备选方案
                self._bag_infos = [SimpleBagInfo(
                    path=Path(bag_path),
                    start_time=start_time,
                    end_time=end_time
                )]
        
        # 设置报告元数据
        self.reporter.set_metadata(
            bag_path=bag_path,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            is_multi_bag=is_multi_bag,
            bag_list=bag_list
        )
        
        return bag_info
    
    def _step3_read_topic_data(self, bag_info: Dict):
        """
        步骤3: 读取Topic数据
        """
        self.progress_callback(3, 6, "读取Topic数据...")
        
        required_topics = self._get_required_topics()
        available_topics = set(bag_info.keys())
        topics_to_read = [t for t in required_topics if t in available_topics]
        missing_topics = [t for t in required_topics if t not in available_topics]
        
        if missing_topics:
            logger.warning(f"以下topic不存在: {missing_topics}")
            print(f"  警告: 以下topic不存在: {missing_topics}")
        
        # 【内存优化】估算内存占用
        if self._is_multi_bag and hasattr(self.bag_reader, 'estimate_memory_usage'):
            mem_estimate = self.bag_reader.estimate_memory_usage(topics_to_read)
            print(f"  [内存估算] 预计消息数: {mem_estimate['total_messages']:,}")
            print(f"  [内存估算] 预计内存占用: ~{mem_estimate['estimated_memory_mb']:.0f} MB")
            if mem_estimate.get('warning'):
                print(f"  ⚠️  {mem_estimate['warning']}")
        
        # 获取采样配置（用于内存优化）
        sample_interval = self.config.get('base', {}).get('sample_interval', None)
        if sample_interval:
            print(f"  [降采样] 采样间隔: {sample_interval}s")
        
        # 读取 topic 数据（多 bag 会自动合并）
        light_mode = self.config.get('base', {}).get('light_mode', False)
        mode_str = "[轻量模式] " if light_mode else ""
        print(f"  {mode_str}读取 {len(topics_to_read)} 个topic...")
        self._topic_data = self.bag_reader.read_topics(
            topics_to_read, 
            progress=True,
            sample_interval=sample_interval,
            light_mode=light_mode
        )
        
        # 打印读取统计
        for topic, data in self._topic_data.items():
            print(f"    - {topic}: {len(data.messages)} 条消息")
    
    def _synchronize_topic_data(self, topic_data: Dict) -> List:
        """
        对 topic 数据进行时间同步
        
        Args:
            topic_data: topic 数据字典
            
        Returns:
            同步帧列表
        """
        base_topic = self.config.get(ConfigKeys.TOPICS, {}).get(
            'function_manager', Topics.FUNCTION_MANAGER)
        
        synchronizer = TimeSynchronizer(
            base_topic=base_topic,
            tolerance=self.config.get(ConfigKeys.BASE, {}).get(
                ConfigKeys.SYNC_TOLERANCE, 0.3)
        )
        synchronizer.add_topic_data(topic_data)
        return synchronizer.synchronize(use_global_timestamp=True)
    
    def _step4_synchronize_data(self):
        """
        步骤4: 时间同步
        """
        self.progress_callback(4, 6, "时间同步...")
        
        # 时间同步（多 bag 数据已合并）
        self._synced_frames = self._synchronize_topic_data(self._topic_data)
        print(f"  同步帧数: {len(self._synced_frames)}")
        valid_frames = [f for f in self._synced_frames if f.valid]
        print(f"  有效帧数: {len(valid_frames)}")
    
    def _compute_single_kpi(self, kpi: BaseKPI, extra_kwargs: Dict, 
                            synced_frames: List = None) -> KPIComputeResult:
        """
        计算单个 KPI（线程安全）
        
        Args:
            kpi: KPI 计算器
            extra_kwargs: 额外参数
            synced_frames: 同步帧列表，如果为 None 则使用 self._synced_frames
        """
        frames = synced_frames if synced_frames is not None else self._synced_frames
        
        start_time = time.perf_counter()
        try:
            results = kpi.compute(frames, **extra_kwargs)
            duration = time.perf_counter() - start_time
            duration_ms = duration * 1000
            
            # 记录 KPI 耗时
            self._timing_stats.add_kpi(kpi.name, duration)
            
            logger.info(f"KPI {kpi.name} 计算成功，结果数: {len(results)}，耗时: {duration_ms:.1f}ms")
            return KPIComputeResult(
                kpi_name=kpi.name,
                status=KPIStatus.SUCCESS,
                results=results,
                duration_ms=duration_ms
            )
        except Exception as e:
            logger.error(f"KPI {kpi.name} 计算失败: {e}", exc_info=True)
            return KPIComputeResult(
                kpi_name=kpi.name,
                status=KPIStatus.FAILED,
                error=str(e)
            )
    
    def _step5_compute_kpis(self):
        """
        步骤5: 计算KPI
        
        所有 bag 数据已合并，KPI 计算时会自动过滤时间间隔点。
        """
        self.progress_callback(5, 6, "计算KPI...")
        
        # 解析控制调试数据
        self._parsed_debug_data = self._parse_control_debug()
        
        # 【内存优化】释放 topic_data，所有数据已同步到 synced_frames
        self._release_topic_data()
        
        # 按依赖关系分组
        independent_kpis, dependent_kpis = self._sort_kpis_by_dependency()
        
        # 构建基础参数
        base_kwargs = {
            'parsed_debug_data': self._parsed_debug_data,
            'bag_infos': self._bag_infos,  # 总是传递 bag_infos，用于异常溯源
            'output_dir': self._output_dir  # 输出目录（用于可视化等）
        }
        
        auto_mileage_km = 0.0
        
        # === 第一阶段：计算无依赖的 KPI ===
        total_kpis = len(independent_kpis) + len(dependent_kpis)
        computed = 0
        success_count = 0
        failed_kpis = []
        
        for kpi in independent_kpis:
            computed += 1
            result = self._compute_single_kpi(kpi, base_kwargs)
            self._kpi_results[kpi.name] = result
            
            if result.status == KPIStatus.SUCCESS:
                self.reporter.add_results(kpi.name, result.results)
                success_count += 1
                print(f"  [{computed}/{total_kpis}] ✓ {kpi.name} ({result.duration_ms:.0f}ms)")
                
                # 提取里程统计结果
                if kpi.name == KPINames.MILEAGE:
                    for r in result.results:
                        if r.name == "自动驾驶里程":
                            auto_mileage_km = r.value
                            break
            else:
                failed_kpis.append(kpi.name)
                print(f"  [{computed}/{total_kpis}] ✗ {kpi.name}: {result.error}")
        
        # === 第二阶段：顺序计算有依赖的 KPI ===
        for kpi in dependent_kpis:
            computed += 1
            
            # 添加依赖参数
            dep_kwargs = base_kwargs.copy()
            dep_kwargs['auto_mileage_km'] = auto_mileage_km
            
            result = self._compute_single_kpi(kpi, dep_kwargs)
            self._kpi_results[kpi.name] = result
            
            if result.status == KPIStatus.SUCCESS:
                self.reporter.add_results(kpi.name, result.results)
                success_count += 1
                print(f"  [{computed}/{total_kpis}] ✓ {kpi.name} ({result.duration_ms:.0f}ms)")
            else:
                failed_kpis.append(kpi.name)
                print(f"  [{computed}/{total_kpis}] ✗ {kpi.name}: {result.error}")
    
    def _step6_generate_reports(self) -> Dict:
        """
        步骤6: 生成报告
        """
        self.progress_callback(6, 6, "生成报告...")
        
        report_files = self.reporter.generate_all_reports()
        
        # 汇总统计
        success_count = sum(1 for r in self._kpi_results.values() 
                          if r.status == KPIStatus.SUCCESS)
        failed_count = sum(1 for r in self._kpi_results.values() 
                         if r.status == KPIStatus.FAILED)
        
        print(f"\n  KPI统计: 成功 {success_count}, 失败 {failed_count}")
        
        return report_files
    
    def _parse_control_debug(self) -> Dict:
        """解析控制调试数据"""
        parsed_data = {}
        
        debug_topic = Topics.CONTROL_DEBUG
        if debug_topic not in self._topic_data:
            logger.info(f"未找到topic: {debug_topic}")
            print(f"    未找到topic: {debug_topic}")
            return parsed_data
        
        self.control_parser = ControlDebugParser()
        topic = self._topic_data[debug_topic]
        parsed_data = self.control_parser.batch_parse(topic.messages, topic.timestamps)
        
        return parsed_data
    
    def _parse_debug_from_frames(self, frame_store) -> Dict:
        """
        从 FrameStore 中的帧数据解析控制调试信息
        
        用于缓存模式下重新解析 parsed_debug_data
        
        Args:
            frame_store: FrameStore 实例
            
        Returns:
            {timestamp: {'lateral_error': value, ...}}
        """
        parsed_data = {}
        
        if self.control_parser is None:
            self.control_parser = ControlDebugParser()
        
        debug_topic = Topics.CONTROL_DEBUG
        parse_count = 0
        
        for frame in frame_store:
            # 获取 CONTROL_DEBUG 消息
            debug_msg = frame.messages.get(debug_topic)
            if debug_msg is None:
                continue
            
            # 解析消息
            debug_info = self.control_parser.parse_control_debug(debug_msg)
            if debug_info is None:
                continue
            
            # 使用帧时间戳作为 key
            ts = frame.timestamp
            parsed_data[ts] = {
                'lateral_error': debug_info.lat_debug.lateral_error,
                'heading_error': debug_info.lat_debug.heading_error,
                'nearest_lateral_error': debug_info.lat_debug.nearest_lateral_error,
                'nearest_heading_error': debug_info.lat_debug.nearest_heading_error,
                'ref_kappa': debug_info.lat_debug.ref_kappa,
                'vehicle_speed': debug_info.lat_debug.vehicle_speed,
                'current_speed': debug_info.lon_debug.current_speed,
                'current_acceleration': debug_info.lon_debug.current_acceleration,
                'acceleration_command': debug_info.lon_debug.acceleration_command,
                'jerk_command': debug_info.lon_debug.jerk_command,
                'current_s_error': debug_info.lon_debug.current_s_error,
                'current_v_error': debug_info.lon_debug.current_v_error
            }
            parse_count += 1
        
        if parse_count > 0:
            print(f"  ✓ 解析 {parse_count:,} 条控制调试数据")
        else:
            print(f"  (无控制调试数据)")
        
        return parsed_data
    
    def _release_topic_data(self):
        """
        释放 topic 数据以节省内存
        
        在时间同步完成后，原始 topic 数据不再需要（已同步到 synced_frames）
        """
        import gc
        
        if self._topic_data:
            # 计算释放前的内存占用（估算）
            msg_count = sum(len(td.messages) for td in self._topic_data.values())
            
            # 清空数据
            self._topic_data.clear()
            self._topic_data = {}
            
            # 强制垃圾回收
            gc.collect()
            
            logger.debug(f"已释放 topic 数据 ({msg_count} 条消息)")
    
    def _release_synced_frames(self):
        """
        释放同步帧数据以节省内存
        
        在 KPI 计算完成后调用
        """
        import gc
        
        if self._synced_frames:
            frame_count = len(self._synced_frames)
            
            # 清空数据
            self._synced_frames.clear()
            self._synced_frames = []
            
            # 强制垃圾回收
            gc.collect()
            
            logger.debug(f"已释放同步帧数据 ({frame_count} 帧)")
    
    @staticmethod
    def _get_memory_usage_mb() -> float:
        """获取当前进程内存使用量 (MB)"""
        try:
            import psutil
            process = psutil.Process()
            return process.memory_info().rss / (1024 * 1024)
        except ImportError:
            # psutil 未安装，尝试读取 /proc
            try:
                with open('/proc/self/status', 'r') as f:
                    for line in f:
                        if line.startswith('VmRSS:'):
                            return int(line.split()[1]) / 1024  # KB to MB
            except:
                pass
        return 0.0
    
    def _log_memory(self, stage: str):
        """记录内存使用情况"""
        mem_mb = self._get_memory_usage_mb()
        if mem_mb > 0:
            print(f"  [内存] {stage}: {mem_mb:.0f} MB")
    
    def _synchronize_single_bag_data(self, topic_data: Dict) -> List:
        """
        对单个 bag 的 topic 数据进行时间同步
        
        Args:
            topic_data: topic 数据字典
            
        Returns:
            同步帧列表
        """
        base_topic = self.config.get(ConfigKeys.TOPICS, {}).get(
            'function_manager', Topics.FUNCTION_MANAGER)
        
        synchronizer = TimeSynchronizer(
            base_topic=base_topic,
            tolerance=self.config.get(ConfigKeys.BASE, {}).get(
                ConfigKeys.SYNC_TOLERANCE, 0.3)
        )
        synchronizer.add_topic_data(topic_data)
        return synchronizer.synchronize(use_global_timestamp=True)
    
    def _compute_kpis_for_bag(self, synced_frames: List, bag_info: Any,
                              parsed_debug_data: Dict) -> Dict[str, List[KPIResult]]:
        """
        对单个 bag 的同步帧计算所有 KPI
        
        Args:
            synced_frames: 同步帧列表
            bag_info: bag 信息
            parsed_debug_data: 解析后的控制调试数据
            
        Returns:
            {kpi_name: [KPIResult, ...]}
        """
        results = {}
        
        # 构建基础参数
        base_kwargs = {
            'parsed_debug_data': parsed_debug_data,
            'bag_infos': [bag_info],
            'output_dir': self._output_dir
        }
        
        auto_mileage_km = 0.0
        
        # 按依赖关系分组
        independent_kpis, dependent_kpis = self._sort_kpis_by_dependency()
        
        # 计算无依赖的 KPI
        for kpi in independent_kpis:
            start_time = time.perf_counter()
            try:
                kpi_results = kpi.compute(synced_frames, **base_kwargs)
                results[kpi.name] = kpi_results
                
                # 提取里程统计结果
                if kpi.name == KPINames.MILEAGE:
                    for r in kpi_results:
                        if r.name == "自动驾驶里程":
                            auto_mileage_km = r.value
                            break
                
                duration = time.perf_counter() - start_time
                self._timing_stats.add_kpi(kpi.name, 
                    self._timing_stats.kpi_timings.get(kpi.name, 0) + duration)
            except Exception as e:
                logger.error(f"KPI {kpi.name} 计算失败: {e}")
                results[kpi.name] = []
        
        # 计算有依赖的 KPI
        for kpi in dependent_kpis:
            start_time = time.perf_counter()
            dep_kwargs = base_kwargs.copy()
            dep_kwargs['auto_mileage_km'] = auto_mileage_km
            
            try:
                kpi_results = kpi.compute(synced_frames, **dep_kwargs)
                results[kpi.name] = kpi_results
                
                duration = time.perf_counter() - start_time
                self._timing_stats.add_kpi(kpi.name,
                    self._timing_stats.kpi_timings.get(kpi.name, 0) + duration)
            except Exception as e:
                logger.error(f"KPI {kpi.name} 计算失败: {e}")
                results[kpi.name] = []
        
        return results
    
    def analyze_true_streaming(self, bag_path: str, output_dir: str = "./output", 
                                use_cache: bool = True) -> Dict:
        """
        真正的流式处理模式（内存最优 + 缓存支持）
        
        特点：
        1. 读取一个 bag → 同步 → 直接 collect() → 释放内存
        2. 支持磁盘缓存：首次处理写入缓存，后续直接加载
        3. 内存峰值只是单个 bag 的大小
        4. 适合处理超大数据集
        
        Args:
            bag_path: ROS2 bag路径或包含多个bag的目录
            output_dir: 输出目录
            use_cache: 是否使用缓存（默认 True）
            
        Returns:
            分析结果字典
        """
        import gc
        from datetime import datetime
        from .kpi.base_kpi import StreamingData
        from .data_loader.frame_store import FrameStoreCache, DiskFrameStore
        
        print(f"\n{'='*60}")
        print("Hello KPI - 自动驾驶数据分析 [流式模式]")
        print(f"{'='*60}")
        
        # 创建带时间戳的输出目录
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._output_dir = os.path.join(output_dir, timestamp_str)
        os.makedirs(self._output_dir, exist_ok=True)
        print(f"输出目录: {self._output_dir}")
        
        self._current_bag_path = bag_path
        self._timing_stats = TimingStats()
        total_start = time.perf_counter()
        
        # 初始化 KPI 计算器
        self._init_kpi_calculators()
        
        # 检测 bag 结构
        bag_path_obj = Path(bag_path)
        db3_files = sorted(bag_path_obj.glob("*.db3")) if bag_path_obj.is_dir() else []
        has_metadata = (bag_path_obj / "metadata.yaml").exists() if bag_path_obj.is_dir() else False
        is_single_dir_multi_db3 = len(db3_files) > 1 and has_metadata
        is_multi_bag_dir = bag_path_obj.is_dir() and not has_metadata
        
        # 初始化多 bag 读取器
        self.bag_reader = MultiBagReader(bag_path)
        self._bag_infos = self.bag_reader.get_bag_infos()
        bag_list = self.bag_reader.get_bag_list()
        
        # 获取时间范围
        start_time, end_time = self.bag_reader.get_time_range()
        duration = end_time - start_time
        
        print(f"\n[数据信息]")
        print(f"  - 时间范围: {start_time:.2f} - {end_time:.2f}")
        print(f"  - 持续时间: {duration:.2f}秒 ({duration/3600:.2f}小时)")
        
        if is_single_dir_multi_db3:
            print(f"  - 模式: 单目录多db3文件 ({len(db3_files)} 个文件)")
        elif is_multi_bag_dir:
            print(f"  - 模式: 多Bag目录 ({len(bag_list)} 个bag)")
        else:
            print(f"  - 模式: 单Bag")
        
        # 检查缓存
        cache = FrameStoreCache(cache_dir=".frame_cache") if use_cache else None
        cached_store = cache.load(bag_path) if cache else None
        
        if cached_store:
            print(f"  - 存储: 使用缓存 ({len(cached_store):,} 帧)")
        else:
            print(f"  - 存储: 流式处理" + (" + 写入缓存" if use_cache else ""))
        
        step_start = time.perf_counter()
        
        # 初始化报告器
        self.reporter = KPIReporter(self._output_dir)
        self.reporter.set_metadata(
            bag_path=bag_path,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            is_multi_bag=is_multi_bag_dir or is_single_dir_multi_db3,
            bag_list=bag_list if not is_single_dir_multi_db3 else [f.stem for f in db3_files]
        )
        
        # 获取需要读取的 topics
        required_topics = self._get_required_topics()
        bag_info_dict = self.bag_reader.get_topic_info()
        available_topics = set(bag_info_dict.keys())
        topics_to_read = [t for t in required_topics if t in available_topics]
        
        self._log_memory("开始前")
        
        # 初始化 StreamingData
        streaming_data = StreamingData()
        global_parsed_debug_data = {}
        all_bag_infos = []
        
        # 准备 KPI
        independent_kpis, dependent_kpis = self._sort_kpis_by_dependency()
        all_kpis = independent_kpis + dependent_kpis
        streaming_kpis = [kpi for kpi in all_kpis if kpi.supports_streaming]
        non_streaming_kpis = [kpi for kpi in all_kpis if not kpi.supports_streaming]
        
        print(f"\n  [流式处理] {len(streaming_kpis)} 个 KPI 支持流式, {len(non_streaming_kpis)} 个需要完整数据")
        
        self._timing_stats.add_step("1.初始化", time.perf_counter() - step_start)
        
        # ========== 阶段1: 读取数据 + collect ==========
        step_start = time.perf_counter()
        
        if cached_store:
            # ===== 从缓存加载 =====
            print(f"\n[阶段1] 从缓存流式加载...")
            
            # 从缓存获取 bag 信息
            for bag_info in cached_store.get_bag_infos():
                all_bag_infos.append(bag_info.to_multi_bag_info())
            streaming_data.bag_infos = all_bag_infos
            
            total_frames = len(cached_store)
            bag_names = cached_store.get_bag_names()
            
            # 初始化控制调试解析器
            if self.control_parser is None:
                self.control_parser = ControlDebugParser()
            debug_topic = Topics.CONTROL_DEBUG
            
            debug_count = 0
            first_frame_topics = None
            for idx, bag_name in enumerate(bag_names):
                print(f"\n  [{idx+1}/{len(bag_names)}] 加载: {bag_name}")
                
                # 从缓存流式读取该 bag 的帧
                frames_batch = []
                frame_count = 0
                
                for frame in cached_store.iterate_by_bag(bag_name):
                    frames_batch.append(frame)
                    frame_count += 1
                    
                    # # 调试：打印第一帧的 topics
                    # if first_frame_topics is None:
                    #     first_frame_topics = list(frame.messages.keys())
                    #     print(f"      [DEBUG] 第一帧 topics: {first_frame_topics[:5]}...")
                    #     print(f"      [DEBUG] debug_topic={debug_topic}")
                    #     print(f"      [DEBUG] debug_topic in messages: {debug_topic in frame.messages}")
                    
                    # 从帧中解析 debug 数据
                    if debug_topic in frame.messages:
                        debug_msg = frame.messages[debug_topic]
                        debug_info = self.control_parser.parse_control_debug(debug_msg)
                        if debug_info:
                            # 转换为与 batch_parse 相同的字典格式
                            global_parsed_debug_data[frame.timestamp] = {
                                'lateral_error': debug_info.lat_debug.nearest_lateral_error,
                                'heading_error': debug_info.lat_debug.nearest_heading_error,
                                'lateral_error_raw': debug_info.lat_debug.lateral_error,
                                'heading_error_raw': debug_info.lat_debug.heading_error,
                                'ref_kappa': debug_info.lat_debug.ref_kappa,
                                'vehicle_speed': debug_info.lat_debug.vehicle_speed,
                                'current_speed': debug_info.lon_debug.current_speed,
                                'current_acceleration': debug_info.lon_debug.current_acceleration,
                                'acceleration_command': debug_info.lon_debug.acceleration_command,
                            }
                            debug_count += 1
                    
                    # 每 5000 帧处理一次，控制内存
                    if len(frames_batch) >= 5000:
                        for kpi in streaming_kpis:
                            try:
                                kpi.collect(frames_batch, streaming_data,
                                           parsed_debug_data=global_parsed_debug_data)
                            except Exception:
                                pass
                        frames_batch.clear()
                        gc.collect()
                
                # 处理剩余帧
                if frames_batch:
                    for kpi in streaming_kpis:
                        try:
                            kpi.collect(frames_batch, streaming_data,
                                       parsed_debug_data=global_parsed_debug_data)
                        except Exception:
                            pass
                    frames_batch.clear()
                
                print(f"      帧数: {frame_count:,}")
                gc.collect()
                self._log_memory(f"[{idx+1}/{len(bag_names)}] 加载后")
            
            print(f"\n  加载完成: 共 {total_frames:,} 帧, 解析 {debug_count:,} 条调试数据")
            
        else:
            # ===== 首次处理：读取 bag + 写入缓存 =====
            
            # 准备 DiskFrameStore（用于写入缓存）
            disk_store = None
            if use_cache:
                cache_path = cache.get_cache_path_for_bag(bag_path)
                cache_dir = str(Path(cache_path).parent)
                disk_store = DiskFrameStore(cache_dir=cache_dir, persistent=True, db_path=cache_path)
            
            # 准备 bag_infos
            if is_single_dir_multi_db3:
                single_reader = BagReader(str(bag_path_obj))
                db3_infos = single_reader.get_db3_file_infos()
                
                from .data_loader.multi_bag_reader import BagInfo
                for info in db3_infos:
                    all_bag_infos.append(BagInfo(
                        path=info['path'],
                        start_time=info['start_time'],
                        end_time=info['end_time'],
                        duration=info['end_time'] - info['start_time']
                    ))
                iterate_items = list(zip(db3_files, all_bag_infos))
            elif is_multi_bag_dir:
                all_bag_infos = list(self._bag_infos)
                iterate_items = [(info.path, info) for info in self._bag_infos]
            else:
                if self._bag_infos:
                    all_bag_infos = list(self._bag_infos)
                else:
                    from .data_loader.multi_bag_reader import BagInfo
                    all_bag_infos = [BagInfo(
                        path=Path(bag_path),
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration
                    )]
                iterate_items = [(bag_path, all_bag_infos[0])]
            
            streaming_data.bag_infos = all_bag_infos
            total_items = len(iterate_items)
            total_frames = 0
            
            for item_idx, (item_path, bag_info_obj) in enumerate(iterate_items):
                item_name = Path(item_path).name
                print(f"\n  [{item_idx+1}/{total_items}] 处理: {item_name}")
                
                # 读取单个 bag/db3 的数据
                single_reader = BagReader(str(item_path))
                topic_data = single_reader.read_topics(topics_to_read, progress=False, light_mode=True)
                
                msg_count = sum(len(td.messages) for td in topic_data.values())
                print(f"      读取消息: {msg_count:,} 条")
                
                # 解析控制调试数据
                debug_topic = Topics.CONTROL_DEBUG
                if debug_topic in topic_data:
                    if self.control_parser is None:
                        self.control_parser = ControlDebugParser()
                    topic = topic_data[debug_topic]
                    parsed = self.control_parser.batch_parse(topic.messages, topic.timestamps)
                    global_parsed_debug_data.update(parsed)
                
                # 时间同步
                synced_frames = self._synchronize_single_bag_data(topic_data)
                print(f"      同步帧数: {len(synced_frames)}")
                total_frames += len(synced_frames)
                
                # 写入缓存（如果启用）
                if disk_store is not None and synced_frames:
                    disk_store.append_batch(synced_frames, item_name)
                
                # 直接调用所有 KPI 的 collect（核心优化点！）
                for kpi in streaming_kpis:
                    try:
                        kpi.collect(synced_frames, streaming_data,
                                   parsed_debug_data=global_parsed_debug_data)
                    except Exception:
                        pass
                
                # 释放内存
                for td in topic_data.values():
                    td.clear()
                topic_data.clear()
                synced_frames.clear()
                gc.collect()
                
                self._log_memory(f"[{item_idx+1}/{total_items}] 处理后")
            
            print(f"\n  处理完成: 共 {total_frames:,} 帧")
            
            # 保存缓存
            if disk_store is not None:
                disk_store.finalize()
                cache.save_meta(bag_path, disk_store)
                print(f"  缓存已保存: {len(disk_store):,} 帧")
        
        self._timing_stats.add_step("2.读取+收集", time.perf_counter() - step_start)
        
        # ========== 阶段2: 计算 KPI ==========
        print(f"\n[阶段2] 计算 KPI...")
        step_start = time.perf_counter()
        
        self._parsed_debug_data = global_parsed_debug_data
        self._bag_infos = all_bag_infos
        
        base_kwargs = {
            'parsed_debug_data': self._parsed_debug_data,
            'bag_infos': self._bag_infos,
            'output_dir': self._output_dir
        }
        
        auto_mileage_km = 0.0
        total_kpis = len(streaming_kpis) + len(non_streaming_kpis)
        computed = 0
        
        # 计算流式 KPI
        for kpi in streaming_kpis:
            computed += 1
            kpi_start = time.perf_counter()
            
            try:
                compute_kwargs = base_kwargs.copy()
                if kpi.name not in [KPINames.MILEAGE]:
                    compute_kwargs['auto_mileage_km'] = auto_mileage_km
                
                results = kpi.compute_from_collected(streaming_data, **compute_kwargs)
                duration_ms = (time.perf_counter() - kpi_start) * 1000
                
                result = KPIComputeResult(
                    kpi_name=kpi.name,
                    status=KPIStatus.SUCCESS,
                    results=results,
                    duration_ms=duration_ms
                )
                self._kpi_results[kpi.name] = result
                self.reporter.add_results(kpi.name, results)
                print(f"  [{computed}/{total_kpis}] ✓ {kpi.name} ({duration_ms:.0f}ms)")
                
                # 提取自动驾驶里程
                if kpi.name == KPINames.MILEAGE:
                    for r in results:
                        if r.name == "自动驾驶里程":
                            auto_mileage_km = r.value
                            break
                            
            except Exception as e:
                duration_ms = (time.perf_counter() - kpi_start) * 1000
                result = KPIComputeResult(
                    kpi_name=kpi.name,
                    status=KPIStatus.FAILED,
                    error=str(e),
                    duration_ms=duration_ms
                )
                self._kpi_results[kpi.name] = result
                print(f"  [{computed}/{total_kpis}] ✗ {kpi.name}: {e}")
        
        # 非流式 KPI 暂不支持（需要完整数据）
        if non_streaming_kpis:
            print(f"\n  警告: {len(non_streaming_kpis)} 个非流式 KPI 在此模式下不计算")
        
        self._timing_stats.add_step("3.计算KPI", time.perf_counter() - step_start)
        
        # 释放 StreamingData
        streaming_data.clear()
        gc.collect()
        
        # ========== 阶段3: 生成报告 ==========
        print(f"\n[阶段3] 生成报告...")
        step_start = time.perf_counter()
        
        self.reporter.set_timing_stats(self._timing_stats.to_dict())
        report_files = self.reporter.generate_all_reports()
        self._timing_stats.add_step("4.生成报告", time.perf_counter() - step_start)
        
        total_duration = time.perf_counter() - total_start
        self._timing_stats.total_time = total_duration
        
        # 打印统计
        print(f"\n{'='*60}")
        print(f"分析完成！")
        print(f"{'='*60}")
        print(f"  总帧数: {total_frames:,}")
        print(f"  总耗时: {total_duration:.2f}s")
        self._timing_stats.print_summary()
        print(f"\n  报告目录: {self._output_dir}")
        
        return {
            'output_dir': self._output_dir,
            'total_frames': total_frames,
            'duration': total_duration,
            'kpi_results': self._kpi_results
        }
    
    def analyze_with_frame_store(self, bag_path: str, output_dir: str = "./output",
                                  force_disk: bool = False) -> Dict:
        """
        使用 FrameStore 的分析模式（推荐）
        
        特点：
        1. 自动根据数据量选择内存/磁盘模式
        2. 所有 KPI 使用统一的 compute 方法，结果完全一致
        3. 支持大数据量，不会 OOM
        
        Args:
            bag_path: ROS2 bag路径或包含多个bag的目录
            output_dir: 输出目录
            force_disk: 强制使用磁盘模式（用于测试或内存受限场景）
            
        Returns:
            分析结果字典
        """
        import gc
        from datetime import datetime
        
        print(f"\n{'='*60}")
        print("Hello KPI - 自动驾驶数据分析 [FrameStore 模式]")
        print(f"{'='*60}")
        
        # 创建带时间戳的输出目录
        timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._output_dir = os.path.join(output_dir, timestamp_str)
        os.makedirs(self._output_dir, exist_ok=True)
        print(f"输出目录: {self._output_dir}")
        
        self._current_bag_path = bag_path
        self._timing_stats = TimingStats()
        total_start = time.perf_counter()
        
        # 初始化 KPI 计算器
        self._init_kpi_calculators()
        
        # 检测 bag 结构
        bag_path_obj = Path(bag_path)
        db3_files = sorted(bag_path_obj.glob("*.db3")) if bag_path_obj.is_dir() else []
        has_metadata = (bag_path_obj / "metadata.yaml").exists() if bag_path_obj.is_dir() else False
        is_single_dir_multi_db3 = len(db3_files) > 1 and has_metadata
        is_multi_bag_dir = bag_path_obj.is_dir() and not has_metadata
        
        # 初始化多 bag 读取器
        self.bag_reader = MultiBagReader(bag_path)
        self._bag_infos = self.bag_reader.get_bag_infos()
        bag_list = self.bag_reader.get_bag_list()
        
        # 获取时间范围
        start_time, end_time = self.bag_reader.get_time_range()
        duration = end_time - start_time
        
        print(f"\n[数据信息]")
        print(f"  - 时间范围: {start_time:.2f} - {end_time:.2f}")
        print(f"  - 持续时间: {duration:.2f}秒 ({duration/3600:.2f}小时)")
        
        if is_single_dir_multi_db3:
            print(f"  - 模式: 单目录多db3文件 ({len(db3_files)} 个文件)")
        elif is_multi_bag_dir:
            print(f"  - 模式: 多Bag目录 ({len(bag_list)} 个bag)")
        else:
            print(f"  - 模式: 单Bag")
        
        # 检查持久化缓存
        step_start = time.perf_counter()
        frame_store = None
        use_cached = False
        
        if self.use_cache and self.frame_cache:
            if self.frame_cache.is_valid(bag_path):
                print(f"\n[缓存] 检测到有效缓存，加载中...")
                frame_store = self.frame_cache.load(bag_path)
                if frame_store:
                    use_cached = True
                    print(f"  ✓ 从缓存加载 {len(frame_store):,} 帧")
                    # 转换为 multi_bag_reader.BagInfo 格式（用于 KPI 事件溯源）
                    self._bag_infos = [info.to_multi_bag_info() for info in frame_store.get_bag_infos()]
        
        # 如果没有缓存，预估帧数并创建 FrameStore
        if not use_cached:
            # 收集所有需要处理的路径
            if is_single_dir_multi_db3:
                paths_to_process = [str(f) for f in db3_files]
            elif is_multi_bag_dir:
                paths_to_process = [str(info.path) for info in self._bag_infos]
            else:
                paths_to_process = [bag_path]
            
            estimated_frames = estimate_frame_count(paths_to_process)
            
            # 创建 FrameStore
            if self.use_cache and self.frame_cache:
                # 持久化模式：使用固定的缓存路径
                cache_db_path = self.frame_cache.get_cache_path_for_bag(bag_path)
                frame_store = DiskFrameStore(
                    cache_dir=str(self.frame_cache.cache_dir),
                    persistent=True,
                    existing_db=None  # 新建
                )
                frame_store.db_path = cache_db_path
                frame_store._init_db()
                storage_mode = "磁盘(持久化)"
            elif force_disk or estimated_frames > 100000:
                cache_dir = os.path.join(self._output_dir, ".cache")
                frame_store = create_frame_store(estimated_frames, cache_dir, force_disk)
                storage_mode = "磁盘"
            else:
                frame_store = create_frame_store(estimated_frames, None, False)
                storage_mode = "内存"
            
            print(f"\n[存储模式] {storage_mode}模式 (预估 {estimated_frames:,} 帧)")
        
        self._timing_stats.add_step("1.初始化", time.perf_counter() - step_start)
        
        # 初始化报告器
        self.reporter = KPIReporter(self._output_dir)
        self.reporter.set_metadata(
            bag_path=bag_path,
            start_time=start_time,
            end_time=end_time,
            duration=duration,
            is_multi_bag=is_multi_bag_dir or is_single_dir_multi_db3,
            bag_list=bag_list if not is_single_dir_multi_db3 else [f.stem for f in db3_files]
        )
        
        # 获取需要读取的 topics
        required_topics = self._get_required_topics()
        bag_info_dict = self.bag_reader.get_topic_info()
        available_topics = set(bag_info_dict.keys())
        topics_to_read = [t for t in required_topics if t in available_topics]
        
        self._log_memory("开始前")
        
        # 存储全局 parsed_debug_data
        global_parsed_debug_data = {}
        all_bag_infos = []
        
        # ========== 第一阶段：逐个处理并写入 FrameStore ==========
        if use_cached:
            # 使用缓存，跳过数据读取
            print(f"\n[阶段1] 跳过（使用缓存）")
            # 转换为 multi_bag_reader.BagInfo 格式（用于 KPI 事件溯源）
            all_bag_infos = [info.to_multi_bag_info() for info in frame_store.get_bag_infos()]
            
            # 从缓存帧中重新解析 parsed_debug_data
            print(f"  解析控制调试数据...")
            global_parsed_debug_data = self._parse_debug_from_frames(frame_store)
        else:
            print(f"\n[阶段1] 读取数据并写入 FrameStore...")
            step_start = time.perf_counter()
        
        if not use_cached:
            # 准备 bag_infos
            if is_single_dir_multi_db3:
                # 获取每个 db3 文件的时间范围
                single_reader = BagReader(str(bag_path_obj))
                db3_infos = single_reader.get_db3_file_infos()
                
                from .data_loader.multi_bag_reader import BagInfo
                for info in db3_infos:
                    all_bag_infos.append(BagInfo(
                        path=info['path'],
                        start_time=info['start_time'],
                        end_time=info['end_time'],
                        duration=info['end_time'] - info['start_time']
                    ))
                
                iterate_items = list(zip(db3_files, all_bag_infos))
            elif is_multi_bag_dir:
                all_bag_infos = list(self._bag_infos)
                iterate_items = [(info.path, info) for info in self._bag_infos]
            else:
                # 单 bag 模式
                if self._bag_infos:
                    all_bag_infos = list(self._bag_infos)
                else:
                    from .data_loader.multi_bag_reader import BagInfo
                    all_bag_infos = [BagInfo(
                        path=Path(bag_path),
                        start_time=start_time,
                        end_time=end_time,
                        duration=duration
                    )]
                iterate_items = [(bag_path, all_bag_infos[0])]
            
            total_items = len(iterate_items)
            for item_idx, (item_path, bag_info_obj) in enumerate(iterate_items):
                item_name = Path(item_path).name
                print(f"\n  [{item_idx+1}/{total_items}] 处理: {item_name}")
                
                # 读取单个 bag/db3 的数据（启用轻量模式，减少内存占用）
                single_reader = BagReader(str(item_path))
                topic_data = single_reader.read_topics(topics_to_read, progress=False, light_mode=True)
                # 注：light_mode=True 将完整 ROS 消息转为轻量级消息，内存占用减少 80-90%
                
                msg_count = sum(len(td.messages) for td in topic_data.values())
                print(f"      读取消息: {msg_count:,} 条")
                
                # 解析控制调试数据
                debug_topic = Topics.CONTROL_DEBUG
                if debug_topic in topic_data:
                    if self.control_parser is None:
                        self.control_parser = ControlDebugParser()
                    topic = topic_data[debug_topic]
                    parsed = self.control_parser.batch_parse(topic.messages, topic.timestamps)
                    global_parsed_debug_data.update(parsed)
                
                # 时间同步
                synced_frames = self._synchronize_single_bag_data(topic_data)
                print(f"      同步帧数: {len(synced_frames)}")
                
                # 写入 FrameStore
                frame_store.append_batch(synced_frames, item_name)
                
                # 释放内存
                for td in topic_data.values():
                    td.clear()
                topic_data.clear()
                synced_frames.clear()
                gc.collect()
                
                self._log_memory(f"[{item_idx+1}/{total_items}] 处理后")
            
            # 完成写入，准备读取
            frame_store.finalize()
            
            print(f"\n  写入完成: 共 {len(frame_store):,} 帧")
            if isinstance(frame_store, DiskFrameStore):
                stats = frame_store.get_stats()
                print(f"  磁盘占用: {stats['db_size_mb']:.1f} MB")
                
                # 保存缓存元数据（持久化模式）
                if self.use_cache and self.frame_cache and frame_store.persistent:
                    self.frame_cache.save_meta(bag_path, frame_store)
                    print(f"  [缓存] 已保存持久化缓存")
            
            self._timing_stats.add_step("2.读取数据", time.perf_counter() - step_start)
        
        # ========== 第二阶段：统一计算 KPI ==========
        print(f"\n[阶段2] 计算 KPI...")
        step_start = time.perf_counter()
        
        self._parsed_debug_data = global_parsed_debug_data
        self._bag_infos = all_bag_infos
        
        # 按依赖关系分组
        independent_kpis, dependent_kpis = self._sort_kpis_by_dependency()
        
        # 构建基础参数
        base_kwargs = {
            'parsed_debug_data': self._parsed_debug_data,
            'bag_infos': self._bag_infos,
            'output_dir': self._output_dir
        }
        
        auto_mileage_km = 0.0
        total_kpis = len(independent_kpis) + len(dependent_kpis)
        
        # ========== 流式处理：一次遍历，所有 KPI 并行 collect ==========
        # 这样避免 list(frame_store) 导致的 OOM 问题
        from .kpi.base_kpi import StreamingData
        
        streaming_data = StreamingData()
        streaming_data.bag_infos = list(self._bag_infos)
        
        # 收集所有支持流式处理的 KPI
        all_kpis = independent_kpis + dependent_kpis
        streaming_kpis = [kpi for kpi in all_kpis if kpi.supports_streaming]
        non_streaming_kpis = [kpi for kpi in all_kpis if not kpi.supports_streaming]
        
        print(f"\n  [流式处理] {len(streaming_kpis)} 个 KPI 支持流式, {len(non_streaming_kpis)} 个需要完整数据")
        
        # 一次遍历，所有 KPI 并行收集数据
        print(f"  遍历 FrameStore ({len(frame_store):,} 帧)...")
        collect_start = time.perf_counter()
        frame_count = 0
        
        for frame in frame_store:
            frame_count += 1
            
            # 所有流式 KPI 收集这一帧
            for kpi in streaming_kpis:
                try:
                    kpi.collect([frame], streaming_data, 
                               parsed_debug_data=self._parsed_debug_data)
                except Exception as e:
                    # 单帧收集失败不影响整体
                    pass
            
            # 定期输出进度
            if frame_count % 50000 == 0:
                print(f"    已处理 {frame_count:,} 帧...")
        
        collect_duration = time.perf_counter() - collect_start
        print(f"  遍历完成: {frame_count:,} 帧 ({collect_duration:.1f}s)")
        
        # 从收集的数据计算 KPI
        print(f"\n  计算 KPI...")
        computed = 0
        
        # 计算流式 KPI
        for kpi in streaming_kpis:
            computed += 1
            kpi_start = time.perf_counter()
            
            try:
                compute_kwargs = base_kwargs.copy()
                if kpi.name not in [KPINames.MILEAGE]:
                    compute_kwargs['auto_mileage_km'] = auto_mileage_km
                
                results = kpi.compute_from_collected(streaming_data, **compute_kwargs)
                duration_ms = (time.perf_counter() - kpi_start) * 1000
                
                result = KPIComputeResult(
                    kpi_name=kpi.name,
                    status=KPIStatus.SUCCESS,
                    results=results,
                    duration_ms=duration_ms
                )
                self._kpi_results[kpi.name] = result
                self.reporter.add_results(kpi.name, results)
                print(f"  [{computed}/{total_kpis}] ✓ {kpi.name} ({duration_ms:.0f}ms)")
                
                # 提取自动驾驶里程
                if kpi.name == KPINames.MILEAGE:
                    for r in results:
                        if r.name == "自动驾驶里程":
                            auto_mileage_km = r.value
                            break
                            
            except Exception as e:
                duration_ms = (time.perf_counter() - kpi_start) * 1000
                result = KPIComputeResult(
                    kpi_name=kpi.name,
                    status=KPIStatus.FAILED,
                    error=str(e),
                    duration_ms=duration_ms
                )
                self._kpi_results[kpi.name] = result
                logger.error(f"KPI {kpi.name} 计算失败: {e}", exc_info=True)
                print(f"  [{computed}/{total_kpis}] ✗ {kpi.name}: {e}")
        
        # 如果有不支持流式的 KPI，需要加载数据（这种情况较少）
        if non_streaming_kpis:
            print(f"\n  [非流式 KPI] 需要加载完整数据...")
            synced_frames_list = list(frame_store)
            
            for kpi in non_streaming_kpis:
                computed += 1
                dep_kwargs = base_kwargs.copy()
                dep_kwargs['auto_mileage_km'] = auto_mileage_km
                
                result = self._compute_single_kpi(kpi, dep_kwargs, synced_frames_list)
                self._kpi_results[kpi.name] = result
                
                if result.status == KPIStatus.SUCCESS:
                    self.reporter.add_results(kpi.name, result.results)
                    print(f"  [{computed}/{total_kpis}] ✓ {kpi.name} ({result.duration_ms:.0f}ms)")
                else:
                    print(f"  [{computed}/{total_kpis}] ✗ {kpi.name}: {result.error}")
            
            synced_frames_list.clear()
        
        self._timing_stats.add_step("3.计算KPI", time.perf_counter() - step_start)
        
        # 释放内存
        streaming_data.clear()
        gc.collect()
        
        # 清理 FrameStore 缓存
        frame_store.cleanup()
        self._log_memory("计算完成")
        
        # ========== 生成报告 ==========
        self._timing_stats.total_time = time.perf_counter() - total_start
        self.reporter.set_timing_stats(self._timing_stats.to_dict())
        
        step_start = time.perf_counter()
        report_files = self.reporter.generate_all_reports()
        self._timing_stats.add_step("4.生成报告", time.perf_counter() - step_start)
        
        self._timing_stats.total_time = time.perf_counter() - total_start
        self._timing_stats.print_summary()
        
        print(f"\n{'='*60}")
        print("分析完成! [FrameStore 模式]")
        print(f"{'='*60}")
        
        # 构建返回结果
        results = {}
        for name, compute_result in self._kpi_results.items():
            results[name] = compute_result.results
        
        return {
            'results': results,
            'kpi_status': {name: r.to_dict() for name, r in self._kpi_results.items()},
            'report_files': report_files,
            'metadata': self.reporter.metadata,
            'timing_stats': self._timing_stats.to_dict()
        }
    
    def analyze(self, bag_path: str, output_dir: str = "./output", 
                multi_bag: bool = False,
                use_frame_store: bool = True) -> Dict:
        """
        执行分析
        
        Args:
            bag_path: ROS2 bag路径或包含多个bag的目录
            output_dir: 输出目录（基础目录，会在其下创建时间戳子目录）
            multi_bag: 是否启用多bag模式
            use_frame_store: 是否使用 FrameStore 模式（推荐，自动处理大数据量）
            
        Returns:
            分析结果字典
        """
        bag_path_obj = Path(bag_path)
        is_multi_bag_dir = multi_bag or (bag_path_obj.is_dir() and not (bag_path_obj / "metadata.yaml").exists())
        
        # 检测数据量
        db3_files = list(bag_path_obj.glob("*.db3")) if bag_path_obj.is_dir() else []
        has_multiple_files = len(db3_files) > 1 or is_multi_bag_dir
        
        # 预估数据量（用于决定是否使用 FrameStore）
        if bag_path_obj.is_dir():
            paths_to_check = [str(f) for f in db3_files] if db3_files else [bag_path]
        else:
            paths_to_check = [bag_path]
        
        estimated_frames = estimate_frame_count(paths_to_check)
        is_large_data = estimated_frames > 200_000  # 20万帧以上视为大数据量
        
        # 自动选择模式：多文件 或 大数据量 → 使用 FrameStore
        if use_frame_store and (has_multiple_files or is_large_data):
            reason = "多个数据文件" if has_multiple_files else f"大数据量 (预估 {estimated_frames:,} 帧)"
            print(f"\n[模式选择] {reason}，使用 FrameStore 模式")
            return self.analyze_with_frame_store(bag_path, output_dir, force_disk=False)
        
        print(f"\n{'='*60}")
        print("Hello KPI - 自动驾驶数据分析")
        print(f"{'='*60}")
        
        # 创建带时间戳的输出目录
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._output_dir = os.path.join(output_dir, timestamp)
        os.makedirs(self._output_dir, exist_ok=True)
        print(f"输出目录: {self._output_dir}")
        
        # 记录当前 bag 路径（用于缓存）
        self._current_bag_path = bag_path
        
        # 重置耗时统计
        self._timing_stats = TimingStats()
        total_start = time.perf_counter()
        
        # 执行各步骤（带耗时统计）
        step_start = time.perf_counter()
        is_multi_bag, bag_list = self._step1_init_components(bag_path, self._output_dir, multi_bag)
        self._timing_stats.add_step("1.初始化", time.perf_counter() - step_start)
        
        step_start = time.perf_counter()
        bag_info = self._step2_read_bag_info(bag_path, is_multi_bag, bag_list)
        self._timing_stats.add_step("2.读取Bag信息", time.perf_counter() - step_start)
        
        step_start = time.perf_counter()
        self._step3_read_topic_data(bag_info)
        self._timing_stats.add_step("3.读取Topic数据", time.perf_counter() - step_start)
        self._log_memory("读取数据后")
        
        step_start = time.perf_counter()
        self._step4_synchronize_data()
        self._timing_stats.add_step("4.时间同步", time.perf_counter() - step_start)
        self._log_memory("时间同步后")
        
        step_start = time.perf_counter()
        self._step5_compute_kpis()
        self._timing_stats.add_step("5.计算KPI", time.perf_counter() - step_start)
        self._log_memory("KPI计算后")
        
        # 【内存优化】KPI 计算完成后释放同步帧数据
        self._release_synced_frames()
        self._log_memory("释放数据后")
        
        # 记录总耗时（在生成报告前计算，这样报告中能包含耗时）
        self._timing_stats.total_time = time.perf_counter() - total_start
        
        # 将耗时统计传递给报告器
        self.reporter.set_timing_stats(self._timing_stats.to_dict())
        
        step_start = time.perf_counter()
        report_files = self._step6_generate_reports()
        self._timing_stats.add_step("6.生成报告", time.perf_counter() - step_start)
        
        # 更新总耗时（包含报告生成时间）
        self._timing_stats.total_time = time.perf_counter() - total_start
        
        # 打印耗时统计
        self._timing_stats.print_summary()
        
        print(f"\n{'='*60}")
        print("分析完成!")
        print(f"{'='*60}")
        
        # 构建返回结果
        results = {}
        for name, compute_result in self._kpi_results.items():
            results[name] = compute_result.results
        
        return {
            'results': results,
            'kpi_status': {name: r.to_dict() for name, r in self._kpi_results.items()},
            'report_files': report_files,
            'metadata': self.reporter.metadata,
            'timing_stats': self._timing_stats.to_dict()
        }


def setup_logging(verbose: bool = False):
    """配置日志"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('kpi_analysis.log', encoding='utf-8')
        ]
    )


def main():
    """主函数"""
    parser = argparse.ArgumentParser(
        description='Hello KPI - 自动驾驶KPI分析工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 单bag分析
  python -m src.main /path/to/ros2bag
  python -m src.main /path/to/ros2bag -o ./my_output
  
  # 多bag合并分析（自动扫描目录下所有bag）
  python -m src.main /path/to/bag_directory -m
  python -m src.main /path/to/bag_directory -m -o ./merged_output
  
  # 使用自定义配置
  python -m src.main /path/to/ros2bag -c ./my_config.yaml
  
  # 指定并行线程数
  python -m src.main /path/to/ros2bag --workers 8
        """
    )
    
    parser.add_argument('bag_path', 
                        help='ROS2 bag文件夹路径或包含多个bag的目录')
    parser.add_argument('-o', '--output', 
                        default='./output',
                        help='输出目录 (默认: ./output)')
    parser.add_argument('-c', '--config',
                        default=None,
                        help='配置文件路径 (默认: config/kpi_config.yaml)')
    parser.add_argument('-m', '--multi-bag',
                        action='store_true',
                        help='多bag模式：自动扫描目录下所有bag文件并合并分析')
    parser.add_argument('--no-frame-store',
                        action='store_true',
                        help='禁用 FrameStore 模式（默认启用，自动处理大数据量）')
    parser.add_argument('--force-disk',
                        action='store_true',
                        help='强制使用磁盘缓存模式（用于测试或内存受限场景）')
    parser.add_argument('-s','--true-streaming',
                        action='store_true',
                        help='使用流式模式（内存最优，默认启用缓存）')
    parser.add_argument('-w', '--workers',
                        type=int,
                        default=8,
                        help='并行计算KPI的线程数 (默认: 8)')
    parser.add_argument('--no-cache',
                        action='store_true',
                        help='禁用持久化缓存')
    parser.add_argument('--clear-cache',
                        action='store_true',
                        help='清除缓存后运行')
    parser.add_argument('--cache-dir',
                        default='.frame_cache',
                        help='缓存目录 (默认: .frame_cache)')
    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='详细输出')
    
    args = parser.parse_args()
    
    # 配置日志
    setup_logging(args.verbose)
    
    # 检查bag路径
    if not os.path.exists(args.bag_path):
        print(f"错误: Bag路径不存在: {args.bag_path}")
        sys.exit(1)
    
    # 处理缓存选项
    use_cache = not args.no_cache
    if args.clear_cache:
        cache = FrameStoreCache(args.cache_dir)
        count = cache.clear()
        print(f"已清除 {count} 个缓存文件")
    
    # 创建分析器并执行
    analyzer = KPIAnalyzer(
        config_path=args.config,
        max_workers=args.workers,
        use_cache=use_cache,
        cache_dir=args.cache_dir
    )
    
    try:
        # 选择分析模式
        if args.true_streaming:
            # 流式模式：内存最优 + 可选缓存
            results = analyzer.analyze_true_streaming(
                args.bag_path, args.output, use_cache=use_cache)
        elif args.force_disk:
            # 强制磁盘模式
            results = analyzer.analyze_with_frame_store(
                args.bag_path, args.output, force_disk=True)
        else:
            # 默认：自动选择模式
            results = analyzer.analyze(args.bag_path, args.output, 
                                       multi_bag=args.multi_bag,
                                       use_frame_store=not args.no_frame_store)
        
        # 检查是否有失败的 KPI
        failed_kpis = [name for name, r in results.get('kpi_status', {}).items() 
                      if r.get('status') == KPIStatus.FAILED]
        if failed_kpis:
            logger.warning(f"以下 KPI 计算失败: {failed_kpis}")
        
        return 0
    except Exception as e:
        logger.error(f"分析失败: {e}", exc_info=True)
        print(f"\n错误: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
