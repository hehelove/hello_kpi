"""
多Bag文件读取器
支持读取目录下多个连续的ROS2 bag文件并合并
支持多线程加速读取
"""
import os
from typing import Dict, List, Optional, Tuple, Callable
from pathlib import Path
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
from tqdm import tqdm

from .bag_reader import BagReader, TopicData


@dataclass
class BagInfo:
    """单个Bag文件信息"""
    path: Path
    start_time: float
    end_time: float
    duration: float
    
    def __lt__(self, other):
        """按开始时间排序"""
        return self.start_time < other.start_time


class MultiBagReader:
    """多Bag文件读取器 - 按时间顺序合并多个连续的bag文件，支持多线程加速"""
    
    def __init__(self, bag_dir: str, pattern: str = "*", max_workers: int = 8):
        """
        初始化多Bag读取器
        
        Args:
            bag_dir: 包含多个bag文件夹的目录路径
            pattern: bag文件夹名称匹配模式（默认: "*" 匹配所有）
            max_workers: 最大并行线程数（默认: 4）
        """
        self.bag_dir = Path(bag_dir)
        if not self.bag_dir.exists():
            raise FileNotFoundError(f"Bag directory not found: {bag_dir}")
        
        self.pattern = pattern
        self.max_workers = max_workers
        self.bag_infos: List[BagInfo] = []
        self._discover_bags()
    
    def _discover_bags(self):
        """
        发现目录下的所有bag文件夹
        
        支持两种模式：
        1. 单bag模式：当前目录直接包含多个连续的.db3文件（如 file_0.db3, file_1.db3 等）
           这些.db3文件会被合并为一个bag读取
        2. 多bag模式：当前目录包含多个子目录，每个子目录是一个独立的bag
        """
        print(f"  扫描目录: {self.bag_dir}")
        
        # 首先检查当前目录是否直接包含.db3文件
        db3_files_in_current_dir = sorted(self.bag_dir.glob("*.db3"))
        if db3_files_in_current_dir:
            # 当前目录本身就是bag文件夹，包含多个连续的.db3文件
            print(f"  [模式] 单bag模式 - 当前目录包含 {len(db3_files_in_current_dir)} 个连续的.db3文件")
            for f in db3_files_in_current_dir:
                print(f"    - {f.name}")
            bag_paths = [self.bag_dir]
        else:
            # 查找子目录中的bag文件夹
            print(f"  [模式] 多bag模式 - 扫描子目录...")
            bag_paths = []
            for item in sorted(self.bag_dir.iterdir()):
                if item.is_dir():
                    # 检查是否是有效的bag文件夹（包含.db3文件）
                    db3_files = list(item.glob("*.db3"))
                    if db3_files:
                        bag_paths.append(item)
        
        if not bag_paths:
            raise ValueError(f"在目录 {self.bag_dir} 中未找到有效的bag文件（需要.db3文件）")
        
        print(f"  发现 {len(bag_paths)} 个bag目录")
        
        # 读取每个bag的时间信息
        for bag_path in tqdm(bag_paths, desc="读取bag信息"):
            try:
                # BagReader 会自动生成缺失的 metadata.yaml
                reader = BagReader(str(bag_path))
                start_time, end_time = reader.get_time_range()
                duration = end_time - start_time
                
                self.bag_infos.append(BagInfo(
                    path=bag_path,
                    start_time=start_time,
                    end_time=end_time,
                    duration=duration
                ))
            except Exception as e:
                print(f"\n    警告: 无法读取 {bag_path}: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        if not self.bag_infos:
            raise ValueError(f"无法读取任何有效的bag文件")
        
        # 按开始时间排序
        self.bag_infos.sort()
        
        # 打印bag信息
        print(f"\n  Bag文件信息:")
        for i, info in enumerate(self.bag_infos):
            print(f"    [{i+1}] {info.path.name}")
            print(f"        时间: {info.start_time:.2f} - {info.end_time:.2f} ({info.duration:.2f}s)")
            
            if i > 0:
                gap = info.start_time - self.bag_infos[i-1].end_time
                if gap < 0.2:  # 小于1秒认为是连续的
                    print(f"        与前一个bag间隔: {gap:.2f}s (连续)")
                else:
                    print(f"        与前一个bag间隔: {gap:.2f}s (可能有间隔)")
    
    def get_time_range(self) -> Tuple[float, float]:
        """获取所有bag合并后的时间范围"""
        if not self.bag_infos:
            return 0.0, 0.0
        
        start_time = self.bag_infos[0].start_time
        end_time = self.bag_infos[-1].end_time
        
        return start_time, end_time
    
    def get_duration(self) -> float:
        """获取总时长"""
        start, end = self.get_time_range()
        return end - start
    
    def get_topic_info(self) -> Dict[str, Dict]:
        """
        获取所有bag中topic的合并信息
        
        Returns:
            topic信息字典
        """
        all_topics = {}
        
        for bag_info in self.bag_infos:
            reader = BagReader(str(bag_info.path))
            topics = reader.get_topic_info()
            
            for topic, info in topics.items():
                if topic not in all_topics:
                    all_topics[topic] = {
                        'type': info.get('type', ''),
                        'count': 0,
                        'bags': []
                    }
                all_topics[topic]['count'] += info.get('count', 0)
                all_topics[topic]['bags'].append(bag_info.path.name)
        
        return all_topics
    
    def _read_single_bag(self, bag_info: BagInfo, topics: List[str],
                         time_range: Optional[Tuple[float, float]] = None,
                         sample_interval: Optional[float] = None,
                         light_mode: bool = False) -> Tuple[Path, Dict[str, TopicData]]:
        """
        读取单个bag的topics（用于多线程）
        
        Args:
            bag_info: bag信息
            topics: 要读取的topic列表
            time_range: 可选的时间范围过滤
            sample_interval: 可选的采样间隔
            light_mode: 轻量模式，只保留必要字段
            
        Returns:
            (bag路径, topic数据字典)
        """
        reader = BagReader(str(bag_info.path))
        bag_data = reader.read_topics(topics, progress=False, 
                                       time_range=time_range,
                                       sample_interval=sample_interval,
                                       light_mode=light_mode)
        return bag_info.path, bag_data
    
    def read_topics(self, topics: List[str], progress: bool = True, 
                    use_multithread: bool = True,
                    time_range: Optional[Tuple[float, float]] = None,
                    sample_interval: Optional[float] = None,
                    light_mode: bool = False) -> Dict[str, TopicData]:
        """
        从所有bag文件中读取指定topics并合并
        
        Args:
            topics: 要读取的topic列表
            progress: 是否显示进度条
            use_multithread: 是否使用多线程加速（默认: True）
            time_range: 可选的时间范围过滤 (start_sec, end_sec)
            sample_interval: 可选的采样间隔（秒），用于降采样减少内存占用
            light_mode: 轻量模式，只保留 KPI 计算需要的字段，大幅降低内存占用
            
        Returns:
            合并后的topic数据字典
        """
        # 初始化结果字典
        result: Dict[str, TopicData] = {}
        for topic in topics:
            result[topic] = TopicData(topic_name=topic)
        
        total_bags = len(self.bag_infos)
        
        # 根据时间范围过滤需要读取的 bag
        bags_to_read = self.bag_infos
        if time_range is not None:
            start_t, end_t = time_range
            bags_to_read = [
                info for info in self.bag_infos
                if not (info.end_time < start_t or info.start_time > end_t)
            ]
            if progress and len(bags_to_read) < total_bags:
                print(f"  [时间范围过滤] 跳过 {total_bags - len(bags_to_read)} 个不在范围内的 bag")
            total_bags = len(bags_to_read)
        
        if total_bags == 0:
            if progress:
                print(f"  [警告] 没有 bag 文件在指定时间范围内")
            return result
        
        if use_multithread and total_bags > 1:
            # 多线程并行读取
            if progress:
                mode_str = "[轻量模式] " if light_mode else ""
                print(f"\n  {mode_str}[多线程模式] 使用 {min(self.max_workers, total_bags)} 个线程并行读取 {total_bags} 个bag...")
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # 提交所有任务
                futures = {
                    executor.submit(self._read_single_bag, bag_info, topics, 
                                   time_range, sample_interval, light_mode): bag_info 
                    for bag_info in bags_to_read
                }
                
                # 使用tqdm显示进度
                completed = 0
                for future in as_completed(futures):
                    bag_info = futures[future]
                    try:
                        bag_path, bag_data = future.result()
                        
                        # 合并到结果中
                        for topic, topic_data in bag_data.items():
                            if topic in result:
                                result[topic].extend(topic_data)
                        
                        completed += 1
                        if progress:
                            print(f"    [{completed}/{total_bags}] 完成: {bag_path.name}")
                            
                    except Exception as e:
                        print(f"    [错误] 读取 {bag_info.path.name} 失败: {e}")
        else:
            # 单线程顺序读取
            for bag_idx, bag_info in enumerate(bags_to_read):
                if progress:
                    mode_str = "[轻量] " if light_mode else ""
                    print(f"\n  {mode_str}读取 Bag [{bag_idx+1}/{total_bags}]: {bag_info.path.name}")
                
                reader = BagReader(str(bag_info.path))
                bag_data = reader.read_topics(topics, progress=False,
                                               time_range=time_range,
                                               sample_interval=sample_interval,
                                               light_mode=light_mode)
                
                # 合并到结果中
                for topic, topic_data in bag_data.items():
                    if topic in result:
                        result[topic].extend(topic_data)
        
        # 对每个topic按时间戳排序（因为多个bag可能有时间重叠或乱序）
        if progress:
            print(f"\n  按时间戳排序合并数据...")
        
        for topic, topic_data in result.items():
            if len(topic_data.timestamps) > 0:
                # 使用numpy排序
                indices = np.argsort(topic_data.timestamps)
                topic_data.timestamps = [topic_data.timestamps[i] for i in indices]
                topic_data.messages = [topic_data.messages[i] for i in indices]
        
        # 打印统计信息
        if progress:
            print(f"\n  合并后数据统计:")
            for topic, topic_data in result.items():
                if len(topic_data.messages) > 0:
                    print(f"    - {topic}: {len(topic_data.messages)} 条消息")
        
        return result
    
    def get_bag_list(self) -> List[str]:
        """获取bag文件列表（按时间顺序）"""
        return [str(info.path) for info in self.bag_infos]
    
    def get_bag_infos(self) -> List[BagInfo]:
        """获取所有 bag 的详细信息"""
        return self.bag_infos
    
    def read_topics_per_bag(self, topics: List[str], progress: bool = True) -> List[Tuple[BagInfo, Dict[str, TopicData]]]:
        """
        分别读取每个 bag 的 topic 数据（不合并）
        
        用于需要分 bag 计算的场景，避免 bag 间隔导致的计算错误
        
        Args:
            topics: 要读取的topic列表
            progress: 是否显示进度条
            
        Returns:
            [(bag_info, topic_data), ...] 列表，按时间顺序排列
        """
        results = []
        total_bags = len(self.bag_infos)
        
        if progress:
            print(f"\n  [分bag模式] 读取 {total_bags} 个bag...")
        
        # 使用多线程读取
        if total_bags > 1 and self.max_workers > 1:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._read_single_bag, bag_info, topics): bag_info 
                    for bag_info in self.bag_infos
                }
                
                bag_data_map = {}
                for future in as_completed(futures):
                    bag_info = futures[future]
                    try:
                        bag_path, bag_data = future.result()
                        bag_data_map[str(bag_path)] = (bag_info, bag_data)
                        if progress:
                            print(f"    完成: {bag_path.name}")
                    except Exception as e:
                        print(f"    [错误] 读取 {bag_info.path.name} 失败: {e}")
                
                # 按时间顺序排列
                for bag_info in self.bag_infos:
                    key = str(bag_info.path)
                    if key in bag_data_map:
                        results.append(bag_data_map[key])
        else:
            for bag_info in self.bag_infos:
                if progress:
                    print(f"    读取: {bag_info.path.name}")
                reader = BagReader(str(bag_info.path))
                bag_data = reader.read_topics(topics, progress=False)
                results.append((bag_info, bag_data))
        
        return results
    
    def has_gaps(self, max_gap_seconds: float = 1.0) -> bool:
        """
        检查 bag 之间是否有间隔
        
        Args:
            max_gap_seconds: 最大允许间隔（秒），超过此值认为有间隔（默认1秒）
            
        Returns:
            True 如果有间隔
        """
        if len(self.bag_infos) <= 1:
            return False
        
        for i in range(1, len(self.bag_infos)):
            gap = self.bag_infos[i].start_time - self.bag_infos[i-1].end_time
            if gap > max_gap_seconds:
                return True
        return False
    
    def get_gaps(self) -> List[Tuple[int, int, float]]:
        """
        获取所有 bag 之间的间隔信息
        
        Returns:
            [(bag1_idx, bag2_idx, gap_seconds), ...]
        """
        gaps = []
        for i in range(1, len(self.bag_infos)):
            gap = self.bag_infos[i].start_time - self.bag_infos[i-1].end_time
            gaps.append((i-1, i, gap))
        return gaps
    
    def iter_bags(self, topics: List[str],
                  batch_callback: Callable[[BagInfo, Dict[str, TopicData]], None],
                  time_range: Optional[Tuple[float, float]] = None,
                  sample_interval: Optional[float] = None,
                  progress: bool = True) -> int:
        """
        流式迭代处理每个 bag（内存友好模式）
        
        适用于超大数据集，每次只加载一个 bag 到内存，处理完立即释放。
        
        Args:
            topics: 要读取的 topic 列表
            batch_callback: 处理每个 bag 的回调函数 (bag_info, topic_data) -> None
            time_range: 可选的时间范围过滤
            sample_interval: 可选的采样间隔
            progress: 是否显示进度
            
        Returns:
            处理的 bag 数量
        
        Example:
            results = []
            def process_bag(bag_info, data):
                # 处理单个 bag 的数据
                result = compute_kpi(data)
                results.append(result)
            
            reader.iter_bags(topics, process_bag)
        """
        import gc
        
        # 根据时间范围过滤
        bags_to_read = self.bag_infos
        if time_range is not None:
            start_t, end_t = time_range
            bags_to_read = [
                info for info in self.bag_infos
                if not (info.end_time < start_t or info.start_time > end_t)
            ]
        
        total_bags = len(bags_to_read)
        if progress:
            print(f"\n  [流式模式] 逐个处理 {total_bags} 个 bag...")
        
        for bag_idx, bag_info in enumerate(bags_to_read):
            if progress:
                print(f"  [{bag_idx+1}/{total_bags}] 处理: {bag_info.path.name}")
            
            # 读取单个 bag
            reader = BagReader(str(bag_info.path))
            bag_data = reader.read_topics(topics, progress=False,
                                           time_range=time_range,
                                           sample_interval=sample_interval)
            
            # 回调处理
            try:
                batch_callback(bag_info, bag_data)
            except Exception as e:
                print(f"    [错误] 处理 {bag_info.path.name} 失败: {e}")
            
            # 释放内存
            for td in bag_data.values():
                td.clear()
            bag_data.clear()
            gc.collect()
        
        return total_bags
    
    def estimate_memory_usage(self, topics: List[str]) -> Dict:
        """
        估算读取指定 topics 需要的内存
        
        Returns:
            内存估算信息
        """
        topic_info = self.get_topic_info()
        
        total_messages = 0
        topic_counts = {}
        
        for topic in topics:
            if topic in topic_info:
                count = topic_info[topic].get('count', 0)
                if isinstance(count, int):
                    total_messages += count
                    topic_counts[topic] = count
        
        # 估算每条消息平均大小（保守估计）
        # 小消息（状态）约 1KB，大消息（感知）约 10KB
        avg_msg_size_kb = 5  # 5KB 平均值
        estimated_mb = (total_messages * avg_msg_size_kb) / 1024
        
        return {
            'total_messages': total_messages,
            'topic_counts': topic_counts,
            'estimated_memory_mb': round(estimated_mb, 1),
            'recommended_sample_interval': 0.1 if estimated_mb > 2000 else None,
            'warning': '预计内存占用较大，建议使用 sample_interval 降采样' if estimated_mb > 2000 else None
        }
