"""
帧数据存储抽象层

提供统一的帧数据访问接口，支持内存模式和磁盘模式自动切换。
KPI 计算代码无需感知数据来源，实现透明访问。

支持持久化缓存：
    - 首次分析时将 synced_frames 缓存到磁盘
    - 再次分析同一 bag 时直接加载缓存，跳过解析步骤
    - 基于文件哈希校验缓存有效性

使用方式：
    # 自动选择模式
    frame_store = create_frame_store(estimated_frames, cache_dir)
    
    # 写入数据
    frame_store.append_batch(synced_frames, bag_name)
    
    # 迭代访问（KPI 代码中使用）
    for frame in frame_store:
        process(frame)
"""

from abc import ABC, abstractmethod
from typing import Iterator, List, Optional, Dict, Any
from dataclasses import dataclass, field
from pathlib import Path
import sqlite3
import pickle
import zlib
import hashlib
import json
import os
import gc
import logging

logger = logging.getLogger(__name__)


# ============================================================================
# 持久化缓存管理
# ============================================================================

class FrameStoreCache:
    """
    持久化缓存管理器
    
    缓存 synced_frames 数据，基于 bag 文件哈希校验有效性。
    """
    
    CACHE_VERSION = "2.0"
    
    def __init__(self, cache_dir: str = ".frame_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def _compute_bag_hash(self, bag_path: str) -> str:
        """
        计算 bag 目录的哈希值（基于文件列表、大小和修改时间）
        """
        bag_path = Path(bag_path)
        
        if not bag_path.exists():
            return ""
        
        hash_input = []
        
        # 收集所有 .db3 文件信息
        if bag_path.is_dir():
            db3_files = sorted(bag_path.glob("**/*.db3"))
        else:
            db3_files = [bag_path] if bag_path.suffix == '.db3' else []
        
        for f in db3_files:
            stat = f.stat()
            hash_input.append(f"{f.name}:{stat.st_size}:{int(stat.st_mtime)}")
        
        # 检查 metadata.yaml
        if bag_path.is_dir():
            metadata_file = bag_path / "metadata.yaml"
            if metadata_file.exists():
                stat = metadata_file.stat()
                hash_input.append(f"metadata.yaml:{stat.st_size}:{int(stat.st_mtime)}")
        
        # 生成哈希
        hash_str = "|".join(hash_input)
        return hashlib.md5(hash_str.encode()).hexdigest()[:16]
    
    def _get_cache_path(self, bag_hash: str) -> Path:
        """获取缓存数据库路径"""
        return self.cache_dir / f"frames_{bag_hash}.db"
    
    def _get_meta_path(self, bag_hash: str) -> Path:
        """获取缓存元数据路径"""
        return self.cache_dir / f"frames_{bag_hash}.json"
    
    def is_valid(self, bag_path: str) -> bool:
        """检查缓存是否有效"""
        bag_hash = self._compute_bag_hash(bag_path)
        meta_path = self._get_meta_path(bag_hash)
        cache_path = self._get_cache_path(bag_hash)
        
        if not meta_path.exists() or not cache_path.exists():
            return False
        
        try:
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            
            # 检查版本
            if meta.get('version') != self.CACHE_VERSION:
                logger.info(f"缓存版本不匹配: {meta.get('version')} != {self.CACHE_VERSION}")
                return False
            
            # 检查哈希
            if meta.get('bag_hash') != bag_hash:
                logger.info("Bag 文件已变更，缓存失效")
                return False
            
            return True
        except Exception as e:
            logger.warning(f"读取缓存元数据失败: {e}")
            return False
    
    def load(self, bag_path: str) -> Optional['DiskFrameStore']:
        """
        加载缓存的 FrameStore
        
        Returns:
            DiskFrameStore 实例，如果缓存无效返回 None
        """
        if not self.is_valid(bag_path):
            return None
        
        bag_hash = self._compute_bag_hash(bag_path)
        cache_path = self._get_cache_path(bag_hash)
        meta_path = self._get_meta_path(bag_hash)
        
        try:
            with open(meta_path, 'r') as f:
                meta = json.load(f)
            
            # 创建 DiskFrameStore 并设置已有缓存
            store = DiskFrameStore(
                cache_dir=str(self.cache_dir),
                persistent=True,
                existing_db=str(cache_path)
            )
            store._frame_count = meta.get('frame_count', 0)
            store._bag_infos = [
                FrameStoreBagInfo(**info) for info in meta.get('bag_infos', [])
            ]
            store._finalized = True
            
            logger.info(f"从缓存加载 FrameStore: {store._frame_count:,} 帧")
            return store
        except Exception as e:
            logger.error(f"加载缓存失败: {e}")
            return None
    
    def save_meta(self, bag_path: str, frame_store: 'DiskFrameStore'):
        """保存缓存元数据"""
        bag_hash = self._compute_bag_hash(bag_path)
        meta_path = self._get_meta_path(bag_hash)
        
        meta = {
            'version': self.CACHE_VERSION,
            'bag_hash': bag_hash,
            'bag_path': str(bag_path),
            'frame_count': len(frame_store),
            'bag_infos': [
                {'name': info.name, 'frame_count': info.frame_count,
                 'start_timestamp': info.start_timestamp, 'end_timestamp': info.end_timestamp}
                for info in frame_store.get_bag_infos()
            ]
        }
        
        with open(meta_path, 'w') as f:
            json.dump(meta, f, indent=2)
        
        logger.info(f"缓存元数据已保存: {meta_path}")
    
    def get_cache_path_for_bag(self, bag_path: str) -> str:
        """获取 bag 对应的缓存路径（用于 DiskFrameStore）"""
        bag_hash = self._compute_bag_hash(bag_path)
        return str(self._get_cache_path(bag_hash))
    
    def clear(self, bag_path: Optional[str] = None) -> int:
        """
        清除缓存
        
        Args:
            bag_path: 如果指定，只清除该 bag 的缓存；否则清除所有缓存
            
        Returns:
            清除的文件数
        """
        count = 0
        
        if bag_path:
            bag_hash = self._compute_bag_hash(bag_path)
            for suffix in ['.db', '.json', '.db-wal', '.db-shm']:
                path = self.cache_dir / f"frames_{bag_hash}{suffix}"
                if path.exists():
                    path.unlink()
                    count += 1
        else:
            for f in self.cache_dir.glob("frames_*"):
                f.unlink()
                count += 1
        
        logger.info(f"已清除 {count} 个缓存文件")
        return count
    
    def get_stats(self) -> Dict:
        """获取缓存统计"""
        db_files = list(self.cache_dir.glob("*.db"))
        total_size = sum(f.stat().st_size for f in db_files)
        
        return {
            'cache_dir': str(self.cache_dir),
            'cached_bags': len(db_files),
            'total_size_mb': round(total_size / (1024 * 1024), 2)
        }


@dataclass
class FrameStoreBagInfo:
    """
    FrameStore 内部使用的 Bag 信息
    
    注意：这个类与 multi_bag_reader.BagInfo 不同，用于 FrameStore 内部存储。
    """
    name: str
    frame_count: int
    start_timestamp: float = 0.0
    end_timestamp: float = 0.0
    
    def to_multi_bag_info(self):
        """
        转换为 multi_bag_reader.BagInfo 格式
        
        用于 KPI 计算时的事件溯源
        """
        from .multi_bag_reader import BagInfo as MultiBagInfo
        return MultiBagInfo(
            path=Path(self.name),  # 用 name 作为路径
            start_time=self.start_timestamp,
            end_time=self.end_timestamp,
            duration=self.end_timestamp - self.start_timestamp
        )


# 为了兼容性，提供别名
BagInfo = FrameStoreBagInfo


class FrameStore(ABC):
    """
    帧数据存储抽象接口
    
    支持两种实现：
    1. MemoryFrameStore - 内存存储，适合小数据量
    2. DiskFrameStore - 磁盘存储，适合大数据量
    """
    
    @abstractmethod
    def __iter__(self) -> Iterator:
        """迭代所有帧（按时间戳排序）"""
        pass
    
    @abstractmethod
    def __len__(self) -> int:
        """返回帧总数"""
        pass
    
    @abstractmethod
    def append_batch(self, frames: List, bag_name: str):
        """
        追加一批帧数据
        
        Args:
            frames: SyncedFrame 列表
            bag_name: 来源 bag 名称
        """
        pass
    
    @abstractmethod
    def get_bag_infos(self) -> List[FrameStoreBagInfo]:
        """获取所有 bag 信息"""
        pass
    
    @abstractmethod
    def finalize(self):
        """完成数据写入，准备读取（如排序）"""
        pass
    
    def cleanup(self):
        """清理资源（子类可重写）"""
        pass


class MemoryFrameStore(FrameStore):
    """
    内存模式帧存储
    
    适用于小数据量场景，数据直接保存在内存中。
    """
    
    def __init__(self):
        self._frames: List = []
        self._bag_infos: List[FrameStoreBagInfo] = []
        self._finalized = False
    
    def __iter__(self) -> Iterator:
        if not self._finalized:
            self.finalize()
        return iter(self._frames)
    
    def __len__(self) -> int:
        return len(self._frames)
    
    def append_batch(self, frames: List, bag_name: str):
        """追加帧数据"""
        if not frames:
            return
        
        start_ts = frames[0].timestamp if frames else 0
        end_ts = frames[-1].timestamp if frames else 0
        
        self._frames.extend(frames)
        self._bag_infos.append(FrameStoreBagInfo(
            name=bag_name,
            frame_count=len(frames),
            start_timestamp=start_ts,
            end_timestamp=end_ts
        ))
        self._finalized = False
    
    def get_bag_infos(self) -> List[FrameStoreBagInfo]:
        return self._bag_infos
    
    def finalize(self):
        """按时间戳排序"""
        if self._finalized:
            return
        self._frames.sort(key=lambda f: f.timestamp)
        self._finalized = True
        logger.info(f"MemoryFrameStore: 完成排序，共 {len(self._frames):,} 帧")


class DiskFrameStore(FrameStore):
    """
    磁盘模式帧存储
    
    使用 SQLite 存储帧数据，支持大数据量场景。
    特点：
    1. 增量写入：每批数据立即写入磁盘
    2. 压缩存储：使用 zlib 压缩减少磁盘占用
    3. 分批读取：迭代时分批加载，控制内存使用
    4. 持久化缓存：支持跨会话复用（可选）
    """
    
    BATCH_SIZE = 5000  # 每批读取的帧数
    
    def __init__(self, cache_dir: str, persistent: bool = False, existing_db: str = None, 
                 db_path: str = None):
        """
        初始化磁盘帧存储
        
        Args:
            cache_dir: 缓存目录
            persistent: 是否持久化（不自动清理）
            existing_db: 已存在的数据库路径（用于加载缓存，跳过初始化）
            db_path: 指定数据库路径（用于新建，会初始化）
        """
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self.persistent = persistent
        
        if existing_db:
            # 使用已存在的数据库（加载缓存）
            self.db_path = existing_db
        elif db_path:
            # 使用指定路径（新建）
            self.db_path = db_path
        else:
            self.db_path = os.path.join(cache_dir, "frames.db")
        
        self._frame_count = 0
        self._bag_infos: List[FrameStoreBagInfo] = []
        self._finalized = False
        
        if not existing_db:
            self._init_db()
    
    def _init_db(self):
        """初始化数据库"""
        # 如果存在旧的缓存，先删除
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        conn = sqlite3.connect(self.db_path)
        
        # 创建帧表
        conn.execute("""
            CREATE TABLE frames (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                bag_name TEXT,
                data BLOB NOT NULL
            )
        """)
        
        # 创建 bag 信息表
        conn.execute("""
            CREATE TABLE bag_infos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                frame_count INTEGER,
                start_timestamp REAL,
                end_timestamp REAL
            )
        """)
        
        # 优化配置
        conn.execute("PRAGMA journal_mode=WAL")  # WAL 模式提高并发性能
        conn.execute("PRAGMA synchronous=NORMAL")  # 平衡安全性和性能
        conn.execute("PRAGMA cache_size=-64000")  # 64MB 缓存
        
        conn.commit()
        conn.close()
        
        logger.info(f"DiskFrameStore: 初始化数据库 {self.db_path}")
    
    def append_batch(self, frames: List, bag_name: str):
        """
        批量写入帧数据（高性能）
        
        优化点：
        1. 使用 pickle.HIGHEST_PROTOCOL 最快序列化
        2. zlib level=1 快速压缩（压缩率约 50-70%）
        3. 批量 INSERT 减少 IO 开销
        4. 配合轻量模式（light_mode=True），序列化数据更小
        """
        if not frames:
            return
        
        start_ts = frames[0].timestamp
        end_ts = frames[-1].timestamp
        
        conn = sqlite3.connect(self.db_path)
        
        # 批量插入帧数据（压缩存储）
        data_to_insert = []
        for frame in frames:
            # 序列化并压缩
            # 注：如果使用轻量模式，frame.messages 中是 LightMessage 对象，序列化更快更小
            serialized = pickle.dumps(frame, protocol=pickle.HIGHEST_PROTOCOL)
            compressed = zlib.compress(serialized, level=1)  # level=1 快速压缩
            data_to_insert.append((frame.timestamp, bag_name, compressed))
        
        conn.executemany(
            "INSERT INTO frames (timestamp, bag_name, data) VALUES (?, ?, ?)",
            data_to_insert
        )
        
        # 记录 bag 信息
        conn.execute(
            "INSERT INTO bag_infos (name, frame_count, start_timestamp, end_timestamp) VALUES (?, ?, ?, ?)",
            (bag_name, len(frames), start_ts, end_ts)
        )
        
        conn.commit()
        conn.close()
        
        self._frame_count += len(frames)
        self._bag_infos.append(FrameStoreBagInfo(
            name=bag_name,
            frame_count=len(frames),
            start_timestamp=start_ts,
            end_timestamp=end_ts
        ))
        
        logger.debug(f"DiskFrameStore: 写入 {len(frames):,} 帧 from {bag_name}")
    
    def __iter__(self) -> Iterator:
        """分批迭代（内存友好）"""
        if not self._finalized:
            self.finalize()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT data FROM frames ORDER BY timestamp"
        )
        
        batch_count = 0
        while True:
            rows = cursor.fetchmany(self.BATCH_SIZE)
            if not rows:
                break
            
            batch_count += 1
            for row in rows:
                # 解压并反序列化
                decompressed = zlib.decompress(row[0])
                frame = pickle.loads(decompressed)
                yield frame
            
            # 每批处理后触发 GC
            if batch_count % 10 == 0:
                gc.collect()
        
        conn.close()
    
    def __len__(self) -> int:
        return self._frame_count
    
    def get_bag_infos(self) -> List[FrameStoreBagInfo]:
        return self._bag_infos
    
    def finalize(self):
        """创建索引，准备读取"""
        if self._finalized:
            return
        
        conn = sqlite3.connect(self.db_path)
        
        # 创建时间戳索引
        conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON frames(timestamp)")
        
        # 分析优化
        conn.execute("ANALYZE")
        
        conn.commit()
        conn.close()
        
        self._finalized = True
        logger.info(f"DiskFrameStore: 完成索引，共 {self._frame_count:,} 帧")
    
    def cleanup(self):
        """清理缓存文件（持久化模式下跳过）"""
        if self.persistent:
            logger.info(f"DiskFrameStore: 持久化模式，保留缓存 {self.db_path}")
            return
        
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
            logger.info(f"DiskFrameStore: 清理缓存 {self.db_path}")
        
        # 清理 WAL 文件
        wal_path = self.db_path + "-wal"
        shm_path = self.db_path + "-shm"
        if os.path.exists(wal_path):
            os.remove(wal_path)
        if os.path.exists(shm_path):
            os.remove(shm_path)
        
        # 尝试删除缓存目录（如果为空）
        try:
            os.rmdir(self.cache_dir)
        except OSError:
            pass  # 目录不为空，保留
    
    def iterate_by_bag(self, bag_name: str) -> Iterator:
        """
        按 bag 名称迭代帧数据（内存友好）
        
        Args:
            bag_name: bag 名称
            
        Yields:
            该 bag 的所有帧（按时间戳排序）
        """
        if not self._finalized:
            self.finalize()
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute(
            "SELECT data FROM frames WHERE bag_name = ? ORDER BY timestamp",
            (bag_name,)
        )
        
        while True:
            rows = cursor.fetchmany(self.BATCH_SIZE)
            if not rows:
                break
            
            for row in rows:
                decompressed = zlib.decompress(row[0])
                frame = pickle.loads(decompressed)
                yield frame
        
        conn.close()
    
    def get_bag_names(self) -> List[str]:
        """获取所有 bag 名称列表"""
        return [info.name for info in self._bag_infos]
    
    def get_stats(self) -> Dict[str, Any]:
        """获取存储统计信息"""
        db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
        return {
            'frame_count': self._frame_count,
            'bag_count': len(self._bag_infos),
            'db_size_mb': round(db_size / (1024 * 1024), 2),
            'finalized': self._finalized
        }


# 内存阈值：帧数超过此值时自动切换到磁盘模式
MEMORY_THRESHOLD_FRAMES = 100_000  # 10万帧约 600-800MB 内存


def create_frame_store(
    estimated_frames: int = 0,
    cache_dir: Optional[str] = None,
    force_disk: bool = False
) -> FrameStore:
    """
    工厂函数：根据数据量自动选择存储模式
    
    Args:
        estimated_frames: 预估帧数
        cache_dir: 缓存目录（指定则强制使用磁盘模式）
        force_disk: 强制使用磁盘模式
        
    Returns:
        FrameStore 实例
    """
    use_disk = force_disk or cache_dir is not None or estimated_frames > MEMORY_THRESHOLD_FRAMES
    
    if use_disk:
        if cache_dir is None:
            cache_dir = "/tmp/hello_kpi_cache"
        logger.info(f"[FrameStore] 使用磁盘模式，缓存目录: {cache_dir}")
        return DiskFrameStore(cache_dir)
    else:
        logger.info(f"[FrameStore] 使用内存模式")
        return MemoryFrameStore()


def estimate_frame_count(bag_paths: List[str], avg_hz: float = 20.0) -> int:
    """
    估算帧数
    
    基于文件大小和平均频率估算，用于决定存储模式。
    
    Args:
        bag_paths: bag 文件路径列表
        avg_hz: 平均帧率
        
    Returns:
        预估帧数
    """
    total_size = 0
    for path in bag_paths:
        if os.path.isfile(path):
            total_size += os.path.getsize(path)
        elif os.path.isdir(path):
            for f in os.listdir(path):
                if f.endswith('.db3'):
                    total_size += os.path.getsize(os.path.join(path, f))
    
    # 经验公式：1GB 数据约 10-15 分钟，按 20Hz 约 12000-18000 帧
    # 保守估计：1GB = 15000 帧
    estimated = int(total_size / (1024 * 1024 * 1024) * 15000)
    
    logger.info(f"[FrameStore] 预估帧数: {estimated:,} (基于 {total_size/(1024*1024):.1f}MB 数据)")
    
    return max(estimated, 1000)  # 至少返回 1000
