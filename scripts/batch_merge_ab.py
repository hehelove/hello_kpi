#!/usr/bin/env python3
"""
批量合并 A/B 后缀的 db3 文件（多进程高性能版）

性能优化:
  - 使用多进程并行（避免 Python GIL 限制）
  - 内联 SQLite 合并（避免子进程启动开销）
  - 增大 SQLite 缓存和内存映射
  - 延迟创建索引

支持两种模式:
  1. 单目录模式: A/B 子目录在同一个父目录下
  2. 双目录模式: A 和 B 分别在不同目录

目录结构示例 (单目录 - 子目录模式):
    /dat/rosbag/20251213/1_010/
    ├── 1_010_20251213-111122-A/
    │   └── 1_010_20251213-111122-A_0.db3
    ├── 1_010_20251213-111122-B/
    │   └── 1_010_20251213-111122-B_0.db3
    └── ...

目录结构示例 (单目录 - 平铺模式):
    /dat/rosbag/20251213/1_010/
    ├── 1_010_20251213-111122-A_0.db3
    ├── 1_010_20251213-111122-B_0.db3
    ├── 1_010_20251213-112233-A_0.db3
    ├── 1_010_20251213-112233-B_0.db3
    └── ...

目录结构示例 (单目录 - 子目录内 a/b 文件模式):
    /dat/rosbag/20251216/
    ├── 1_010_20251216-170609/
    │   ├── 1_010_20251216-170609_a.db3
    │   ├── 1_010_20251216-170609_b.db3
    │   └── metadata.yaml
    └── 1_010_20251216-171051/
        ├── 1_010_20251216-171051_a.db3
        ├── 1_010_20251216-171051_b.db3
        └── metadata.yaml

目录结构示例 (双目录):
    /dat/rosbag/20251213/1_010_A/          # A 目录
    ├── 1_010_20251213-111122-A/
    └── 1_010_20251213-112233-A/
    
    /dat/rosbag/20251213/1_010_B/          # B 目录
    ├── 1_010_20251213-111122-B/
    └── 1_010_20251213-112233-B/

输出结构:
    /dat/rosbag/20251213/1_010/merged/
    ├── 1_010_20251213-111122/
    │   ├── 1_010_20251213-111122_0.db3    # 合并后的 db3
    │   └── metadata.yaml                   # 合并后的 metadata
    └── 1_010_20251213-112233/
        ├── 1_010_20251213-112233_0.db3
        └── metadata.yaml

用法:
    # 单目录模式
    python scripts/batch_merge_ab.py /dat/rosbag/20251213/1_010
    
    # 双目录模式
    python scripts/batch_merge_ab.py /dat/rosbag/1_010_A /dat/rosbag/1_010_B
    
    # 其他选项
    python scripts/batch_merge_ab.py /path/to/dir -j 8 --dry-run
"""

import argparse
import os
import re
import sys
import time
import yaml
import sqlite3
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import List, Dict, Tuple, Optional
import multiprocessing


@dataclass
class Db3ValidationResult:
    """db3 文件校验结果"""
    valid: bool
    error: str = ""
    file_size: int = 0


def validate_db3_file(db_path: str) -> Db3ValidationResult:
    """
    校验 db3 文件是否有效
    
    检查项:
      1. 文件名不以 ._ 开头（macOS 元数据文件）
      2. 文件存在且不为空
      3. 是有效的 SQLite 数据库
      4. 包含必需的 topics 表
    
    Returns:
        Db3ValidationResult: 校验结果
    """
    path = Path(db_path)
    
    # 1. 检查是否为 macOS 隐藏文件
    if path.name.startswith('._'):
        return Db3ValidationResult(False, "macOS 元数据文件 (._xxx)")
    
    # 2. 检查文件是否存在
    if not path.exists():
        return Db3ValidationResult(False, "文件不存在")
    
    # 3. 检查文件大小
    file_size = path.stat().st_size
    if file_size == 0:
        return Db3ValidationResult(False, "文件为空 (0 字节)")
    
    # 4. 检查 SQLite 文件头 (前16字节应该是 "SQLite format 3\0")
    try:
        with open(db_path, 'rb') as f:
            header = f.read(16)
            if not header.startswith(b'SQLite format 3'):
                return Db3ValidationResult(False, "不是有效的 SQLite 数据库文件")
    except IOError as e:
        return Db3ValidationResult(False, f"无法读取文件: {e}")
    
    # 5. 尝试打开数据库并检查必需的表
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 检查 topics 表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='topics'")
        if not cursor.fetchone():
            conn.close()
            return Db3ValidationResult(False, "缺少 topics 表")
        
        # 检查 messages 表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages'")
        if not cursor.fetchone():
            conn.close()
            return Db3ValidationResult(False, "缺少 messages 表")
        
        # 快速检查 topics 表是否可读
        cursor.execute("SELECT COUNT(*) FROM topics")
        topic_count = cursor.fetchone()[0]
        if topic_count == 0:
            conn.close()
            return Db3ValidationResult(False, "topics 表为空")
        
        conn.close()
        return Db3ValidationResult(True, "", file_size)
        
    except sqlite3.DatabaseError as e:
        return Db3ValidationResult(False, f"数据库错误: {e}")
    except Exception as e:
        return Db3ValidationResult(False, f"未知错误: {e}")


def validate_db3_files(files: List[Path]) -> Tuple[List[Path], List[Tuple[Path, str]]]:
    """
    批量校验 db3 文件列表
    
    Args:
        files: 要校验的文件列表
    
    Returns:
        (valid_files, invalid_files): 有效文件列表和无效文件列表（带错误信息）
    """
    valid_files = []
    invalid_files = []
    
    for f in files:
        result = validate_db3_file(str(f))
        if result.valid:
            valid_files.append(f)
        else:
            invalid_files.append((f, result.error))
    
    return valid_files, invalid_files


def get_db3_info(db_path: str) -> Dict:
    """获取 db3 文件信息"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, type, serialization_format FROM topics")
    topics = {row[0]: {'name': row[1], 'type': row[2], 'format': row[3]} for row in cursor.fetchall()}
    
    cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM messages")
    row = cursor.fetchone()
    msg_count, min_ts, max_ts = row
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='message_definitions'")
    has_msg_defs = cursor.fetchone() is not None
    
    message_definitions = {}
    if has_msg_defs:
        cursor.execute("SELECT topic_type, encoding, encoded_message_definition FROM message_definitions")
        for row in cursor.fetchall():
            message_definitions[row[0]] = {'encoding': row[1], 'definition': row[2]}
    
    conn.close()
    
    return {
        'topics': topics,
        'message_count': msg_count or 0,
        'min_timestamp': min_ts,
        'max_timestamp': max_ts,
        'has_message_definitions': has_msg_defs,
        'message_definitions': message_definitions
    }


def merge_db3_inline(input_files: List[str], output_file: str) -> Tuple[int, float]:
    """
    内联版本的 db3 合并函数（高性能）
    
    Returns:
        (total_inserted, elapsed_seconds)
    """
    start_time = time.time()
    
    # 收集文件信息
    file_infos = []
    all_topics = {}
    all_message_definitions = {}
    
    for f in input_files:
        if not os.path.exists(f):
            raise FileNotFoundError(f"文件不存在: {f}")
        
        info = get_db3_info(f)
        file_infos.append({'path': os.path.abspath(f), 'info': info})
        
        for topic_id, topic_data in info['topics'].items():
            topic_name = topic_data['name']
            if topic_name not in all_topics:
                all_topics[topic_name] = {
                    'type': topic_data['type'],
                    'format': topic_data['format']
                }
        
        for topic_type, msg_def in info.get('message_definitions', {}).items():
            if topic_type not in all_message_definitions:
                all_message_definitions[topic_type] = msg_def
    
    # 按开始时间排序
    file_infos.sort(key=lambda x: x['info']['min_timestamp'] or 0)
    
    # 删除已存在的输出文件
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # 创建输出数据库
    out_conn = sqlite3.connect(output_file)
    out_cursor = out_conn.cursor()
    
    # 性能优化设置
    out_cursor.execute("PRAGMA journal_mode = WAL")
    out_cursor.execute("PRAGMA synchronous = OFF")
    out_cursor.execute("PRAGMA cache_size = -512000")  # 512MB cache (增大)
    out_cursor.execute("PRAGMA temp_store = MEMORY")
    out_cursor.execute("PRAGMA mmap_size = 1073741824")  # 1GB mmap
    out_cursor.execute("PRAGMA page_size = 65536")  # 64KB pages
    
    # 创建表结构
    out_cursor.execute("""
        CREATE TABLE schema (
            schema_version INTEGER PRIMARY KEY,
            ros_distro TEXT NOT NULL
        )
    """)
    out_cursor.execute("INSERT INTO schema VALUES (3, 'humble')")
    
    out_cursor.execute("""
        CREATE TABLE topics (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            type TEXT NOT NULL,
            serialization_format TEXT NOT NULL,
            offered_qos_profiles TEXT NOT NULL DEFAULT ''
        )
    """)
    
    out_cursor.execute("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            topic_id INTEGER NOT NULL,
            timestamp INTEGER NOT NULL,
            data BLOB NOT NULL
        )
    """)
    
    if all_message_definitions:
        out_cursor.execute("""
            CREATE TABLE message_definitions (
                id INTEGER PRIMARY KEY,
                topic_type TEXT NOT NULL,
                encoding TEXT NOT NULL,
                encoded_message_definition TEXT NOT NULL
            )
        """)
        
        msg_def_id = 1
        for topic_type, msg_def in sorted(all_message_definitions.items()):
            out_cursor.execute(
                "INSERT INTO message_definitions (id, topic_type, encoding, encoded_message_definition) VALUES (?, ?, ?, ?)",
                (msg_def_id, topic_type, msg_def['encoding'], msg_def['definition'])
            )
            msg_def_id += 1
    
    # 构建 topic_name -> new_id 映射
    topic_name_to_new_id = {}
    new_id = 1
    for topic_name, topic_info in sorted(all_topics.items()):
        out_cursor.execute(
            "INSERT INTO topics (id, name, type, serialization_format) VALUES (?, ?, ?, ?)",
            (new_id, topic_name, topic_info['type'], topic_info['format'])
        )
        topic_name_to_new_id[topic_name] = new_id
        new_id += 1
    
    out_conn.commit()
    
    # 使用 ATTACH + INSERT SELECT 直接复制数据
    total_inserted = 0
    
    for file_info in file_infos:
        src_path = file_info['path']
        src_info = file_info['info']
        
        out_cursor.execute("ATTACH DATABASE ? AS src", (src_path,))
        
        out_cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS topic_map (
                old_id INTEGER PRIMARY KEY,
                new_id INTEGER
            )
        """)
        out_cursor.execute("DELETE FROM topic_map")
        
        for old_id, topic_data in src_info['topics'].items():
            topic_name = topic_data['name']
            new_id = topic_name_to_new_id.get(topic_name)
            if new_id:
                out_cursor.execute(
                    "INSERT INTO topic_map (old_id, new_id) VALUES (?, ?)",
                    (old_id, new_id)
                )
        
        out_cursor.execute("""
            INSERT INTO messages (topic_id, timestamp, data)
            SELECT tm.new_id, m.timestamp, m.data
            FROM src.messages m
            JOIN topic_map tm ON m.topic_id = tm.old_id
        """)
        
        total_inserted += out_cursor.rowcount
        out_conn.commit()
        out_cursor.execute("DETACH DATABASE src")
    
    # 创建索引
    out_cursor.execute("CREATE INDEX timestamp_idx ON messages (timestamp)")
    out_cursor.execute("CREATE INDEX topic_id_idx ON messages (topic_id)")
    
    out_cursor.execute("PRAGMA journal_mode = DELETE")
    out_cursor.execute("PRAGMA synchronous = FULL")
    
    out_conn.commit()
    out_conn.close()
    
    elapsed = time.time() - start_time
    return total_inserted, elapsed


@dataclass
class MergeTask:
    """合并任务"""
    base_name: str
    a_files: List[Path]
    b_files: List[Path]
    output_dir: Path  # 输出目录 (如 merged/1_010_20251213-111122/)
    output_file: Path  # 输出 db3 文件 (如 merged/1_010_20251213-111122/1_010_20251213-111122_0.db3)
    a_metadata: Optional[Path] = None  # A 的 metadata.yaml
    b_metadata: Optional[Path] = None  # B 的 metadata.yaml
    output_metadata: Optional[Path] = None  # 输出的 metadata.yaml


@dataclass
class MergeResult:
    """合并结果"""
    base_name: str
    success: bool
    message: str
    size_mb: float = 0.0
    elapsed: float = 0.0


def scan_directory(input_path: Path, pairs: Dict, target_suffix: str = None):
    """
    扫描单个目录，查找 A/B 子目录
    
    Args:
        input_path: 要扫描的目录
        pairs: 配对字典，格式: {base_name: {'A': {'files': [], 'metadata': None}, 'B': {...}}}
        target_suffix: 如果指定，只查找该后缀 ('A' 或 'B')
    """
    pattern = re.compile(r'^(.+)-([AB])$')
    
    for subdir in sorted(input_path.iterdir()):
        if not subdir.is_dir():
            continue
        
        match = pattern.match(subdir.name)
        if not match:
            continue
        
        base_name = match.group(1)
        suffix = match.group(2)
        
        # 如果指定了目标后缀，只处理匹配的
        if target_suffix and suffix != target_suffix:
            continue
        
        # 过滤掉 macOS 隐藏文件 (._xxx.db3)
        db3_files = sorted([f for f in subdir.glob('*.db3') if not f.name.startswith('._')])
        if db3_files:
            pairs[base_name][suffix]['files'].extend(db3_files)
            # 查找 metadata.yaml
            metadata_file = subdir / 'metadata.yaml'
            if metadata_file.exists():
                pairs[base_name][suffix]['metadata'] = metadata_file


def scan_flat_files(input_path: Path, pairs: Dict, target_suffix: str = None):
    """
    扫描目录下直接存在的 A/B db3 文件（平铺模式）
    
    支持文件名格式: {base_name}-A_0.db3, {base_name}-B_0.db3
    
    Args:
        input_path: 要扫描的目录
        pairs: 配对字典，格式: {base_name: {'A': {'files': [], 'metadata': None}, 'B': {...}}}
        target_suffix: 如果指定，只查找该后缀 ('A' 或 'B')
    """
    # 匹配格式: xxx-A_数字.db3 或 xxx-B_数字.db3
    pattern = re.compile(r'^(.+)-([AB])_\d+\.db3$')
    
    for file_path in sorted(input_path.iterdir()):
        if not file_path.is_file():
            continue
        
        # 过滤掉 macOS 隐藏文件
        if file_path.name.startswith('._'):
            continue
        
        match = pattern.match(file_path.name)
        if not match:
            continue
        
        base_name = match.group(1)
        suffix = match.group(2)
        
        # 如果指定了目标后缀，只处理匹配的
        if target_suffix and suffix != target_suffix:
            continue
        
        pairs[base_name][suffix]['files'].append(file_path)
        # 平铺模式下暂不支持 metadata（文件直接在目录下，没有对应的子目录）


def scan_subdir_ab_files(input_path: Path, pairs: Dict, target_suffix: str = None):
    """
    扫描子目录内的 a/b db3 文件模式
    
    支持目录结构:
        1_010_20251216-170609/
        ├── 1_010_20251216-170609_a.db3
        ├── 1_010_20251216-170609_b.db3
        └── metadata.yaml
    
    Args:
        input_path: 要扫描的目录
        pairs: 配对字典
        target_suffix: 如果指定，只查找该后缀 ('A' 或 'B')
    """
    # 匹配文件名格式: xxx_a.db3 或 xxx_b.db3（不区分大小写）
    pattern = re.compile(r'^(.+)_([aAbB])\.db3$')
    
    for subdir in sorted(input_path.iterdir()):
        if not subdir.is_dir():
            continue
        
        # 使用子目录名作为 base_name
        base_name = subdir.name
        
        # 查找该子目录下的 a/b db3 文件（过滤 macOS 隐藏文件）
        a_files = []
        b_files = []
        
        for file_path in sorted(subdir.glob('*.db3')):
            # 过滤掉 macOS 隐藏文件
            if file_path.name.startswith('._'):
                continue
            match = pattern.match(file_path.name)
            if match:
                suffix = match.group(2).upper()  # 统一转为大写
                if suffix == 'A':
                    a_files.append(file_path)
                elif suffix == 'B':
                    b_files.append(file_path)
        
        # 如果找到了 a/b 文件对
        if a_files or b_files:
            if not target_suffix or target_suffix == 'A':
                pairs[base_name]['A']['files'].extend(a_files)
            if not target_suffix or target_suffix == 'B':
                pairs[base_name]['B']['files'].extend(b_files)
            
            # metadata.yaml 是共享的，A 和 B 都引用同一个
            metadata_file = subdir / 'metadata.yaml'
            if metadata_file.exists():
                if not target_suffix or target_suffix == 'A':
                    pairs[base_name]['A']['metadata'] = metadata_file
                if not target_suffix or target_suffix == 'B':
                    pairs[base_name]['B']['metadata'] = metadata_file


def find_ab_pairs(input_dirs: List[str]) -> Dict[str, Dict[str, Dict]]:
    """
    扫描目录，找到所有 A/B 配对
    
    支持:
      - 单目录: 在同一目录下查找 -A 和 -B 子目录，以及直接的 A/B db3 文件
      - 双目录: 第一个目录找 -A，第二个目录找 -B
    
    返回格式: {base_name: {'A': {'files': [Path], 'metadata': Path|None}, 'B': {...}}}
    """
    pairs = defaultdict(lambda: {
        'A': {'files': [], 'metadata': None},
        'B': {'files': [], 'metadata': None}
    })
    
    if len(input_dirs) == 1:
        # 单目录模式：同时查找 A 和 B
        # 1. 扫描 -A/-B 子目录模式
        scan_directory(Path(input_dirs[0]), pairs)
        # 2. 扫描平铺文件模式 (xxx-A_0.db3)
        scan_flat_files(Path(input_dirs[0]), pairs)
        # 3. 扫描子目录内 a/b 文件模式 (subdir/xxx_a.db3)
        scan_subdir_ab_files(Path(input_dirs[0]), pairs)
    elif len(input_dirs) == 2:
        # 双目录模式：分别查找
        dir_a = Path(input_dirs[0])
        dir_b = Path(input_dirs[1])
        
        # 自动检测哪个目录是 A，哪个是 B
        # 先扫描看看各自包含什么（子目录和平铺文件）
        test_pairs_0 = defaultdict(lambda: {
            'A': {'files': [], 'metadata': None},
            'B': {'files': [], 'metadata': None}
        })
        test_pairs_1 = defaultdict(lambda: {
            'A': {'files': [], 'metadata': None},
            'B': {'files': [], 'metadata': None}
        })
        scan_directory(dir_a, test_pairs_0)
        scan_flat_files(dir_a, test_pairs_0)
        scan_subdir_ab_files(dir_a, test_pairs_0)
        scan_directory(dir_b, test_pairs_1)
        scan_flat_files(dir_b, test_pairs_1)
        scan_subdir_ab_files(dir_b, test_pairs_1)
        
        # 统计各目录的 A/B 数量
        count_0_a = sum(1 for p in test_pairs_0.values() if p['A']['files'])
        count_0_b = sum(1 for p in test_pairs_0.values() if p['B']['files'])
        count_1_a = sum(1 for p in test_pairs_1.values() if p['A']['files'])
        count_1_b = sum(1 for p in test_pairs_1.values() if p['B']['files'])
        
        # 判断目录类型
        if count_0_a > count_0_b and count_1_b > count_1_a:
            # 目录0主要是A，目录1主要是B
            scan_directory(dir_a, pairs, 'A')
            scan_flat_files(dir_a, pairs, 'A')
            scan_subdir_ab_files(dir_a, pairs, 'A')
            scan_directory(dir_b, pairs, 'B')
            scan_flat_files(dir_b, pairs, 'B')
            scan_subdir_ab_files(dir_b, pairs, 'B')
        elif count_0_b > count_0_a and count_1_a > count_1_b:
            # 目录0主要是B，目录1主要是A
            scan_directory(dir_a, pairs, 'B')
            scan_flat_files(dir_a, pairs, 'B')
            scan_subdir_ab_files(dir_a, pairs, 'B')
            scan_directory(dir_b, pairs, 'A')
            scan_flat_files(dir_b, pairs, 'A')
            scan_subdir_ab_files(dir_b, pairs, 'A')
        else:
            # 无法确定，两个目录都完整扫描
            scan_directory(dir_a, pairs)
            scan_flat_files(dir_a, pairs)
            scan_subdir_ab_files(dir_a, pairs)
            scan_directory(dir_b, pairs)
            scan_flat_files(dir_b, pairs)
            scan_subdir_ab_files(dir_b, pairs)
    
    return pairs


def merge_metadata(a_metadata: Optional[Path], b_metadata: Optional[Path], 
                   output_metadata: Path, output_db3_name: str) -> bool:
    """
    合并两个 metadata.yaml 文件
    
    Args:
        a_metadata: A 的 metadata.yaml 路径
        b_metadata: B 的 metadata.yaml 路径
        output_metadata: 输出的 metadata.yaml 路径
        output_db3_name: 输出的 db3 文件名（不含路径）
    
    Returns:
        是否成功
    """
    # 如果两个都没有，跳过
    if not a_metadata and not b_metadata:
        return False
    
    try:
        # 读取可用的 metadata
        meta_a = None
        meta_b = None
        
        if a_metadata and a_metadata.exists():
            with open(a_metadata, 'r') as f:
                meta_a = yaml.safe_load(f)
        
        if b_metadata and b_metadata.exists():
            with open(b_metadata, 'r') as f:
                meta_b = yaml.safe_load(f)
        
        # 如果只有一个，直接使用它作为基础
        if meta_a and not meta_b:
            base_meta = meta_a
        elif meta_b and not meta_a:
            base_meta = meta_b
        else:
            # 两个都有，进行合并
            base_meta = meta_a
            
            info_a = meta_a.get('rosbag2_bagfile_information', {})
            info_b = meta_b.get('rosbag2_bagfile_information', {})
            
            # 合并 duration（取较大值，因为时间可能有重叠）
            duration_a = info_a.get('duration', {}).get('nanoseconds', 0)
            duration_b = info_b.get('duration', {}).get('nanoseconds', 0)
            
            # 合并 starting_time（取较小值）
            start_a = info_a.get('starting_time', {}).get('nanoseconds_since_epoch', float('inf'))
            start_b = info_b.get('starting_time', {}).get('nanoseconds_since_epoch', float('inf'))
            
            # 合并 message_count
            msg_count_a = info_a.get('message_count', 0)
            msg_count_b = info_b.get('message_count', 0)
            
            # 合并 topics_with_message_count
            topics_dict = {}
            for topic in info_a.get('topics_with_message_count', []):
                name = topic.get('topic_metadata', {}).get('name', '')
                if name:
                    topics_dict[name] = topic.copy()
            
            for topic in info_b.get('topics_with_message_count', []):
                name = topic.get('topic_metadata', {}).get('name', '')
                if name:
                    if name in topics_dict:
                        # 累加消息数
                        topics_dict[name]['message_count'] = (
                            topics_dict[name].get('message_count', 0) + 
                            topic.get('message_count', 0)
                        )
                    else:
                        topics_dict[name] = topic.copy()
            
            # 更新 base_meta
            merged_info = base_meta.get('rosbag2_bagfile_information', {})
            merged_info['duration'] = {'nanoseconds': max(duration_a, duration_b)}
            merged_info['starting_time'] = {'nanoseconds_since_epoch': min(start_a, start_b)}
            merged_info['message_count'] = msg_count_a + msg_count_b
            merged_info['topics_with_message_count'] = list(topics_dict.values())
        
        # 更新文件路径
        merged_info = base_meta.get('rosbag2_bagfile_information', {})
        merged_info['relative_file_paths'] = [output_db3_name]
        
        # 写入输出
        with open(output_metadata, 'w') as f:
            yaml.dump(base_meta, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
        
        return True
        
    except Exception as e:
        # 合并失败时不影响主流程
        return False


def merge_worker_process(args: Tuple) -> MergeResult:
    """
    工作进程：执行单个合并任务（多进程版本）
    
    注意：参数需要是可序列化的，所以使用 tuple 传递
    """
    (base_name, a_files, b_files, output_dir, output_file, 
     a_metadata, b_metadata, output_metadata, dry_run) = args
    
    # 转换回 Path 对象
    a_files = [Path(f) for f in a_files]
    b_files = [Path(f) for f in b_files]
    output_dir = Path(output_dir)
    output_file = Path(output_file)
    a_metadata = Path(a_metadata) if a_metadata else None
    b_metadata = Path(b_metadata) if b_metadata else None
    output_metadata = Path(output_metadata) if output_metadata else None
    
    all_files = sorted(a_files) + sorted(b_files)
    
    if not all_files:
        return MergeResult(base_name, False, "无输入文件")
    
    if dry_run:
        return MergeResult(base_name, True, "DRY-RUN")
    
    # 再次校验文件（防止扫描后文件状态变化）
    for f in all_files:
        result = validate_db3_file(str(f))
        if not result.valid:
            return MergeResult(base_name, False, f"{f.name}: {result.error}")
    
    try:
        # 创建输出目录
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 直接调用内联合并函数（避免子进程开销）
        input_files = [str(f) for f in all_files]
        total_msgs, elapsed = merge_db3_inline(input_files, str(output_file))
        
        # 合并 metadata.yaml
        if output_metadata:
            merge_metadata(
                a_metadata, 
                b_metadata, 
                output_metadata,
                output_file.name
            )
        
        size_mb = output_file.stat().st_size / (1024 * 1024) if output_file.exists() else 0
        return MergeResult(base_name, True, "OK", size_mb, elapsed)
        
    except Exception as e:
        return MergeResult(base_name, False, str(e)[:200])


def main():
    parser = argparse.ArgumentParser(
        description="批量合并 A/B 后缀的 db3 文件（多线程）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('input_dirs', nargs='+', 
                        help='输入目录 (1个=单目录模式, 2个=双目录模式)')
    parser.add_argument('-o', '--output', help='输出目录，默认为第一个input_dir/merged')
    parser.add_argument('-j', '--jobs', type=int, default=None, 
                        help='并行进程数，默认为 CPU 核心数')
    parser.add_argument('--dry-run', action='store_true', help='仅显示将要执行的操作')
    parser.add_argument('--skip-existing', action='store_true', help='跳过已存在的输出文件')
    
    args = parser.parse_args()
    
    # 验证输入目录
    input_dirs = args.input_dirs
    if len(input_dirs) > 2:
        print(f"错误: 最多支持 2 个输入目录，收到 {len(input_dirs)} 个")
        sys.exit(1)
    
    for d in input_dirs:
        if not Path(d).exists():
            print(f"错误: 目录不存在: {d}")
            sys.exit(1)
    
    # 输出目录
    first_dir = Path(input_dirs[0])
    output_dir = Path(args.output) if args.output else first_dir / 'merged'
    
    # 确定并行度
    num_jobs = args.jobs or multiprocessing.cpu_count()
    num_jobs = min(num_jobs, multiprocessing.cpu_count())
    
    # 扫描 A/B 配对
    if len(input_dirs) == 1:
        print(f"单目录模式: {input_dirs[0]}")
    else:
        print(f"双目录模式:")
        print(f"  目录1: {input_dirs[0]}")
        print(f"  目录2: {input_dirs[1]}")
    
    pairs = find_ab_pairs(input_dirs)
    
    if not pairs:
        print("未找到 A/B 配对目录")
        sys.exit(0)
    
    # 统计配对
    complete_pairs = []
    a_only = []
    b_only = []
    
    for base_name, files in pairs.items():
        has_a = bool(files['A']['files'])
        has_b = bool(files['B']['files'])
        
        if has_a and has_b:
            complete_pairs.append(base_name)
        elif has_a:
            a_only.append(base_name)
        elif has_b:
            b_only.append(base_name)
    
    print(f"\n发现配对:")
    print(f"  完整配对 (A+B): {len(complete_pairs)}")
    print(f"  仅有 A: {len(a_only)}")
    print(f"  仅有 B: {len(b_only)}")
    
    if not complete_pairs:
        print("\n没有完整的 A/B 配对可合并")
        sys.exit(0)
    
    # 创建任务列表（含文件校验）
    tasks = []
    skipped_count = 0
    validation_errors = []  # 收集校验失败的信息
    
    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n正在校验 db3 文件...")
    
    for base_name in sorted(complete_pairs):
        files = pairs[base_name]
        # 输出目录结构: merged/{base_name}/{base_name}_0.db3
        task_output_dir = output_dir / base_name
        output_file = task_output_dir / f"{base_name}_0.db3"
        output_metadata = task_output_dir / "metadata.yaml"
        
        if args.skip_existing and output_file.exists():
            skipped_count += 1
            continue
        
        # 校验 A 文件
        valid_a_files, invalid_a_files = validate_db3_files(files['A']['files'])
        # 校验 B 文件
        valid_b_files, invalid_b_files = validate_db3_files(files['B']['files'])
        
        # 收集校验错误
        for f, err in invalid_a_files + invalid_b_files:
            validation_errors.append((base_name, f.name, err))
        
        # 如果没有有效文件，跳过此任务
        if not valid_a_files and not valid_b_files:
            continue
        
        tasks.append(MergeTask(
            base_name=base_name,
            a_files=valid_a_files,
            b_files=valid_b_files,
            output_dir=task_output_dir,
            output_file=output_file,
            a_metadata=files['A']['metadata'],
            b_metadata=files['B']['metadata'],
            output_metadata=output_metadata
        ))
    
    # 显示校验错误
    if validation_errors:
        print(f"\n⚠ 发现 {len(validation_errors)} 个无效文件:")
        for base_name, filename, err in validation_errors:
            print(f"  - [{base_name}] {filename}: {err}")
    
    if skipped_count > 0:
        print(f"\n跳过已存在: {skipped_count}")
    
    if not tasks:
        print("\n没有需要处理的任务")
        sys.exit(0)
    
    # 显示任务信息
    total_files = sum(len(t.a_files) + len(t.b_files) for t in tasks)
    print(f"\n待处理: {len(tasks)} 个配对, 共 {total_files} 个文件")
    print(f"并行进程: {num_jobs} (CPU 核心数: {multiprocessing.cpu_count()})")
    print(f"输出目录: {output_dir}")
    
    if a_only:
        print(f"\n仅有 A (跳过): {len(a_only)} 个")
    if b_only:
        print(f"\n仅有 B (跳过): {len(b_only)} 个")
    
    # 准备任务参数（需要可序列化）
    task_args = []
    for t in tasks:
        task_args.append((
            t.base_name,
            [str(f) for f in t.a_files],
            [str(f) for f in t.b_files],
            str(t.output_dir),
            str(t.output_file),
            str(t.a_metadata) if t.a_metadata else None,
            str(t.b_metadata) if t.b_metadata else None,
            str(t.output_metadata) if t.output_metadata else None,
            args.dry_run
        ))
    
    # 执行并行合并（使用多进程）
    print(f"\n{'='*60}")
    print("开始合并（多进程模式）...")
    start_time = time.time()
    
    results: List[MergeResult] = []
    completed = 0
    
    with ProcessPoolExecutor(max_workers=num_jobs) as executor:
        # 提交所有任务
        future_to_name = {
            executor.submit(merge_worker_process, arg): arg[0]
            for arg in task_args
        }
        
        # 收集结果并实时显示进度
        for future in as_completed(future_to_name):
            base_name = future_to_name[future]
            completed += 1
            try:
                result = future.result()
                results.append(result)
                
                # 实时输出
                status = "✓" if result.success else "✗"
                prog = f"[{completed}/{len(tasks)}]"
                if result.success and result.size_mb > 0:
                    print(f"{prog} {status} {result.base_name} ({result.size_mb:.1f}MB, {result.elapsed:.1f}s)")
                elif result.success:
                    print(f"{prog} {status} {result.base_name}")
                else:
                    print(f"{prog} {status} {result.base_name}: {result.message}")
                    
            except Exception as e:
                print(f"[!] {base_name}: 异常 - {e}")
    
    elapsed = time.time() - start_time
    
    # 统计结果
    success_count = sum(1 for r in results if r.success)
    failed_count = sum(1 for r in results if not r.success)
    total_size = sum(r.size_mb for r in results if r.success)
    
    # 总结
    print(f"\n{'='*60}")
    print(f"合并完成!")
    print(f"  成功: {success_count}")
    print(f"  失败: {failed_count}")
    print(f"  跳过: {skipped_count}")
    print(f"  总大小: {total_size:.1f} MB")
    print(f"  总耗时: {elapsed:.1f}s")
    if success_count > 0:
        print(f"  平均速度: {total_size/elapsed:.1f} MB/s")
    
    # 显示失败详情
    failed_results = [r for r in results if not r.success]
    if failed_results:
        print(f"\n失败详情:")
        for r in failed_results:
            print(f"  - {r.base_name}: {r.message}")
    
    if failed_count > 0:
        sys.exit(1)


if __name__ == '__main__':
    main()
