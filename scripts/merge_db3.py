#!/usr/bin/env python3
"""
合并多个 ROS2 bag db3 文件（高性能版本）

用法:
    python scripts/merge_db3.py input1.db3 input2.db3 -o merged.db3
    python scripts/merge_db3.py *.db3 -o merged.db3
    
性能优化:
    - 使用 ATTACH DATABASE + INSERT SELECT 直接在 SQLite 层面复制数据
    - 避免 Python 层面的数据传输
    - 使用 WAL 模式和禁用同步提高写入速度
    - 单事务批量操作
"""
import argparse
import sqlite3
import os
import sys
from pathlib import Path
from typing import List, Dict
import time


def get_db3_info(db_path: str) -> Dict:
    """获取 db3 文件信息"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 获取 topics
    cursor.execute("SELECT id, name, type, serialization_format FROM topics")
    topics = {row[0]: {'name': row[1], 'type': row[2], 'format': row[3]} for row in cursor.fetchall()}
    
    # 获取消息数量和时间范围
    cursor.execute("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM messages")
    row = cursor.fetchone()
    msg_count, min_ts, max_ts = row
    
    # 检查是否有 message_definitions 表
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='message_definitions'")
    has_msg_defs = cursor.fetchone() is not None
    
    # 获取 message_definitions（如果存在）
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


def merge_db3_fast(input_files: List[str], output_file: str, verbose: bool = True):
    """
    高性能合并多个 db3 文件
    
    使用 SQLite ATTACH DATABASE 直接在数据库层面复制数据，
    避免 Python 层面的数据传输开销。
    """
    start_time = time.time()
    
    if verbose:
        print(f"准备合并 {len(input_files)} 个 db3 文件...")
    
    # 收集文件信息并按时间排序
    file_infos = []
    all_topics = {}  # topic_name -> {type, format}
    total_msg_estimate = 0
    
    all_message_definitions = {}  # topic_type -> {encoding, definition}
    
    for f in input_files:
        if not os.path.exists(f):
            print(f"错误: 文件不存在 {f}")
            sys.exit(1)
        
        info = get_db3_info(f)
        file_infos.append({'path': os.path.abspath(f), 'info': info})
        total_msg_estimate += info['message_count']
        
        for topic_id, topic_data in info['topics'].items():
            topic_name = topic_data['name']
            if topic_name not in all_topics:
                all_topics[topic_name] = {
                    'type': topic_data['type'],
                    'format': topic_data['format']
                }
        
        # 收集 message_definitions
        for topic_type, msg_def in info.get('message_definitions', {}).items():
            if topic_type not in all_message_definitions:
                all_message_definitions[topic_type] = msg_def
        
        if verbose:
            ts_start = info['min_timestamp'] / 1e9 if info['min_timestamp'] else 0
            ts_end = info['max_timestamp'] / 1e9 if info['max_timestamp'] else 0
            duration = ts_end - ts_start
            print(f"  [{os.path.basename(f)}] {info['message_count']:,} msgs, {duration:.1f}s")
    
    # 按开始时间排序
    file_infos.sort(key=lambda x: x['info']['min_timestamp'] or 0)
    
    if verbose:
        print(f"\n共 {len(all_topics)} 个 topics, 预计 {total_msg_estimate:,} 条消息")
        print(f"创建输出文件...")
    
    # 删除已存在的输出文件
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # 创建输出数据库
    out_conn = sqlite3.connect(output_file)
    out_cursor = out_conn.cursor()
    
    # 性能优化设置
    out_cursor.execute("PRAGMA journal_mode = WAL")
    out_cursor.execute("PRAGMA synchronous = OFF")
    out_cursor.execute("PRAGMA cache_size = -256000")  # 256MB cache
    out_cursor.execute("PRAGMA temp_store = MEMORY")
    
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
    
    # 只有当原始文件有 message_definitions 表时才创建
    # 注意：rosbags 对表结构非常敏感，如果原文件没有此表，就不要添加
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
        
        if verbose:
            print(f"  已写入 {len(all_message_definitions)} 个消息类型定义")
    
    # 构建 topic_name -> new_id 映射，并插入 topics
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
    
    for idx, file_info in enumerate(file_infos):
        src_path = file_info['path']
        src_info = file_info['info']
        
        if verbose:
            print(f"\n[{idx+1}/{len(file_infos)}] 合并 {os.path.basename(src_path)}...")
        
        # 附加源数据库
        out_cursor.execute(f"ATTACH DATABASE ? AS src", (src_path,))
        
        # 构建源 topic_id -> 新 topic_id 的映射
        # 创建临时映射表
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
        
        # 使用单条 INSERT SELECT 复制所有消息（核心优化！）
        out_cursor.execute("""
            INSERT INTO messages (topic_id, timestamp, data)
            SELECT tm.new_id, m.timestamp, m.data
            FROM src.messages m
            JOIN topic_map tm ON m.topic_id = tm.old_id
        """)
        
        inserted = out_cursor.rowcount
        total_inserted += inserted
        
        if verbose:
            print(f"    已插入 {inserted:,} 条消息")
        
        # 必须先 commit，再 detach
        out_conn.commit()
        out_cursor.execute("DETACH DATABASE src")
    
    # 创建索引（在所有数据插入后创建，更快）
    if verbose:
        print(f"\n创建索引...")
    out_cursor.execute("CREATE INDEX timestamp_idx ON messages (timestamp)")
    out_cursor.execute("CREATE INDEX topic_id_idx ON messages (topic_id)")
    
    # 切换回正常模式
    out_cursor.execute("PRAGMA journal_mode = DELETE")
    out_cursor.execute("PRAGMA synchronous = FULL")
    
    out_conn.commit()
    out_conn.close()
    
    elapsed = time.time() - start_time
    
    if verbose:
        size_mb = os.path.getsize(output_file) / (1024 * 1024)
        speed = total_inserted / elapsed if elapsed > 0 else 0
        print(f"\n{'='*50}")
        print(f"合并完成!")
        print(f"  输出文件: {output_file}")
        print(f"  总消息数: {total_inserted:,}")
        print(f"  文件大小: {size_mb:.2f} MB")
        print(f"  耗时: {elapsed:.2f}s")
        print(f"  速度: {speed:,.0f} msg/s")


def main():
    parser = argparse.ArgumentParser(
        description="高性能合并多个 ROS2 bag db3 文件",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
    python scripts/merge_db3.py file1.db3 file2.db3 -o merged.db3
    python scripts/merge_db3.py /path/to/*.db3 -o merged.db3
    python scripts/merge_db3.py file1.db3 file2.db3 -o merged.db3 -q
        """
    )
    
    parser.add_argument('input_files', nargs='+', help='输入的 db3 文件列表')
    parser.add_argument('-o', '--output', required=True, help='输出的 db3 文件路径')
    parser.add_argument('-q', '--quiet', action='store_true', help='静默模式')
    
    args = parser.parse_args()
    
    if len(args.input_files) < 2:
        print("错误: 至少需要 2 个输入文件")
        sys.exit(1)
    
    merge_db3_fast(args.input_files, args.output, verbose=not args.quiet)


if __name__ == '__main__':
    main()
