#!/usr/bin/env python3
"""
Proto 编译脚本 - 递归编译指定目录下所有 .proto 文件

用法:
    python compile_proto.py src/proto/modules              # 编译 modules 目录
    python compile_proto.py src/proto/modules -v          # 详细模式
    python compile_proto.py src/proto/modules -I src/proto # 指定 import 搜索路径
"""
import os
import sys
import subprocess
import argparse


def remove_existing_py(root: str):
    """删除目录下所有 _pb2.py 文件"""
    deleted = 0
    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if f.endswith("_pb2.py"):
                os.remove(os.path.join(dirpath, f))
                deleted += 1
    print(f"[INFO] 已删除 {deleted} 个 _pb2.py 文件")


def compile_proto(proto_root: str, include_path: str = None, verbose: bool = False):
    """
    递归编译 proto_root 下所有 .proto 文件
    
    Args:
        proto_root: proto 文件所在目录
        include_path: import 搜索路径（-I），也是输出路径
        verbose: 是否显示详细命令
    """
    proto_root = os.path.abspath(proto_root)
    include_path = os.path.abspath(include_path) if include_path else proto_root
    
    # 收集所有 proto 文件
    proto_files = []
    for dirpath, _, filenames in os.walk(proto_root):
        for f in filenames:
            if f.endswith(".proto"):
                proto_files.append(os.path.join(dirpath, f))

    if not proto_files:
        print(f"[WARNING] 在 {proto_root} 下未找到任何 .proto 文件")
        return

    print(f"[INFO] 找到 {len(proto_files)} 个 proto 文件")
    print(f"[INFO] import 搜索路径 (-I): {include_path}")
    print(f"[INFO] 输出路径 (--python_out): {include_path}")
    print()

    success, fail = 0, 0
    for proto_file in proto_files:
        relative_path = os.path.relpath(proto_file, include_path)

        cmd = [
            "protoc",
            f"-I={include_path}",        # import 搜索路径
            f"--python_out={include_path}",  # 输出到 include_path
            proto_file
        ]

        try:
            if verbose:
                print(f"[CMD] {' '.join(cmd)}")
            result = subprocess.run(cmd, check=True, capture_output=True)
            print(f"[OK] {relative_path}")
            success += 1
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] {relative_path}")
            if e.stderr:
                # 只显示前几行错误
                errors = e.stderr.decode().strip().split('\n')
                for line in errors[:3]:
                    print(f"        {line}")
                if len(errors) > 3:
                    print(f"        ... ({len(errors)-3} more errors)")
            fail += 1

    print(f"\n[INFO] 编译完成: 成功 {success} 个，失败 {fail} 个")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="递归编译 proto 文件到 *_pb2.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 编译 modules 目录（自动推断 -I 为父目录）
  python compile_proto.py src/proto/modules
  
  # 显式指定 import 搜索路径
  python compile_proto.py src/proto/modules -I src/proto
  
  # 强制重新编译
  python compile_proto.py src/proto/modules -f
        """
    )
    parser.add_argument("proto_root", nargs="?", default=".",
                        help="proto 文件所在目录")
    parser.add_argument("-I", "--include-path", dest="include_path",
                        default=None,
                        help="import 搜索路径（默认：自动推断为 proto_root 的父目录）")
    parser.add_argument("--force-compile", "-f", action="store_true",
                        help="强制重新编译（先删除所有 _pb2.py）")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="显示详细编译命令")
    args = parser.parse_args()

    if not os.path.exists(args.proto_root):
        print(f"[ERROR] 目录不存在: {args.proto_root}")
        sys.exit(1)

    # 自动推断 include_path：如果 proto_root 是 xxx/modules，则 -I 为 xxx
    include_path = args.include_path
    if include_path is None:
        proto_root_abs = os.path.abspath(args.proto_root)
        # 如果目录名是 modules，使用其父目录作为 include_path
        if os.path.basename(proto_root_abs) == "modules":
            include_path = os.path.dirname(proto_root_abs)
            print(f"[INFO] 自动推断 -I 路径: {include_path}")
        else:
            include_path = proto_root_abs

    if args.force_compile:
        remove_existing_py(include_path)

    compile_proto(args.proto_root, include_path, verbose=args.verbose)
