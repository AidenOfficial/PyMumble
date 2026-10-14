"""Regenerate src/pymumble_rev/_proto from proto/*.proto.

用法:uv run --group proto python scripts/gen_proto.py

protoc 版本由 grpcio-tools 决定(见 pyproject 的 proto 依赖组)。生成代码对 protobuf
运行时有最低版本要求,升级 grpcio-tools 时要同步调整 pyproject 里 protobuf 的下限。
"""

from __future__ import annotations

import pathlib
import re
import shutil
import sys
import tempfile

from grpc_tools import protoc  # type: ignore[import-untyped]

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTO_DIR = ROOT / "proto"
SRC_DIR = ROOT / "src"
PKG_PATH = pathlib.Path("pymumble_rev") / "_proto"
FILES = ["Mumble.proto", "MumbleUDP.proto"]


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        staged = pathlib.Path(tmp) / PKG_PATH
        staged.mkdir(parents=True)
        for name in FILES:
            text = (PROTO_DIR / name).read_text()
            # 改 protobuf 包名,并让文件以 pymumble_rev/_proto/ 路径注册:
            # 生成代码会往进程级的默认 descriptor pool 注册 "Mumble.proto" / "MumbleProto.*",
            # 和 pymumble 等同样内置这两个文件的库装在同一环境时会撞名。包名不影响线上格式。
            text, n = re.subn(
                r"^package (\w+);", r"package pymumble_rev.\1;", text, flags=re.MULTILINE
            )
            if n != 1:
                print(f"{name}: expected exactly one package statement", file=sys.stderr)
                return 1
            (staged / name).write_text(text)

        out = SRC_DIR / PKG_PATH
        for old in out.glob("*_pb2.py*"):
            old.unlink()
        rc: int = protoc.main(
            [
                "protoc",
                f"-I{tmp}",
                f"--python_out={SRC_DIR}",
                f"--pyi_out={SRC_DIR}",
                *[str(PKG_PATH / f) for f in FILES],
            ]
        )
        if rc != 0:
            return rc
        shutil.rmtree(SRC_DIR / "pymumble_rev" / "_proto" / "__pycache__", ignore_errors=True)

    for path in out.glob("*_pb2.py*"):
        text = path.read_text()
        if path.suffix == ".pyi":
            # Reject.RejectType 有个叫 None 的枚举值,写进 .pyi 是语法错误;
            # 运行时用 Name()/Value() 访问不受影响
            text = re.sub(r"^\s*None: .*\n", "", text, flags=re.MULTILINE)
        # 去掉行尾空白,让 diff 干净
        path.write_text(re.sub(r"[ \t]+$", "", text, flags=re.MULTILINE))
    return 0


if __name__ == "__main__":
    sys.exit(main())
