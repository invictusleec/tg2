import os
import subprocess
from pathlib import Path

ENV_FILE = os.environ.get("ENV_FILE", "/data/.env")

REQUIRED = [
    "TELEGRAM_API_ID",
    "TELEGRAM_API_HASH",
    "DATABASE_URL",
    "DEFAULT_CHANNELS",
]


def load_env_from_file(path: str):
    p = Path(path)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            # Do not overwrite already-set envs from platform
            os.environ.setdefault(k.strip(), v.strip())


# 先尝试从 /data/.env 加载（支持先部署，后在网页里配置）
load_env_from_file(ENV_FILE)

# 新增：控制安装向导展示模式（always/auto/never），默认 always 满足“每次重新部署都进向导”
setup_mode = os.environ.get("SHOW_SETUP", "always").lower()
if setup_mode not in ("always", "auto", "never"):
    setup_mode = "always"

missing = [k for k in REQUIRED if not os.environ.get(k)]


def run_setup():
    cmd = [
        "streamlit", "run", "setup.py", "--server.port", "8501", "--server.address", "0.0.0.0"
    ]
    subprocess.run(cmd, check=False)


if setup_mode == "always":
    # 无条件进入安装向导
    run_setup()
elif setup_mode == "auto" and missing:
    # 缺少必填项时进入安装向导
    run_setup()
else:
    # 环境齐全或显式跳过安装向导：根据 RUN_MODE 决定启动
    run_mode = os.environ.get("RUN_MODE", "full").lower()  # full / ui
    if run_mode == "ui":
        # 只启动前台 UI（用于调试或轻量模式）
        subprocess.run([
            "bash", "-lc",
            "streamlit run web.py --server.port 8501 --server.address 0.0.0.0"
        ], check=False)
    else:
        # 启动：初始化 -> 监控 + 前台 + 后台
        subprocess.run([
            "bash", "-lc",
            "python init_db.py && (python monitor.py & streamlit run web.py --server.port 8501 --server.address 0.0.0.0 & streamlit run 后台.py --server.port 8502 --server.address 0.0.0.0 & wait)"
        ], check=False)