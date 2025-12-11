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

# 读取平台提供的端口（如 Render/Heroku/自建 PaaS 会注入 PORT）；默认 8501
WEB_PORT = os.environ.get("PORT", "8501")
try:
    int(WEB_PORT)
except Exception:
    WEB_PORT = "8501"
# 后台管理端口可通过 ADMIN_PORT 控制（默认 8502）；部分 PaaS 仅允许暴露单端口，若无法映射请忽略后台端口
ADMIN_PORT = os.environ.get("ADMIN_PORT", "8502")
try:
    int(ADMIN_PORT)
except Exception:
    ADMIN_PORT = "8502"

missing = [k for k in REQUIRED if not os.environ.get(k)]


def run_setup():
    # 安装向导页也绑定到 WEB_PORT，确保在仅允许使用 $PORT 的平台上可被健康检查识别
    cmd = [
        "streamlit", "run", "setup.py", "--server.port", str(WEB_PORT), "--server.address", "0.0.0.0"
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
            f"streamlit run web.py --server.port {WEB_PORT} --server.address 0.0.0.0"
        ], check=False)
    else:
        # 改进：去掉嵌套子进程与双重后台，确保 wait 能正确阻塞主进程，容器不提前退出
        # 顺序：init_db（容忍失败，不阻塞）; 并行启动 web/admin/monitor；最后 wait 保持前台
        # 注意：移除日志重定向，直接输出到 stdout 以便 docker logs 查看
        cmd = (
            "bash -lc \""
            "python init_db.py || echo 'init_db failed (non-fatal)'; "
            f"streamlit run web.py --server.port {WEB_PORT} --server.address 0.0.0.0 & "
            f"streamlit run 后台.py --server.port {ADMIN_PORT} --server.address 0.0.0.0 & "
            "python -u monitor.py & "
            "wait\""
        )
        subprocess.run(cmd, shell=True, check=False)