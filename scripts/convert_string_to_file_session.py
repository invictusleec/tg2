import os
import sys
from telethon.sessions import StringSession, SQLiteSession

# 允许脚本在 scripts/ 目录内执行时找到项目根下的模块
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config import settings

def main():
    s = None
    if getattr(settings, 'EXPORT_STRING_SESSION', None):
        s = settings.EXPORT_STRING_SESSION.strip()
        print('🔐 使用 EXPORT_STRING_SESSION 转换为文件会话')
    elif getattr(settings, 'STRING_SESSION', None):
        s = settings.STRING_SESSION.strip().strip("\"").strip("'")
        print('🔐 使用 STRING_SESSION 转换为文件会话')
    if not s:
        raise RuntimeError('未找到会话字符串，请在 .env 设置 STRING_SESSION 或 EXPORT_STRING_SESSION')

    ss = StringSession(s)
    fs = SQLiteSession('session')
    # 设置数据中心与鉴权
    fs.set_dc(ss.dc_id, ss.server_address, ss.port)
    try:
        fs._auth_key = ss._auth_key
    except Exception:
        raise RuntimeError('无法设置文件会话的鉴权信息')
    # 尝试写入用户ID（如可用）
    try:
        if hasattr(ss, 'user_id') and ss.user_id:
            fs.user_id = ss.user_id
    except Exception:
        pass
    fs.save()
    p = os.path.abspath('session.session')
    print(f'✅ 已生成文件会话: {p}')

if __name__ == '__main__':
    main()
