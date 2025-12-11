import os
import sys
from telethon.sync import TelegramClient
from telethon.sessions import StringSession, SQLiteSession

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.credentials import get_api_credentials

def write_env_kv(env_path: str, key: str, value: str):
    lines = []
    if os.path.exists(env_path):
        with open(env_path, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()
    updated = False
    new_lines = []
    for line in lines:
        if line.startswith(f'{key}='):
            new_lines.append(f'{key}={value}')
            updated = True
        else:
            new_lines.append(line)
    if not updated:
        new_lines.append(f'{key}={value}')
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(new_lines) + '\n')

def main():
    api_id, api_hash = get_api_credentials()
    if not api_id or not api_hash:
        raise RuntimeError('未找到 API 凭据，请在后台添加或在 .env 配置 TELEGRAM_API_ID/TELEGRAM_API_HASH')

    print('=== 交互式生成新的 StringSession 与文件会话 session.session ===')
    print('提示：将按 Telethon 流程要求输入手机号与验证码（如开启两步验证，还需密码）。\n')

    # 1) 交互式登录生成 StringSession
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        client.start()
        s = client.session.save()
        print('你的StringSession长字符串：')
        print('=' * 50)
        print(s)
        print('=' * 50)
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        write_env_kv(env_path, 'STRING_SESSION', s)
        write_env_kv(env_path, 'EXPORT_STRING_SESSION', s)
        print('已写入 .env 的 STRING_SESSION 与 EXPORT_STRING_SESSION')

        # 2) 由刚生成的 StringSession 转换为文件会话（无需再次验证码）
        ss = client.session  # StringSession 实例
        fs = SQLiteSession('session')
        fs.set_dc(ss.dc_id, ss.server_address, ss.port)
        fs._auth_key = ss._auth_key
        try:
            if hasattr(ss, 'user_id') and ss.user_id:
                fs.user_id = ss.user_id
        except Exception:
            pass
        fs.save()

    p = os.path.abspath(os.path.join(os.path.dirname(os.path.dirname(__file__)), 'session.session'))
    print(f'✅ 已生成文件会话: {p}')

if __name__ == '__main__':
    main()
