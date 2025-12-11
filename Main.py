from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import re
import requests
from config import settings

# 优先使用.env中的StringSession
string_session = settings.STRING_SESSION
if not string_session:
    raise RuntimeError("未配置STRING_SESSION，请在.env中设置STRING_SESSION后再运行Main.py")

# 凭证仍使用.env中的API配置（与监控保持一致）
api_id = settings.TELEGRAM_API_ID
api_hash = settings.TELEGRAM_API_HASH

# 目标频道用户名（不要带 @）
target_channel = 'xuexiziliaobaibaoku'

# 输出文件
output_file = 'kkbdziyuan副本_quark_links_unique.txt'

# 匹配夸克网盘链接
quark_url_pattern = r'https?://pan\.quark\.cn/s/[a-zA-Z0-9]+'

def resolve_redirect(url: str) -> str:
    """请求一次跳转或页面，尝试解析夸克直链"""
    try:
        resp = requests.get(url, allow_redirects=True, timeout=8, headers={
            "User-Agent": "Mozilla/5.0"
        })
        # 如果最终跳转就是夸克
        if re.match(quark_url_pattern, resp.url):
            return resp.url
        # 否则在页面里找
        match = re.search(quark_url_pattern, resp.text)
        if match:
            return match.group(0)
    except Exception as e:
        print(f"解析失败: {url} ({e})")
    return None


with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
    print(f'正在拉取频道 {target_channel} 的消息...')
    total = 0
    exported = 0
    all_urls_seen = set()

    with open(output_file, 'w', encoding='utf-8') as f:
        for message in client.iter_messages(target_channel, reverse=True):
            total += 1
            urls_in_message = set()

            # 1️⃣ 消息正文里的直链
            content = message.raw_text or ""
            urls_in_message.update(re.findall(quark_url_pattern, content))

            # 2️⃣ entities 里的 markdown 链接
            if message.entities:
                for ent in message.entities:
                    if hasattr(ent, 'url') and ent.url:  # MessageEntityTextUrl
                        if re.match(quark_url_pattern, ent.url):
                            urls_in_message.add(ent.url)

            # 3️⃣ 按钮里的 URL
            if message.buttons:
                for row in message.buttons:
                    for button in row:
                        if button.url:
                            if re.match(quark_url_pattern, button.url):
                                urls_in_message.add(button.url)
                            else:
                                final = resolve_redirect(button.url)
                                if final:
                                    urls_in_message.add(final)

            # 去重并写入
            urls_to_export = [url for url in urls_in_message if url not in all_urls_seen]
            if urls_to_export:
                exported += 1
                all_urls_seen.update(urls_to_export)

                clean_text = ' '.join(content.split())
                line = f"{exported}. {clean_text} {' '.join(urls_to_export)}"

                f.write(line + '\n')
                f.flush()

            if total % 100 == 0:
                print(f'已扫描 {total} 条消息，已导出 {exported} 条含夸克链接的消息', flush=True)

    print(f'\n完成！共扫描 {total} 条消息，导出 {exported} 条含夸克链接的消息，结果保存在 {output_file}')
