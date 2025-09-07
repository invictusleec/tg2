import asyncio
import sys
from telethon import TelegramClient
from telethon.sessions import StringSession
from sqlalchemy.orm import Session
from model import Message, engine, create_tables
from datetime import timezone, timedelta
from config import settings
import re
import json
from typing import Dict, Any, List, Optional

# 北京时间时区
BEIJING_TZ = timezone(timedelta(hours=8))

# 从monitor.py复制必要的函数
def get_beijing_time():
    """获取当前北京时间"""
    from datetime import datetime
    return datetime.now(BEIJING_TZ)

def to_beijing_time(dt):
    """将UTC时间转换为北京时间"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # 假设是UTC时间
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BEIJING_TZ)

def parse_message(text: str) -> Dict[str, Any]:
    """解析消息文本，提取标题、描述、标签和网盘链接"""
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    if not lines:
        return {"title": "", "description": "", "tags": [], "links": {}}
    
    title = lines[0][:200]  # 限制标题长度
    description_lines = []
    tags = set()
    links = {}
    
    # 网盘链接模式
    patterns = {
        '夸克网盘': [r'https?://pan\.quark\.cn/s/[a-zA-Z0-9]+', r'https?://drive\.uc\.cn/s/[a-zA-Z0-9]+'],
        '阿里云盘': [r'https?://www\.alipan\.com/s/[a-zA-Z0-9]+', r'https?://www\.aliyundrive\.com/s/[a-zA-Z0-9]+'],
        '百度网盘': [r'https?://pan\.baidu\.com/s/[a-zA-Z0-9]+'],
        '115网盘': [r'https?://115\.com/s/[a-zA-Z0-9]+'],
        '天翼云盘': [r'https?://cloud\.189\.cn/[a-zA-Z0-9/]+', r'https?://cloud\.189\.cn/web/share\?code=[a-zA-Z0-9]+'],
        '123云盘': [r'https?://www\.123pan\.com/s/[a-zA-Z0-9]+'],
        'UC网盘': [r'https?://drive\.uc\.cn/s/[a-zA-Z0-9]+'],
        '迅雷': [r'https?://pan\.xunlei\.com/s/[a-zA-Z0-9]+']
    }
    
    for line in lines[1:]:
        # 提取标签
        tag_matches = re.findall(r'#([^#\s]+)', line)
        for tag in tag_matches:
            if len(tag) <= 20:  # 限制标签长度
                tags.add(tag)
        
        # 提取网盘链接
        found_link = False
        for netdisk_name, regexes in patterns.items():
            for regex in regexes:
                matches = re.findall(regex, line)
                if matches:
                    links[netdisk_name] = matches[0]  # 取第一个匹配
                    found_link = True
                    break
            if found_link:
                break
        
        # 如果不是标签行且不包含链接，加入描述
        if not re.match(r'^\s*#', line) and not found_link:
            description_lines.append(line)
    
    description = '\n'.join(description_lines)[:1000]  # 限制描述长度
    
    return {
        "title": title,
        "description": description,
        "tags": list(tags),
        "links": links
    }

def should_drop_by_rules(channel: str, parsed: Dict[str, Any]) -> bool:
    """简化版规则检查，这里暂时不实现复杂规则"""
    return False

def upsert_message_by_links(session: Session, parsed: Dict[str, Any], timestamp) -> str:
    """根据链接唯一性插入或更新消息"""
    if not parsed.get('links'):
        return 'skipped'
    
    # 检查是否已存在相同链接的消息
    existing = None
    for netdisk, link in parsed['links'].items():
        # 使用JSON操作符查询
        existing = session.query(Message).filter(
            Message.links.op('->>')(netdisk) == link
        ).first()
        if existing:
            break
    
    if existing:
        # 更新现有消息
        existing.title = parsed['title']
        existing.description = parsed['description']
        existing.tags = parsed['tags']
        existing.links = parsed['links']
        existing.channel = parsed['channel']
        existing.timestamp = timestamp
        session.commit()
        return 'updated'
    else:
        # 插入新消息
        new_msg = Message(
            title=parsed['title'],
            description=parsed['description'],
            tags=parsed['tags'],
            links=parsed['links'],
            channel=parsed['channel'],
            timestamp=timestamp
        )
        session.add(new_msg)
        session.commit()
        return 'inserted'

async def backfill_channel(client: TelegramClient, channel_username: str):
    """回溯抓取指定频道的历史消息"""
    uname = channel_username.lstrip('@').replace('https://t.me/', '') if channel_username else ''
    if not uname:
        print(f"❌ 无效的频道用户名: {channel_username}")
        return 0, 0, 0

    print(f"⏪ 开始回溯抓取频道: {uname}")
    
    inserted, updated, skipped = 0, 0, 0
    try:
        async for msg in client.iter_messages(uname, limit=None):
            text = getattr(msg, 'message', None) or getattr(msg, 'raw_text', None)
            if not text or not text.strip():
                continue
                
            parsed = parse_message(text)
            # 仅保存"关于网盘"的消息（必须包含 links）
            if not parsed.get('links'):
                skipped += 1
                continue
                
            parsed['channel'] = uname
            if should_drop_by_rules(uname, parsed):
                continue
                
            ts = to_beijing_time(getattr(msg, 'date', None)) or get_beijing_time()
            
            with Session(engine) as session:
                r = upsert_message_by_links(session, parsed, ts)
                if r == 'updated':
                    updated += 1
                else:
                    inserted += 1
                    
        print(f"✅ {uname} 回溯完成：新增 {inserted} 条，更新 {updated} 条，跳过非网盘 {skipped} 条")
        return inserted, updated, skipped
        
    except Exception as e:
        print(f"❌ {uname} 回溯抓取失败：{e}")
        return 0, 0, 0

async def batch_backfill_from_file(file_path: str):
    """从文件中读取频道列表并批量回溯"""
    # 创建数据库表
    create_tables()
    
    # 获取API凭据
    api_id = settings.TELEGRAM_API_ID
    api_hash = settings.TELEGRAM_API_HASH
    string_session = getattr(settings, 'STRING_SESSION', None)
    
    if not string_session:
        raise RuntimeError("未配置 STRING_SESSION，请在 .env 中设置后再运行本脚本")
    
    # 读取频道列表
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            channels = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ 文件不存在: {file_path}")
        return
    
    print(f"📋 从 {file_path} 读取到 {len(channels)} 个频道")
    
    # 创建Telegram客户端
    client = TelegramClient(StringSession(string_session.strip()), api_id, api_hash)
    
    total_inserted, total_updated, total_skipped = 0, 0, 0
    
    try:
        await client.start()
        print("✅ Telegram连接成功！")
        
        # 获取用户信息
        me = await client.get_me()
        print(f"👤 当前用户: {me.first_name} (@{me.username if me.username else 'N/A'})")
        
        # 逐个处理频道
        for i, channel in enumerate(channels, 1):
            print(f"\n[{i}/{len(channels)}] 处理频道: {channel}")
            inserted, updated, skipped = await backfill_channel(client, channel)
            total_inserted += inserted
            total_updated += updated
            total_skipped += skipped
            
            # 添加延迟避免频率限制
            if i < len(channels):
                await asyncio.sleep(2)
        
        print(f"\n🎉 批量回溯完成！")
        print(f"📊 总计：新增 {total_inserted} 条，更新 {total_updated} 条，跳过非网盘 {total_skipped} 条")
        
    except Exception as e:
        print(f"❌ 批量回溯失败：{e}")
    finally:
        await client.disconnect()

if __name__ == "__main__":
    file_path = "tg频道.txt"
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    
    print(f"🚀 开始批量回溯频道历史数据...")
    print(f"📁 频道列表文件: {file_path}")
    
    asyncio.run(batch_backfill_from_file(file_path))