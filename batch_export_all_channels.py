#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
批量导出所有频道历史数据到txt文件
"""

import asyncio
import json
import os
from datetime import datetime
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import ChannelPrivateError, UsernameNotOccupiedError, FloodWaitError
from config import settings

# 优先使用.env中的StringSession（与Main.py保持一致）
string_session = settings.STRING_SESSION
if not string_session:
    raise RuntimeError("未配置STRING_SESSION，请在.env中设置STRING_SESSION后再运行")

# 凭证仍使用.env中的API配置
api_id = settings.TELEGRAM_API_ID
api_hash = settings.TELEGRAM_API_HASH

async def export_channel_messages(client, channel_url, output_file):
    """
    导出单个频道的消息到文件
    """
    try:
        # 提取频道用户名
        channel_username = channel_url.split('/')[-1]
        print(f"📡 开始导出频道: {channel_username}")
        
        # 获取频道实体
        try:
            entity = await client.get_entity(channel_username)
        except (ChannelPrivateError, UsernameNotOccupiedError) as e:
            print(f"❌ 无法访问频道 {channel_username}: {e}")
            return 0
        
        message_count = 0
        
        # 遍历频道消息
        async for message in client.iter_messages(entity, limit=None):
            if message.text:
                # 构造消息数据
                message_data = {
                    'id': message.id,
                    'date': message.date.isoformat() if message.date else None,
                    'text': message.text,
                    'channel': channel_url,
                    'channel_username': channel_username,
                    'message_url': f"{channel_url}/{message.id}",
                    'views': getattr(message, 'views', None),
                    'forwards': getattr(message, 'forwards', None),
                    'reply_to': message.reply_to_msg_id if message.reply_to else None,
                    'media_type': str(type(message.media).__name__) if message.media else None
                }
                
                # 写入文件
                output_file.write(json.dumps(message_data, ensure_ascii=False) + '\n')
                message_count += 1
                
                # 每1000条消息显示进度
                if message_count % 1000 == 0:
                    print(f"  📊 {channel_username}: 已导出 {message_count} 条消息")
        
        print(f"✅ 频道 {channel_username} 导出完成: {message_count} 条消息")
        return message_count
        
    except FloodWaitError as e:
        print(f"⚠️ 频道 {channel_username} 遇到限流，等待 {e.seconds} 秒")
        await asyncio.sleep(e.seconds)
        return 0
    except Exception as e:
        print(f"❌ 导出频道 {channel_username} 时出错: {e}")
        return 0

async def main():
    """
    主函数：批量导出所有频道
    """
    print("🚀 开始批量导出所有频道历史数据...")
    
    # 读取频道列表
    channels_file = 'tg频道.txt'
    if not os.path.exists(channels_file):
        print(f"❌ 频道列表文件不存在: {channels_file}")
        return
    
    with open(channels_file, 'r', encoding='utf-8') as f:
        channels = [line.strip() for line in f if line.strip()]
    
    print(f"📋 找到 {len(channels)} 个频道")
    
    # 创建输出文件
    output_filename = f"all_channels_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    # 创建Telegram客户端（使用StringSession）
    client = TelegramClient(StringSession(string_session), api_id, api_hash)
    
    try:
        await client.start()
        print("✅ Telegram客户端连接成功")
        
        total_messages = 0
        successful_channels = 0
        
        with open(output_filename, 'w', encoding='utf-8') as output_file:
            for i, channel_url in enumerate(channels, 1):
                print(f"\n[{i}/{len(channels)}] 处理频道: {channel_url}")
                
                message_count = await export_channel_messages(client, channel_url, output_file)
                
                if message_count > 0:
                    total_messages += message_count
                    successful_channels += 1
                
                # 每个频道之间稍作延迟，避免限流
                if i < len(channels):
                    await asyncio.sleep(2)
        
        print(f"\n🎉 批量导出完成!")
        print(f"📊 统计信息:")
        print(f"  - 总频道数: {len(channels)}")
        print(f"  - 成功导出: {successful_channels}")
        print(f"  - 总消息数: {total_messages}")
        print(f"  - 输出文件: {output_filename}")
        
    except Exception as e:
        print(f"❌ 批量导出失败: {e}")
    finally:
        await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())