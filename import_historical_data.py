#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
历史数据导入脚本
从export_bsbdbfjfjff_all.txt文件中读取JSON格式的历史数据并导入到数据库
"""

import json
import re
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from model import Message, Base
from config import settings

def extract_links_from_text(text: str) -> dict:
    """从文本中提取网盘链接"""
    links = {}
    
    # 百度网盘链接模式
    baidu_pattern = r'https://pan\.baidu\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?'
    baidu_matches = re.findall(baidu_pattern, text)
    for match in baidu_matches:
        links["百度网盘"] = match
    
    # 夸克网盘链接模式
    quark_pattern = r'https://pan\.quark\.cn/s/[A-Za-z0-9_-]+'
    quark_matches = re.findall(quark_pattern, text)
    for match in quark_matches:
        links["夸克网盘"] = match
    
    # 阿里云盘链接模式
    aliyun_pattern = r'https://www\.aliyundrive\.com/s/[A-Za-z0-9_-]+'
    aliyun_matches = re.findall(aliyun_pattern, text)
    for match in aliyun_matches:
        links["阿里云盘"] = match
    
    # 115网盘链接模式
    pan115_pattern = r'https://115\.com/s/[A-Za-z0-9_-]+'
    pan115_matches = re.findall(pan115_pattern, text)
    for match in pan115_matches:
        links["115网盘"] = match
    
    # 迅雷网盘链接模式
    xunlei_pattern = r'https://pan\.xunlei\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?(?:#)?'
    xunlei_matches = re.findall(xunlei_pattern, text)
    for match in xunlei_matches:
        links["迅雷网盘"] = match
    
    # UC网盘链接模式
    uc_pattern = r'https://drive\.uc\.cn/s/[A-Za-z0-9]+(?:\?public=1)?'
    uc_matches = re.findall(uc_pattern, text)
    for match in uc_matches:
        links["UC网盘"] = match
    
    # 123pan网盘链接模式
    pan123pan_pattern = r'https://www\.123pan\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?'
    pan123pan_matches = re.findall(pan123pan_pattern, text)
    if pan123pan_matches:
        links["123网盘"] = pan123pan_matches[0]  # 取第一个匹配
    
    # 123684网盘链接模式（123网盘新域名）
    pan123684_pattern = r'https://www\.123684\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?'
    pan123684_matches = re.findall(pan123684_pattern, text)
    if pan123684_matches and "123网盘" not in links:
        links["123网盘"] = pan123684_matches[0]  # 只有在没有123pan链接时才使用123684链接
    
    # 天翼云盘链接模式
    tianyi_pattern = r'https://cloud\.189\.cn/t/[A-Za-z0-9]+'
    tianyi_matches = re.findall(tianyi_pattern, text)
    for match in tianyi_matches:
        links["天翼云盘"] = match
    
    # 移动云盘链接模式
    caiyun_pattern = r'https://caiyun\.139\.com/w/i/[A-Za-z0-9]+'
    caiyun_matches = re.findall(caiyun_pattern, text)
    for match in caiyun_matches:
        links["移动云盘"] = match
    
    return links

def extract_tags_from_text(text: str) -> list:
    """从文本中提取标签"""
    # 提取#开头的标签
    tag_pattern = r'#([^\s#]+)'
    tags = re.findall(tag_pattern, text)
    return list(set(tags))  # 去重

def parse_historical_message(data: dict) -> dict:
    """解析历史消息数据"""
    text = data.get('text', '')
    
    # 提取链接
    links = extract_links_from_text(text)
    
    # 如果没有网盘链接，跳过
    if not links:
        return None
    
    # 提取标签
    tags = extract_tags_from_text(text)
    
    # 解析时间
    date_str = data.get('date', '')
    try:
        timestamp = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except:
        timestamp = datetime.utcnow()
    
    # 提取标题和描述
    lines = text.split('\n')
    title = ''
    description = ''
    
    # 寻找标题（通常是第一行非空行）
    for line in lines:
        line = line.strip()
        if line and not line.startswith('#') and not line.startswith('🔗'):
            if not title:
                title = line
            else:
                description += line + '\n'
    
    description = description.strip()
    
    return {
        'title': title,
        'description': description,
        'links': links,
        'tags': tags,
        'timestamp': timestamp,
        'source': 'historical_import',
        'channel': 'unknown',
        'group_name': None,
        'bot': None
    }

def upsert_historical_message(session, parsed_data: dict) -> str:
    """插入或更新历史消息"""
    if not parsed_data or not parsed_data.get('links'):
        return 'skipped'
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # 检查是否已存在相同链接的消息
            existing = None
            for netdisk, link in parsed_data['links'].items():
                existing = session.query(Message).filter(
                    Message.links.op('->>')(netdisk) == link
                ).first()
                if existing:
                    break
            
            if existing:
                # 更新现有消息
                existing.title = parsed_data['title']
                existing.description = parsed_data['description']
                existing.tags = parsed_data['tags']
                existing.links = parsed_data['links']
                existing.timestamp = parsed_data['timestamp']
                session.commit()
                return 'updated'
            else:
                # 插入新消息
                new_msg = Message(
                    title=parsed_data['title'],
                    description=parsed_data['description'],
                    tags=parsed_data['tags'],
                    links=parsed_data['links'],
                    timestamp=parsed_data['timestamp'],
                    source=parsed_data['source'],
                    channel=parsed_data['channel'],
                    group_name=parsed_data['group_name'],
                    bot=parsed_data['bot']
                )
                session.add(new_msg)
                session.commit()
                return 'inserted'
        except OperationalError as e:
            print(f"⚠️ 数据库连接错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            session.rollback()
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避
                continue
            else:
                raise
        except Exception as e:
            print(f"❌ 数据库操作错误: {e}")
            session.rollback()
            return 'error'

def create_db_session_with_retry(max_retries=3):
    """创建数据库会话，带重试机制"""
    for attempt in range(max_retries):
        try:
            engine = create_engine(settings.DATABASE_URL, pool_pre_ping=True, pool_recycle=3600)
            Base.metadata.create_all(bind=engine)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            session = SessionLocal()
            # 测试连接
            session.execute(text('SELECT 1'))
            return session
        except OperationalError as e:
            print(f"⚠️ 数据库连接失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
            else:
                raise

def main():
    """主函数"""
    print("🚀 开始导入历史数据...")
    
    session = None
    try:
        # 创建数据库连接
        session = create_db_session_with_retry()
        
        # 读取历史数据文件
        file_path = '000.txt'
        print(f"📁 读取文件: {file_path}")
        
        total_lines = 0
        processed = 0
        inserted = 0
        updated = 0
        skipped = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                total_lines += 1
                line = line.strip()
                
                if not line:
                    continue
                
                try:
                    # 解析JSON数据
                    data = json.loads(line)
                    
                    # 解析消息
                    parsed = parse_historical_message(data)
                    
                    if parsed:
                        # 存储到数据库
                        result = upsert_historical_message(session, parsed)
                        processed += 1
                        
                        if result == 'inserted':
                            inserted += 1
                        elif result == 'updated':
                            updated += 1
                        else:
                            skipped += 1
                        
                        # 每处理100条记录显示进度
                        if processed % 100 == 0:
                            print(f"📊 已处理 {processed} 条记录 (插入: {inserted}, 更新: {updated}, 跳过: {skipped})")
                        
                        # 每1000条记录重新连接数据库
                        if processed % 1000 == 0:
                            try:
                                session.close()
                                session = create_db_session_with_retry()
                                print(f"🔄 重新连接数据库 (第 {processed} 条记录)")
                            except Exception as e:
                                print(f"❌ 重新连接失败: {e}")
                                break
                    
                except json.JSONDecodeError as e:
                    print(f"❌ 第 {line_num} 行JSON解析错误: {e}")
                    continue
                except Exception as e:
                    print(f"❌ 第 {line_num} 行处理错误: {e}")
                    continue
        
        print(f"\n✅ 导入完成!")
        print(f"📊 总计处理: {total_lines} 行")
        print(f"📊 有效消息: {processed} 条")
        print(f"📊 新增消息: {inserted} 条")
        print(f"📊 更新消息: {updated} 条")
        print(f"📊 跳过消息: {skipped} 条")
        
    except FileNotFoundError:
        print(f"❌ 文件不存在: {file_path}")
    except Exception as e:
        print(f"❌ 导入失败: {e}")
        if session:
            session.rollback()
    finally:
        if session:
            session.close()

if __name__ == "__main__":
    main()