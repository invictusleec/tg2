#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对 all_channels_export_*.txt（很大）文件做快速抽样清洗：
- 每隔 N 行抽取一行，形成样本
- 清洗规则：
  1) 仅将 https://t.me/ 链接用于识别 channel 字段，并从标题/描述/标签中移除
  2) 移除噪声关键词："频道"、"搜索结果"、"夸克频道"、"群组"、"投稿/搜索"、"来自：[雷锋]"、"投稿"
  3) 移除 @username（包括 @yunpans 等）在 title/description/tags 中的出现
  4) 解析网盘链接（夸克/阿里/115/百度/天翼/UC/迅雷/123pan/123684/移动云）
- 产出：cleaned_sample.jsonl（JSONL）
- 可选：--import 导入前 10 条清洗数据以测试
"""

import argparse
import json
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy.orm import Session

from model import Message, engine
from import_historical_data import extract_links_from_text, extract_tags_from_text

BEIJING_TZ = timezone(timedelta(hours=8))

NOISE_KEYWORDS = [
    '频道', '搜索结果', '夸克频道', '群组', '投稿/搜索', '来自：[雷锋]', '投稿'
]

TELEGRAM_LINK_RE = re.compile(r'https?://t\.me/([A-Za-z0-9_+]+)/?')
AT_HANDLE_RE = re.compile(r'@([A-Za-z0-9_]{3,})')

# 简单 URL 提取，用于在清洗后剔除 t.me URL
URL_RE = re.compile(r'https?://[^\s]+')


def to_naive_beijing(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(BEIJING_TZ).replace(tzinfo=None)


def parse_timestamp(raw: Optional[str]) -> datetime:
    if not raw:
        return to_naive_beijing(datetime.now(BEIJING_TZ))
    try:
        # 兼容 "2025-09-04 15:17:15.849487" 与 ISO8601
        raw2 = raw.replace('Z', '+00:00')
        # 如果没有时区，当作北京时间
        dt = datetime.fromisoformat(raw2)
        if dt.tzinfo is None:
            return to_naive_beijing(dt.replace(tzinfo=BEIJING_TZ))
        return to_naive_beijing(dt)
    except Exception:
        try:
            # 常见格式兜底
            return to_naive_beijing(datetime.strptime(raw, '%Y-%m-%d %H:%M:%S'))
        except Exception:
            return to_naive_beijing(datetime.now(BEIJING_TZ))


def remove_noise(text: str) -> str:
    if not text:
        return ''
    out = text
    for kw in NOISE_KEYWORDS:
        out = out.replace(kw, '')
    return out


def strip_at_handles(text: str) -> str:
    # 删除 @username 以及紧随的空白
    return AT_HANDLE_RE.sub('', text)


def extract_channel_from_text(text: str) -> Optional[str]:
    # 从 t.me 链接中提取 channel 名称（第一段 path）
    # 仅接受由字母/数字/下划线组成的用户名（忽略 + 邀请）
    matches = TELEGRAM_LINK_RE.findall(text or '')
    for m in matches:
        if m and not m.startswith('+') and re.fullmatch(r'[A-Za-z0-9_]{3,}', m):
            return m
    return None


def normalize_channel(src_channel: Optional[str], detected_username: Optional[str]) -> str:
    """将 channel 规范为 https://t.me/<username>，优先使用 src_channel 中的 t.me 或 @handle，其次使用 detected_username"""
    def to_url(u: str) -> Optional[str]:
        if not u:
            return None
        u = u.strip()
        if u.startswith('http://t.me/') or u.startswith('https://t.me/'):
            # 提取用户名部分
            m = re.search(r'https?://t\.me/([A-Za-z0-9_]+)', u)
            if m:
                return f"https://t.me/{m.group(1)}"
            return None
        if u.startswith('@'):
            u = u[1:]
        if re.fullmatch(r'[A-Za-z0-9_]{3,}', u):
            return f"https://t.me/{u}"
        return None

    # 1) 尝试 src_channel
    url = to_url(src_channel or '')
    if url:
        return url
    # 2) 尝试 detected_username
    url = to_url(detected_username or '')
    if url:
        return url
    return ''


def remove_telegram_links(text: str) -> str:
    # 移除所有 t.me 链接
    def repl(m):
        url = m.group(0)
        if url.startswith('https://t.me/') or url.startswith('http://t.me/'):
            return ''
        return url
    return URL_RE.sub(repl, text)


def text_to_title_desc(text: str) -> (str, str):
    lines = [ln.strip() for ln in (text or '').split('\n')]
    lines = [ln for ln in lines if ln]
    if not lines:
        return '', ''
    title = lines[0][:200]
    desc = '\n'.join(lines[1:]).strip()
    return title, desc


def clean_record(raw_line: str) -> Optional[Dict[str, Any]]:
    raw_line = raw_line.strip()
    if not raw_line:
        return None

    # 如果整行包含噪声关键词，直接丢弃该记录
    lowered = raw_line
    for kw in NOISE_KEYWORDS:
        if kw in lowered:
            return None

    src_channel = None
    src_timestamp = None
    src_title = None
    src_desc = None
    src_tags: List[str] = []
    src_text = None

    # 1) 先尝试 JSON 解析
    data = None
    try:
        data = json.loads(raw_line)
    except Exception:
        data = None

    if isinstance(data, dict):
        # 兼容几种常见字段
        src_text = data.get('text') or data.get('message') or ''
        src_title = data.get('title')
        src_desc = data.get('description')
        src_tags = data.get('tags') or []
        src_timestamp = data.get('timestamp') or data.get('date') or data.get('created_at')
        src_channel = data.get('channel') or data.get('chat') or None

        # 如果没有 text，将 title/description 拼为 text 以便统一清洗
        if not src_text:
            parts = []
            if src_title:
                parts.append(str(src_title))
            if src_desc:
                parts.append(str(src_desc))
            src_text = '\n'.join(parts)
    else:
        # 非 JSON，按纯文本处理
        src_text = raw_line

    # 2) 抽取 channel（来自 t.me），并从文本中移除 t.me 链接
    detected_channel = extract_channel_from_text(src_text or '')
    # 规范为 URL（https://t.me/<username>），满足“这种只能放在 channel 字段”
    channel_url = normalize_channel(src_channel, detected_channel)

    # 3) 合成原始文本用于清洗
    combined_text = src_text or ''

    # 4) 移除噪声关键词
    combined_text = remove_noise(combined_text)

    # 5) 从文本中移除 t.me 链接
    combined_text = remove_telegram_links(combined_text)

    # 6) 移除 @handles
    combined_text = strip_at_handles(combined_text)

    # 7) 解析网盘链接
    links = extract_links_from_text(combined_text)

    if not links:
        # 没有网盘链接，跳过
        return None

    # 8) 解析标签：优先 JSON tags，否则从清洗后的文本中提取
    tags = list({*(src_tags or []), *extract_tags_from_text(combined_text)}) if combined_text else (src_tags or [])
    # 再次清理 tags 中的 @xxx
    tags = [AT_HANDLE_RE.sub('', t).strip() for t in tags if t and t.strip()]
    tags = [t for t in tags if t]

    # 9) 构造标题与描述
    title, description = text_to_title_desc(combined_text)

    # 如果 JSON 自带标题，且清洗后的标题为空，可回退
    if not title and src_title:
        title = strip_at_handles(remove_noise(str(src_title)))[:200]
    if not description and src_desc:
        description = strip_at_handles(remove_noise(str(src_desc)))

    # 10) 时间
    timestamp = parse_timestamp(src_timestamp)

    cleaned = {
        'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
        'title': title,
        'description': description,
        'links': links,
        'tags': tags,
        'source': 'cleaned_export',
        'channel': channel_url or '',
        'group_name': '',
        'bot': '',
        'created_at': datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S')
    }
    return cleaned


def sample_and_clean(inputs: List[str], interval: int, limit_preview: int = 100) -> List[Dict[str, Any]]:
    """对多个大文件进行抽样（每 interval 行抽 1 行），并清洗，返回样本列表。
    策略：按 offset 从 0..interval-1 逐步抽样（先 0 再 1...），尽量凑够 limit_preview。
    """
    results: List[Dict[str, Any]] = []
    if interval <= 0:
        interval = 1000

    # 先尝试 offset=0（经典每1000行第1000行），不足再逐步偏移
    for offset in range(0, interval):
        if len(results) >= limit_preview:
            break
        for path in inputs:
            if len(results) >= limit_preview:
                break
            if not os.path.exists(path):
                print(f"❌ 文件不存在: {path}")
                continue
            picked_in_this_file = 0
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for idx, line in enumerate(f, 1):
                    if len(results) >= limit_preview:
                        break
                    # idx % interval == 0 对应 offset=0；通用为 (idx - offset) % interval == 0
                    if (idx - offset) % interval != 0:
                        continue
                    cleaned = clean_record(line)
                    if cleaned:
                        results.append(cleaned)
                        picked_in_this_file += 1
            if picked_in_this_file:
                print(f"📦 {os.path.basename(path)} @offset={offset} 抽到 {picked_in_this_file} 条")
    return results


def write_jsonl(path: str, rows: List[Dict[str, Any]]):
    with open(path, 'w', encoding='utf-8') as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + '\n')
    print(f"✅ 已写入样本文件: {path} （共 {len(rows)} 条）")


def import_first_n(rows: List[Dict[str, Any]], n: int = 10) -> List[int]:
    """将样本导入数据库（Message 表），按成功计数直到插入满 n 条或样本用尽；返回插入的记录 id 列表"""
    inserted = 0
    inserted_ids: List[int] = []
    with Session(engine) as session:
        for row in rows:
            if inserted >= n:
                break
            try:
                ts = parse_timestamp(row.get('timestamp'))
                ca = parse_timestamp(row.get('created_at'))

                # 去重：任一网盘链接存在则跳过该条
                exists = None
                for netdisk, link in (row.get('links') or {}).items():
                    q = session.query(Message).filter(
                        Message.links.op('->>')(netdisk) == link
                    ).first()
                    if q:
                        exists = q
                        break
                if exists:
                    continue

                msg = Message(
                    timestamp=ts,
                    title=row.get('title') or '',
                    description=row.get('description') or '',
                    links=row.get('links') or {},
                    tags=row.get('tags') or [],
                    source=row.get('source') or 'cleaned_export',
                    channel=row.get('channel') or '',
                    group_name=row.get('group_name') or '',
                    bot=row.get('bot') or '',
                    created_at=ca,
                )
                session.add(msg)
                session.flush()  # 先获取自增 ID
                inserted_ids.append(msg.id)
                inserted += 1
            except Exception as e:
                print(f"❌ 插入失败: {e}")
        session.commit()
    print(f"✅ 已导入 {inserted} 条样本记录到数据库")
    return inserted_ids


def query_and_print_imported(n: int, run_start_naive: datetime):
    """查询并打印本次运行插入的最近 n 条记录（依据 created_at >= run_start_naive 且 source=cleaned_export）"""
    with Session(engine) as session:
        rows = (
            session.query(Message)
            .filter(Message.source == 'cleaned_export')
            .filter(Message.created_at >= run_start_naive)
            .order_by(Message.created_at.desc())
            .limit(n)
            .all()
        )
        print(f"\n===== 本次导入的 {len(rows)} 条记录（倒序）=====\n")
        for m in rows:
            obj = {
                'id': getattr(m, 'id', None),
                'timestamp': m.timestamp.strftime('%Y-%m-%d %H:%M:%S') if m.timestamp else None,
                'title': m.title,
                'description': m.description,
                'links': m.links,
                'tags': m.tags,
                'source': m.source,
                'channel': m.channel,
                'group_name': m.group_name,
                'bot': m.bot,
                'created_at': m.created_at.strftime('%Y-%m-%d %H:%M:%S') if m.created_at else None,
            }
            print(json.dumps(obj, ensure_ascii=False))


def query_and_print_by_ids(ids: List[int]):
    if not ids:
        print("⚠️ 无可查询的 ID")
        return
    with Session(engine) as session:
        rows = (
            session.query(Message)
            .filter(Message.id.in_(ids))
            .order_by(Message.id.asc())
            .all()
        )
        print(f"\n===== 按ID回显本次导入的 {len(rows)} 条记录 =====\n")
        for m in rows:
            obj = {
                'id': getattr(m, 'id', None),
                'timestamp': m.timestamp.strftime('%Y-%m-%d %H:%M:%S') if m.timestamp else None,
                'title': m.title,
                'description': m.description,
                'links': m.links,
                'tags': m.tags,
                'source': m.source,
                'channel': m.channel,
                'group_name': m.group_name,
                'bot': m.bot,
                'created_at': m.created_at.strftime('%Y-%m-%d %H:%M:%S') if m.created_at else None,
            }
            print(json.dumps(obj, ensure_ascii=False))


def find_existing_by_links(session: Session, links: Dict[str, str]) -> Optional[Message]:
    if not links:
        return None
    # 逐个网盘键查询，任一匹配即认为存在
    for netdisk, link in links.items():
        if not link:
            continue
        try:
            m = (
                session.query(Message)
                .filter(Message.links.op('->>')(netdisk) == link)
                .first()
            )
            if m:
                return m
        except Exception:
            # 某些数据库适配器可能不支持 ->> 操作，忽略异常继续
            continue
    return None


def upsert_row(session: Session, row: Dict[str, Any]) -> Tuple[str, int]:
    """按链接去重并覆盖：
    - 若存在相同链接的记录：覆盖更新并返回 ('updated', id)
    - 否则插入新记录并返回 ('inserted', id)
    """
    ts = parse_timestamp(row.get('timestamp'))
    ca = parse_timestamp(row.get('created_at'))

    links = row.get('links') or {}
    existing = find_existing_by_links(session, links)
    if existing:
        # 覆盖字段
        existing.timestamp = ts
        existing.title = row.get('title') or ''
        existing.description = row.get('description') or ''
        existing.links = links
        existing.tags = row.get('tags') or []
        existing.source = row.get('source') or 'cleaned_export'
        existing.channel = row.get('channel') or ''
        existing.group_name = row.get('group_name') or ''
        existing.bot = row.get('bot') or ''
        existing.created_at = ca
        session.add(existing)
        session.flush()
        return ('updated', int(existing.id or 0))

    # 插入
    msg = Message(
        timestamp=ts,
        title=row.get('title') or '',
        description=row.get('description') or '',
        links=links,
        tags=row.get('tags') or [],
        source=row.get('source') or 'cleaned_export',
        channel=row.get('channel') or '',
        group_name=row.get('group_name') or '',
        bot=row.get('bot') or '',
        created_at=ca,
    )
    session.add(msg)
    session.flush()
    return ('inserted', int(msg.id or 0))


def process_full_clean_only(inputs: List[str], output_path: str, dedup: bool = True) -> Dict[str, int]:
    """全量清洗（不入库），将清洗结果写入 JSONL；在清洗阶段按“链接相同即重复”去重。
    返回统计：{'processed': n, 'written': w, 'skipped': s, 'dedup_skipped': d}
    """
    if not output_path:
        raise ValueError('output_path 不能为空')
    processed = written = skipped = dedup_skipped = 0
    seen_links = set()  # 以链接字符串去重
    with open(output_path, 'w', encoding='utf-8') as fout:
        for path in inputs:
            if not os.path.exists(path):
                print(f"❌ 文件不存在: {path}")
                continue
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    processed += 1
                    cleaned = clean_record(line)
                    if not cleaned:
                        skipped += 1
                        continue
                    if dedup:
                        links = cleaned.get('links') or {}
                        # 如果任一链接已出现，则跳过本条
                        is_dup = False
                        for _k, _v in links.items():
                            if not _v:
                                continue
                            key = _v.strip()
                            if key in seen_links:
                                is_dup = True
                                break
                        if is_dup:
                            dedup_skipped += 1
                            continue
                        # 将本条的所有链接加入已见集合
                        for _k, _v in links.items():
                            if _v:
                                seen_links.add(_v.strip())
                    fout.write(json.dumps(cleaned, ensure_ascii=False) + '\n')
                    written += 1
    print(f"\n===== 全量清洗完成（仅输出JSONL） =====")
    print(f"处理行数: {processed}")
    print(f"写入: {written} 条")
    print(f"跳过(无链接/噪声/错误): {skipped} 条")
    if dedup:
        print(f"去重跳过: {dedup_skipped} 条")
    return {'processed': processed, 'written': written, 'skipped': skipped, 'dedup_skipped': dedup_skipped}


def process_full_upsert(inputs: List[str], output_path: str = '', commit_every: int = 500) -> Dict[str, int]:
    """全量清洗并覆盖导入数据库：
    - 逐行读取 inputs 中的所有文件
    - 清洗后若存在链接则 upsert 到数据库（按链接相同即重复）
    - 可选将清洗后的每条写入 output_path（JSONL）
    返回统计信息：{'inserted': x, 'updated': y, 'skipped': z, 'processed': n}
    """
    inserted = updated = skipped = processed = 0
    fout = None
    if output_path:
        fout = open(output_path, 'w', encoding='utf-8')

    with Session(engine) as session:
        for path in inputs:
            if not os.path.exists(path):
                print(f"❌ 文件不存在: {path}")
                continue
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    processed += 1
                    cleaned = clean_record(line)
                    if not cleaned:
                        skipped += 1
                        continue
                    # 可选落地到 JSONL
                    if fout:
                        fout.write(json.dumps(cleaned, ensure_ascii=False) + '\n')
                    try:
                        action, _id = upsert_row(session, cleaned)
                        if action == 'inserted':
                            inserted += 1
                        else:
                            updated += 1
                    except Exception as e:
                        skipped += 1
                        print(f"❌ upsert 失败: {e}")
                    if (inserted + updated) % commit_every == 0:
                        session.commit()
        session.commit()

    if fout:
        fout.close()
    return {'inserted': inserted, 'updated': updated, 'skipped': skipped, 'processed': processed}


def import_jsonl_insert_only(path: str, commit_every: int = 500) -> Dict[str, int]:
    """从清洗后的 JSONL 导入数据库（仅插入，不去重也不覆盖）。返回统计。"""
    if not path or not os.path.exists(path):
        raise FileNotFoundError(f'文件不存在: {path}')
    inserted = skipped = processed = 0
    with Session(engine) as session:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                processed += 1
                line = line.strip()
                if not line:
                    skipped += 1
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    skipped += 1
                    continue
                try:
                    ts = parse_timestamp(row.get('timestamp'))
                    ca = parse_timestamp(row.get('created_at'))
                    msg = Message(
                        timestamp=ts,
                        title=row.get('title') or '',
                        description=row.get('description') or '',
                        links=row.get('links') or {},
                        tags=row.get('tags') or [],
                        source=row.get('source') or 'cleaned_export',
                        channel=row.get('channel') or '',
                        group_name=row.get('group_name') or '',
                        bot=row.get('bot') or '',
                        created_at=ca,
                    )
                    session.add(msg)
                    inserted += 1
                    if inserted % commit_every == 0:
                        session.commit()
                except Exception as e:
                    skipped += 1
                    print(f"❌ 插入失败: {e}")
            session.commit()
    print(f"\n===== JSONL 导入完成（仅插入） =====")
    print(f"读取行数: {processed}")
    print(f"成功插入: {inserted} 条")
    print(f"跳过(解析错误等): {skipped} 条")
    return {'processed': processed, 'inserted': inserted, 'skipped': skipped}


def main():
    parser = argparse.ArgumentParser(description='抽样清洗 all_channels_export_* 数据并可选导入样本到数据库')
    parser.add_argument('--interval', type=int, default=1000, help='抽样间隔（每 N 行抽 1 行）')
    parser.add_argument('--limit', type=int, default=2000, help='样本数量上限（多文件总计）')
    parser.add_argument('--import', dest='do_import', action='store_true', help='是否导入样本到数据库')
    parser.add_argument('--import_n', type=int, default=20, help='导入的样本条数（按成功计数）')
    parser.add_argument('--inputs', nargs='*', default=[
        'all_channels_export_20250906_161213.txt',
        'all_channels_export_20250907_020701.txt'
    ], help='输入文件列表')
    parser.add_argument('--output', default='cleaned_sample.jsonl', help='清洗样本输出路径')
    # 新增：全量与覆盖导入
    parser.add_argument('--full', dest='do_full', action='store_true', help='全量清洗（不抽样）')
    parser.add_argument('--upsert', dest='do_upsert', action='store_true', help='按链接去重并覆盖导入（数据库）')
    parser.add_argument('--output_full', default='', help='全量清洗JSONL输出路径（可选）')
    # 新增：从 JSONL 导入（仅插入）与批量提交间隔
    parser.add_argument('--import_json', default='', help='从清洗JSONL导入数据库（仅插入，不去重）')
    parser.add_argument('--commit_every', type=int, default=500, help='导入/覆盖时的批量提交间隔')

    args = parser.parse_args()

    run_start_naive = to_naive_beijing(datetime.now(BEIJING_TZ))

    # 全量覆盖导入模式
    if args.do_full and args.do_upsert:
        print(f"🚀 开始全量清洗并覆盖导入：源文件 {len(args.inputs)} 个，按链接相同去重，数据库直接覆盖")
        stats = process_full_upsert(args.inputs, args.output_full)
        print(f"\n===== 全量覆盖导入完成 =====")
        print(f"处理行数: {stats['processed']}")
        print(f"插入: {stats['inserted']} 条")
        print(f"覆盖更新: {stats['updated']} 条")
        print(f"跳过(无链接/噪声/错误): {stats['skipped']} 条")
        print("🎉 处理完成！")
        return

    # 新增：全量清洗仅输出（清洗阶段已去重）
    if args.do_full and not args.do_upsert:
        if not args.output_full:
            print("❌ 全量清洗（仅输出）需要指定 --output_full 路径")
            return
        print(f"🚀 开始全量清洗（仅输出JSONL，清洗阶段去重），源文件 {len(args.inputs)} 个")
        stats = process_full_clean_only(args.inputs, args.output_full, dedup=True)
        print("🎉 处理完成！")
        return

    # 新增：从 JSONL 导入数据库（仅插入）
    if args.import_json:
        print(f"🔄 从 {args.import_json} 导入数据库（仅插入，不去重）...")
        stats = import_jsonl_insert_only(args.import_json, commit_every=args.commit_every)
        print("🎉 处理完成！")
        return

    # 抽样清洗 + 可选样本导入模式（原有逻辑）
    print(f"🚀 开始抽样清洗：每 {args.interval} 行抽 1 行，最多 {args.limit} 条样本（多文件合并）")
    rows = sample_and_clean(args.inputs, args.interval, args.limit)
    if not rows:
        print("❌ 未得到任何有效样本（可能没有网盘链接或文件不可读）")
        return

    write_jsonl(args.output, rows)

    if args.do_import:
        print(f"🔄 正在导入前 {args.import_n} 条样本到数据库（按成功计数）...")
        inserted_ids = import_first_n(rows, args.import_n)
        # 优先按 ID 精确回显
        query_and_print_by_ids(inserted_ids)
        # 若未回显到任何记录，再 fallback 按时间窗口查询
        if not inserted_ids:
            query_and_print_imported(args.import_n, run_start_naive)

    print("🎉 处理完成！")


if __name__ == '__main__':
    main()