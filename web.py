import streamlit as st
from sqlalchemy.orm import Session
from model import Message, engine
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import Counter
from sqlalchemy import or_, cast, String
from sqlalchemy.exc import OperationalError
import json
import os
import math

# 统一在顶部定义分页大小，供后续函数默认参数使用
PAGE_SIZE = 50

# 初始化session_state用于标签筛选
if 'selected_tags' not in st.session_state:
    st.session_state['selected_tags'] = []

st.set_page_config(
    page_title="📱 TG频道监控",
    page_icon="📱",
    layout="wide"
)

# 设置页面标题
st.title("📱 TG频道监控")

# 创建侧边栏
st.sidebar.header("筛选条件")

# 时间范围选择
time_range = st.sidebar.selectbox(
    "时间范围",
    ["最近24小时", "最近7天", "最近30天", "全部"]
)

# 标签选择（标签云，显示数量，降序）
@st.cache_data(ttl=300)
def get_tag_data():
    try:
        with Session(engine) as session:
            cutoff = datetime.now() - timedelta(days=90)
            all_tags = (
                session.query(Message.tags)
                .filter(Message.timestamp >= cutoff)
                .limit(200000)
                .all()
            )
    except OperationalError:
        engine.dispose()
        try:
            with Session(engine) as session:
                cutoff = datetime.now() - timedelta(days=90)
                all_tags = (
                    session.query(Message.tags)
                    .filter(Message.timestamp >= cutoff)
                    .limit(200000)
                    .all()
                )
        except Exception:
            all_tags = []
    tag_list = [tag for tags in all_tags for tag in (tags[0] if tags[0] else [])]
    tag_counter = Counter(tag_list)
    tag_items = sorted(tag_counter.items(), key=lambda x: x[1], reverse=True)
    tag_options = [f"{tag} ({count})" for tag, count in tag_items]
    tag_map = {f"{tag} ({count})": tag for tag, count in tag_items}
    return tag_options, tag_map, {tag: count for tag, count in tag_items}

try:
    tag_options, tag_map, tag_counter = get_tag_data()
except Exception:
    tag_options, tag_map, tag_counter = [], {}, {}

# 默认选中session_state中的标签
selected_tag_labels = st.sidebar.multiselect(
    "标签", tag_options,
    default=[f"{tag} ({tag_counter[tag]})" for tag in st.session_state['selected_tags'] if tag in tag_counter]
)
selected_tags = [tag_map[label] for label in selected_tag_labels]
# 同步session_state
st.session_state['selected_tags'] = selected_tags

# 动态获取网盘类型（近90天，带计数），并允许多选
@st.cache_data(ttl=300)
def get_netdisk_data():
    try:
        with Session(engine) as session:
            cutoff = datetime.now() - timedelta(days=90)
            rows = (
                session.query(Message.links)
                .filter(Message.timestamp >= cutoff)
                .limit(200000)
                .all()
            )
    except OperationalError:
        engine.dispose()
        try:
            with Session(engine) as session:
                cutoff = datetime.now() - timedelta(days=90)
                rows = (
                    session.query(Message.links)
                    .filter(Message.timestamp >= cutoff)
                    .limit(200000)
                    .all()
                )
        except Exception:
            rows = []
    keys = []
    for r in rows:
        links = r[0] if r else None
        if isinstance(links, dict):
            keys.extend(list(links.keys()))
    counter = Counter(keys)
    items = sorted(counter.items(), key=lambda x: x[1], reverse=True)
    options = [f"{k} ({v})" for k, v in items]
    key_map = {f"{k} ({v})": k for k, v in items}
    return options, key_map, {k: v for k, v in items}

try:
    netdisk_options, netdisk_map, netdisk_counter = get_netdisk_data()
except Exception:
    netdisk_options, netdisk_map, netdisk_counter = [], {}, {}

if 'selected_netdisks' not in st.session_state:
    st.session_state['selected_netdisks'] = []
selected_nd_labels = st.sidebar.multiselect(
    "网盘类型", netdisk_options,
    default=[f"{nd} ({netdisk_counter[nd]})" for nd in st.session_state['selected_netdisks'] if nd in netdisk_counter]
)
selected_netdisks = [netdisk_map[label] for label in selected_nd_labels]
# 同步session_state
st.session_state['selected_netdisks'] = selected_netdisks

# 关键词模糊搜索（带搜索按钮）
if 'search_query' not in st.session_state:
    st.session_state['search_query'] = ''
_search_input = st.sidebar.text_input(
    "关键词搜索",
    value=st.session_state['search_query'],
    placeholder="标题/描述/频道 模糊匹配",
    key='kw_input'
)
col_sa, col_sb = st.sidebar.columns([1, 1])
with col_sa:
    if st.button("搜索", key="do_search"):
        st.session_state['search_query'] = _search_input.strip()
        st.session_state['page_num'] = 1
        st.rerun()
with col_sb:
    if st.button("清空", key="clear_search"):
        st.session_state['search_query'] = ''
        st.session_state['page_num'] = 1
        st.rerun()
if st.session_state.get('search_query'):
    st.sidebar.caption(f"当前搜索：{st.session_state['search_query']}")

# 在时间范围选择下方展示“按时间范围估算总页数/总条数”（忽略标签/网盘/关键词过滤，仅基于时间与白名单）
@st.cache_data(ttl=60)
def estimate_total_pages_by_time_range(_time_range: str, page_size: int = PAGE_SIZE):
    def _apply_time_filter(q):
        if _time_range == "最近24小时":
            return q.filter(Message.timestamp >= datetime.now() - timedelta(days=1))
        elif _time_range == "最近7天":
            return q.filter(Message.timestamp >= datetime.now() - timedelta(days=7))
        elif _time_range == "最近30天":
            return q.filter(Message.timestamp >= datetime.now() - timedelta(days=30))
        return q
    whitelist_like_local = or_(
        cast(Message.links, String).ilike('%pan.baidu.com/s/%'),
        cast(Message.links, String).ilike('%pan.quark.cn/s/%'),
        cast(Message.links, String).ilike('%aliyundrive.com/s/%'),
        cast(Message.links, String).ilike('%115.com/s/%'),
        cast(Message.links, String).ilike('%pan.xunlei.com/s/%'),
        cast(Message.links, String).ilike('%drive.uc.cn/s/%'),
        cast(Message.links, String).ilike('%www.123pan.com/s/%'),
        cast(Message.links, String).ilike('%www.123684.com/s/%'),
        cast(Message.links, String).ilike('%cloud.189.cn/t/%'),
        cast(Message.links, String).ilike('%caiyun.139.com/w/i/%'),
    )
    try:
        with Session(engine) as session:
            base = session.query(Message.id)
            base = _apply_time_filter(base)
            base = base.filter(Message.links.isnot(None)).filter(whitelist_like_local)
            total_count = base.count()
    except OperationalError:
        engine.dispose()
        try:
            with Session(engine) as session:
                base = session.query(Message.id)
                base = _apply_time_filter(base)
                base = base.filter(Message.links.isnot(None)).filter(whitelist_like_local)
                total_count = base.count()
        except Exception:
            return None, None
    pages = max(1, math.ceil(total_count / page_size)) if total_count else 1
    return total_count, pages

_total_count, _total_pages = estimate_total_pages_by_time_range(time_range, PAGE_SIZE)
if _total_count is not None:
    st.sidebar.caption(f"按时间范围估算：共 {_total_count} 条，约 {_total_pages} 页")
else:
    st.sidebar.caption("按时间范围估算总页数：暂不可用")

# 分页参数（移除重复定义，仅保留页码状态）
if 'page_num' not in st.session_state:
    st.session_state['page_num'] = 1
page_num = st.session_state['page_num']

# 构建查询（服务端分页 + SQL端过滤）
with Session(engine) as session:
    query = session.query(Message)
    # 应用时间范围过滤
    if time_range == "最近24小时":
        query = query.filter(Message.timestamp >= datetime.now() - timedelta(days=1))
    elif time_range == "最近7天":
        query = query.filter(Message.timestamp >= datetime.now() - timedelta(days=7))
    elif time_range == "最近30天":
        query = query.filter(Message.timestamp >= datetime.now() - timedelta(days=30))

    # 仅展示包含白名单网盘链接的消息（SQL 端粗过滤 + JSON 串匹配）
    whitelist_like = or_(
        cast(Message.links, String).ilike('%pan.baidu.com/s/%'),
        cast(Message.links, String).ilike('%pan.quark.cn/s/%'),
        cast(Message.links, String).ilike('%aliyundrive.com/s/%'),
        cast(Message.links, String).ilike('%115.com/s/%'),
        cast(Message.links, String).ilike('%pan.xunlei.com/s/%'),
        cast(Message.links, String).ilike('%drive.uc.cn/s/%'),
        cast(Message.links, String).ilike('%www.123pan.com/s/%'),
        cast(Message.links, String).ilike('%www.123684.com/s/%'),
        cast(Message.links, String).ilike('%cloud.189.cn/t/%'),
        cast(Message.links, String).ilike('%caiyun.139.com/w/i/%'),
    )
    query = query.filter(Message.links.isnot(None)).filter(whitelist_like)

    # 应用标签过滤
    if selected_tags:
        filters = [Message.tags.any(tag) for tag in selected_tags]
        query = query.filter(or_(*filters))
    # 应用关键词模糊搜索（AND 组合多关键词，OR 匹配多个字段）
    _q = st.session_state.get('search_query', '').strip()
    if _q:
        kws = [k for k in _q.split() if k]
        for kw in kws:
            pattern = f"%{kw}%"
            query = query.filter(
                or_(
                    Message.title.ilike(pattern),
                    Message.description.ilike(pattern),
                    Message.channel.ilike(pattern),
                    Message.source.ilike(pattern),
                )
            )

    # 网盘类型：将筛选条件下推到 SQL（避免 Python 侧全量取数）
    if selected_netdisks:
        # 兼容不同来源的网盘类型名称，优先使用域名模式匹配
        type_patterns = {
            '夸克网盘': ['%pan.quark.cn/s/%'],
            '百度网盘': ['%pan.baidu.com/s/%'],
            '阿里云盘': ['%aliyundrive.com/s/%', '%www.aliyundrive.com/s/%', '%www.alipan.com/s/%', '%alipan.com/s/%'],
            '迅雷网盘': ['%pan.xunlei.com/s/%'],
            'UC网盘': ['%drive.uc.cn/s/%'],
            '115网盘': ['%115.com/s/%'],
            '123网盘': ['%www.123pan.com/s/%', '%www.123684.com/s/%'],
            '天翼云盘': ['%cloud.189.cn/t/%'],
            '移动云盘': ['%caiyun.139.com/w/i/%'],
        }
        nd_filters = []
        for nd in selected_netdisks:
            pats = type_patterns.get(nd, [])
            if pats:
                nd_filters.append(or_(*[cast(Message.links, String).ilike(p) for p in pats]))
            # 额外增加对 JSON 文本包含中文键名的兜底匹配
            nd_filters.append(cast(Message.links, String).ilike(f"%{nd}%"))
        query = query.filter(or_(*nd_filters))

    # 基于 LIMIT+1 的分页，避免昂贵的 count()
    if page_num < 1:
        page_num = 1
        st.session_state['page_num'] = 1
    start_idx = (page_num - 1) * PAGE_SIZE
    try:
        rows = (
            query.order_by(Message.timestamp.desc())
            .offset(start_idx)
            .limit(PAGE_SIZE + 1)
            .all()
        )
    except OperationalError:
        engine.dispose()
        rows = []
    has_next = len(rows) > PAGE_SIZE
    messages_page = rows[:PAGE_SIZE]

# 显示消息列表（分页后）
for msg in messages_page:
    # 标题行保留网盘标签，用特殊符号区分
    if msg.links:
        netdisk_tags = " ".join([f"🔵[{name}]" for name in msg.links.keys()])
    else:
        netdisk_tags = ""
    # 数据库现在存储的是北京时间，直接使用即可
    local_ts = msg.timestamp
    expander_title = f"{msg.title} - 🕒{local_ts.strftime('%Y-%m-%d %H:%M:%S')}  {netdisk_tags}"
    with st.expander(expander_title):
        if msg.description:
            st.markdown(msg.description)
        if msg.links:
            link_str = " ".join([
                f"<a href='{link}' target='_blank'><span class='netdisk-tag'>{name}</span></a>"
                for name, link in msg.links.items()
            ])
            st.markdown(link_str, unsafe_allow_html=True)
        # 条目标签标签区（仅展示，不可点击，保留样式）
        if msg.tags:
            tag_html = ""
            for tag in msg.tags:
                tag_html += f"<span class='tag-btn'>#{tag}</span>"
            st.markdown(tag_html, unsafe_allow_html=True)

# 显示分页信息和跳转控件（按钮和页码信息同一行居中）
col1, col2, col3 = st.columns([1,2,1])
with col1:
    if st.button('上一页', disabled=page_num==1, key='prev_page'):
        st.session_state['page_num'] = max(1, page_num-1)
        st.rerun()
with col2:
    hint = "（已到最后一页）" if not has_next else ""
    extra = f" / 约 {_total_pages} 页（按时间范围）" if _total_pages else ""
    st.markdown(f"<div style='text-align:center;line-height:38px;'>当前第 {page_num} 页 {hint}{extra}</div>", unsafe_allow_html=True)
with col3:
    if st.button('下一页', disabled=(not has_next), key='next_page'):
        st.session_state['page_num'] = page_num + 1
        st.rerun()

# 处理点击条目标签筛选
if 'tag_click' in st.session_state and st.session_state['tag_click']:
    tag = st.session_state['tag_click']
    if tag not in st.session_state['selected_tags']:
        st.session_state['selected_tags'].append(tag)
        st.session_state['tag_click'] = None
        st.rerun()
    st.session_state['tag_click'] = None

# 添加自动刷新与说明
st.empty()

# --- 以下保持不变：自动刷新与 CSS ---
st.markdown("---")

REFRESH_CONFIG = "refresh_config.json"

def get_refresh_interval(default: int = 60) -> int:
    try:
        if os.path.exists(REFRESH_CONFIG):
            with open(REFRESH_CONFIG, 'r', encoding='utf-8') as f:
                data = json.load(f)
                val = int(data.get('interval_sec', default))
                return max(10, min(3600, val))
    except Exception:
        pass
    return default

interval = get_refresh_interval()
st.markdown(f"页面每{interval}秒自动刷新一次")

import hashlib as _hashlib

_filter_state = {
    'time_range': time_range,
    'selected_tags': sorted(st.session_state.get('selected_tags', [])),
    'selected_netdisks': sorted(st.session_state.get('selected_netdisks', [])),
    'search_query': st.session_state.get('search_query', ''),
}
_filter_sig = _hashlib.md5(json.dumps(_filter_state, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
_prev_filter_sig = st.session_state.get('filter_sig')
if _prev_filter_sig != _filter_sig:
    st.session_state['page_num'] = 1
    st.session_state['filter_sig'] = _filter_sig
else:
    _ui_state = {
        'time_range': time_range,
        'selected_tags': sorted(st.session_state.get('selected_tags', [])),
        'selected_netdisks': sorted(st.session_state.get('selected_netdisks', [])),
        'page_num': st.session_state.get('page_num', 1),
        'search_query': st.session_state.get('search_query', ''),
    }
    _ui_sig = _hashlib.md5(json.dumps(_ui_state, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
    _prev_ui_sig = st.session_state.get('ui_sig')
    if _prev_ui_sig != _ui_sig:
        st.session_state['ui_sig'] = _ui_sig
    else:
        import time as _time
        _time.sleep(interval)
        st.rerun()

st.markdown(
    """
    <style>
    .tag-btn { display:inline-block; margin: 2px 6px 2px 0; padding: 2px 8px; background:#f1f5f9; border-radius: 12px; color:#0f172a; font-size:12px; }
    .netdisk-tag { display:inline-block; margin: 2px 6px 2px 0; padding: 2px 8px; background:#ecfeff; border-radius: 12px; color:#155e75; font-size:12px; border:1px solid #a5f3fc; }
    </style>
    """,
    unsafe_allow_html=True,
)