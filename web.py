import streamlit as st
from sqlalchemy.orm import Session
from model import Message, engine
import pandas as pd
from datetime import datetime, timedelta, timezone
from collections import Counter
from sqlalchemy import or_, cast, String
import json
import os

# 初始化session_state用于标签筛选
if 'selected_tags' not in st.session_state:
    st.session_state['selected_tags'] = []

st.set_page_config(
    page_title="TG频道监控",
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
    with Session(engine) as session:
        all_tags = session.query(Message.tags).all()
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

# 网盘类型筛选（基于实际数据库中的类型）
netdisk_types = ['夸克网盘', '百度网盘', '阿里云盘', '迅雷网盘', 'UC网盘', '115网盘', '123网盘', '天翼云盘', '移动云盘']
selected_netdisks = st.sidebar.multiselect("网盘类型", netdisk_types)

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

# 分页参数
PAGE_SIZE = 50
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

    # 网盘类型筛选优化：只有选择了网盘类型时才使用Python过滤
    if selected_netdisks:
        # 先获取当前条件下的消息进行Python过滤
        all_messages = query.order_by(Message.timestamp.desc()).all()
        
        filtered_messages = []
        for msg in all_messages:
            if isinstance(msg.links, dict) and any(nd in msg.links.keys() for nd in selected_netdisks):
                filtered_messages.append(msg)
        
        total_count = len(filtered_messages)
        max_page = (total_count + PAGE_SIZE - 1) // PAGE_SIZE if total_count else 1
        if page_num < 1:
            page_num = 1
        if page_num > max_page:
            page_num = max_page
            st.session_state['page_num'] = page_num
        
        start_idx = (page_num - 1) * PAGE_SIZE
        end_idx = start_idx + PAGE_SIZE
        messages_page = filtered_messages[start_idx:end_idx]
    else:
        total_count = query.order_by(None).count()
        max_page = (total_count + PAGE_SIZE - 1) // PAGE_SIZE if total_count else 1
        if page_num < 1:
            page_num = 1
        if page_num > max_page:
            page_num = max_page
            st.session_state['page_num'] = page_num
        
        start_idx = (page_num - 1) * PAGE_SIZE
        messages_page = query.order_by(Message.timestamp.desc()).offset(start_idx).limit(PAGE_SIZE).all()

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
if max_page > 1:
    col1, col2, col3 = st.columns([1,2,1])
    with col1:
        if st.button('上一页', disabled=page_num==1, key='prev_page'):
            st.session_state['page_num'] = max(1, page_num-1)
            st.rerun()
    with col2:
        st.markdown(f"<div style='text-align:center;line-height:38px;'>共 {total_count} 条，当前第 {page_num} / {max_page} 页</div>", unsafe_allow_html=True)
    with col3:
        if st.button('下一页', disabled=page_num==max_page, key='next_page'):
            st.session_state['page_num'] = min(max_page, page_num+1)
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
st.markdown("---")

# 从配置文件读取刷新间隔，默认60秒
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

# 交互无阻塞刷新：当筛选或分页变化时，跳过sleep，立即完成本次渲染
import hashlib as _hashlib

# 仅用于判断筛选是否变化（不含分页），变化时重置到第1页
_filter_state = {
    'time_range': time_range,
    'selected_tags': sorted(st.session_state.get('selected_tags', [])),
    'selected_netdisks': sorted(selected_netdisks),
    'search_query': st.session_state.get('search_query', ''),
}
_filter_sig = _hashlib.md5(json.dumps(_filter_state, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
_prev_filter_sig = st.session_state.get('filter_sig')
if _prev_filter_sig != _filter_sig:
    # 筛选条件发生变化，重置分页并记录签名
    st.session_state['page_num'] = 1
    st.session_state['filter_sig'] = _filter_sig
    # 本次为交互变更，直接返回（不sleep），让界面立即更新
    # 注意：Streamlit会在下一次空闲渲染时再进入自动刷新
else:
    # 用于判断交互是否发生（含分页在内的任何变化），变化时不sleep
    _ui_state = {
        'time_range': time_range,
        'selected_tags': sorted(st.session_state.get('selected_tags', [])),
        'selected_netdisks': sorted(selected_netdisks),
        'page_num': st.session_state.get('page_num', 1),
        'search_query': st.session_state.get('search_query', ''),
    }
    _ui_sig = _hashlib.md5(json.dumps(_ui_state, ensure_ascii=False, sort_keys=True).encode('utf-8')).hexdigest()
    _prev_ui_sig = st.session_state.get('ui_sig')
    if _prev_ui_sig != _ui_sig:
        st.session_state['ui_sig'] = _ui_sig
        # 本次为交互变更，直接返回（不sleep）
    else:
        # 无交互发生，进入自动拉取模式：sleep后自动重跑
        import time as _time
        _time.sleep(interval)
        st.rerun()

# 添加全局CSS，强力覆盖expander内容区的gap，只保留一处，放在文件最后
st.markdown("""
    <style>
    [data-testid=\"stExpander\"] [data-testid=\"stExpanderContent\"] {
        gap: 0.2rem !important;
    }
    div[data-testid=\"stExpanderContent\"] {
        gap: 0.2rem !important;
    }
    [data-testid=\"stExpander\"] * {
        gap: 0.2rem !important;
    }
    .netdisk-tag {
        display: inline-block;
        background: #e6f0fa;
        color: #409eff;
        border-radius: 12px;
        padding: 2px 10px;
        margin: 2px 4px 2px 0;
        font-size: 13px;
    }
    .tag-btn {
        border:1px solid #222;
        border-radius:8px;
        padding:4px 16px;
        margin:2px 6px 2px 0;
        font-size:15px;
        background:#fff;
        color:#222;
        display:inline-block;
        transition: background 0.2s, color 0.2s;
        cursor: default;
    }
    .tag-btn:hover {
        background: #fff;
        color: #222;
    }
    </style>
""", unsafe_allow_html=True)