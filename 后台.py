import streamlit as st
from sqlalchemy.orm import Session
from model import Credential, Channel, engine, TelegramConfig
from datetime import datetime
import json
import os
from config import settings
import re

st.set_page_config(page_title="后台管理", page_icon="🔧", layout="wide")
st.title("后台管理")

# 缓存与分页常量
@st.cache_data(ttl=300)
def get_telegram_cfg():
    with Session(engine) as session:
        cfg = session.query(TelegramConfig).first()
        if not cfg:
            return {"string_session": "", "updated_at": None}
        return {"string_session": (cfg.string_session or ""), "updated_at": cfg.updated_at}

@st.cache_data(ttl=300)
def get_credentials():
    with Session(engine) as session:
        rows = session.query(Credential).all()
        return [(c.id, c.api_id, c.api_hash) for c in rows]

@st.cache_data(ttl=300)
def get_channels():
    with Session(engine) as session:
        rows = session.query(Channel).all()
        return [(c.id, c.username) for c in rows]

RULES_PAGE_SIZE = 50

# 🔑 Telegram 身份验证配置（StringSession 优先）
st.header("Telegram 身份验证配置")
# 使用缓存读取当前配置
cfg_data = get_telegram_cfg()
current_string = cfg_data.get('string_session', '')

# 在渲染组件之前，初始化和处理清空标记
if 'string_session_input' not in st.session_state:
    st.session_state['string_session_input'] = current_string or ''
if st.session_state.get('clear_string_session_input', False):
    st.session_state['string_session_input'] = ''
    st.session_state['clear_string_session_input'] = False

# 使用 key 绑定，不再传 value，避免与 session_state 冲突
new_string = st.text_area(
    "StringSession（可选）",
    height=100,
    help="填写后将优先使用 StringSession 进行身份验证。不填则回退为本地 session 文件。",
    key="string_session_input",
)
col1, col2 = st.columns([1, 6])
with col1:
    if st.button("保存配置"):
        with Session(engine) as session:
            cfg = session.query(TelegramConfig).first()
            if cfg:
                cfg.string_session = new_string.strip() if new_string.strip() else None
                cfg.updated_at = datetime.utcnow()
            else:
                cfg = TelegramConfig(string_session=new_string.strip() if new_string.strip() else None)
                session.add(cfg)
            # 自动补充当前环境中的 API 凭据到 Credential（若不存在）
            try:
                api_id_str = str(settings.TELEGRAM_API_ID)
                api_hash_str = settings.TELEGRAM_API_HASH
                exists = session.query(Credential).filter_by(api_id=api_id_str, api_hash=api_hash_str).first()
                if not exists:
                    session.add(Credential(api_id=api_id_str, api_hash=api_hash_str))
            except Exception as e:
                st.warning(f"同步 API 凭据失败: {e}")
            session.commit()
        # 变更后清理缓存并刷新
        try:
            get_telegram_cfg.clear()
            get_credentials.clear()
        except Exception:
            pass
        st.session_state['clear_string_session_input'] = True
        st.rerun()
with col2:
    if st.button("清空配置"):
        with Session(engine) as session:
            cfg = session.query(TelegramConfig).first()
            if cfg:
                cfg.string_session = None
                cfg.updated_at = datetime.utcnow()
                session.commit()
        # 变更后清理缓存并刷新
        try:
            get_telegram_cfg.clear()
        except Exception:
            pass
        st.session_state['clear_string_session_input'] = True
        st.rerun()

st.markdown("---")

# 🕒 首页自动刷新频率设置（秒）
CONFIG_FILE = "refresh_config.json"

def load_refresh_interval(default: int = 60) -> int:
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                val = int(data.get('interval_sec', default))
                return max(10, min(3600, val))
    except Exception:
        pass
    return default

def save_refresh_interval(val: int):
    try:
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump({"interval_sec": int(val)}, f, ensure_ascii=False)
    except Exception as e:
        st.error(f"保存失败: {e}")

st.header("首页自动刷新频率")
current_interval = load_refresh_interval()
new_interval = st.number_input("刷新频率（秒）", min_value=10, max_value=3600, step=10, value=current_interval, help="用于前台 web.py 首页的自动刷新间隔。")
if st.button("保存刷新频率"):
    save_refresh_interval(int(new_interval))
    st.success(f"已保存刷新频率为 {int(new_interval)} 秒。前台页面将按新频率刷新。")

st.markdown("---")

# API凭据管理（可选：保留以便切换为非 StringSession 模式）
st.header("API凭据管理")

# 在凭据管理中展示当前 StringSession（掩码显示，可切换明文）
cfg_data = get_telegram_cfg()
cur_ss = (cfg_data.get('string_session') or None)
updated_at = cfg_data.get('updated_at')

if cur_ss:
    def _mask(s: str) -> str:
        return s if len(s) <= 12 else f"{s[:6]}...{s[-6:]}"
    with st.expander("当前 StringSession", expanded=True):
        show_plain = st.checkbox("显示明文", value=False, key="show_plain_ss")
        st.text(cur_ss if show_plain else _mask(cur_ss))
        if updated_at:
            st.caption(f"最后更新时间: {updated_at}")
else:
    st.info("当前未配置 StringSession。可在上方“Telegram 身份验证配置”中保存。")

# 使用缓存读取凭据列表
creds = get_credentials()
for cred_id, api_id_val, api_hash_val in creds:
    col1, col2, col3 = st.columns([3, 5, 2])
    col1.write(f"api_id: {api_id_val}")
    col2.write(f"api_hash: {api_hash_val}")
    if col3.button(f"删除", key=f"del_cred_{cred_id}"):
        with Session(engine) as session:
            obj = session.query(Credential).get(cred_id)
            if obj:
                session.delete(obj)
                session.commit()
        try:
            get_credentials.clear()
        except Exception:
            pass
        st.rerun()
st.markdown("---")
with st.form("add_cred_form"):
    api_id = st.text_input("新API ID")
    api_hash = st.text_input("新API Hash")
    submitted = st.form_submit_button("添加API凭据")
    if submitted and api_id and api_hash:
        with Session(engine) as session:
            session.add(Credential(api_id=api_id, api_hash=api_hash))
            session.commit()
        try:
            get_credentials.clear()
        except Exception:
            pass
        st.success("添加成功！")
        st.rerun()

# 频道管理
st.header("监听频道管理")
chans = get_channels()
for chan_id, chan_username in chans:
    col1, col2 = st.columns([6, 2])
    col1.write(f"频道: {chan_username}")
    if col2.button(f"删除", key=f"del_chan_{chan_id}"):
        with Session(engine) as session:
            obj = session.query(Channel).get(chan_id)
            if obj:
                session.delete(obj)
                session.commit()
        # 触发监控端刷新
        try:
            with open("channels_refresh.flag", "w") as f:
                f.write("refresh")
        except Exception as e:
            st.warning(f"触发刷新失败: {e}")
        try:
            get_channels.clear()
        except Exception:
            pass
        st.rerun()
st.markdown("---")
with st.form("add_chan_form"):
    username = st.text_input("新频道用户名（不加@）")
    submitted = st.form_submit_button("添加频道")
    if submitted:
        uname = (username or "").strip().lstrip('@')
        if not uname:
            st.warning("频道用户名不能为空")
        else:
            with Session(engine) as session:
                exists = session.query(Channel).filter_by(username=uname).first()
                if exists:
                    st.info("该频道已存在，无需重复添加")
                else:
                    session.add(Channel(username=uname))
                    session.commit()
                    st.success("添加成功！")
                    # 触发监控端刷新
                    try:
                        with open("channels_refresh.flag", "w") as f:
                            f.write("refresh")
                    except Exception as e:
                        st.warning(f"触发刷新失败: {e}")
                    try:
                        get_channels.clear()
                    except Exception:
                        pass
                    st.rerun()

st.markdown("---")

# 频道规则管理
st.header("频道规则管理")
from model import ChannelRule
NETDISK_OPTIONS = ['夸克网盘', '阿里云盘', '百度网盘', '115网盘', '天翼云盘', '123云盘', 'UC网盘', '迅雷']

with Session(engine) as session:
    chan_list = [u for _, u in get_channels()]
    if not chan_list:
        st.info("请先在上方添加至少一个频道")
    else:
        # 在渲染选择框之前，处理“载入编辑”的待应用值，避免组件创建后再改写同 key
        if 'rule_sel_chan_pending' in st.session_state:
            st.session_state['rule_sel_chan'] = st.session_state.pop('rule_sel_chan_pending')
        if 'rule_sel_chan' not in st.session_state:
            st.session_state['rule_sel_chan'] = chan_list[0]

        colL, colR = st.columns([3,5])
        with colL:
            sel_chan = st.selectbox("选择频道", options=chan_list, key="rule_sel_chan")
        with colR:
            existing = session.query(ChannelRule).filter_by(channel=sel_chan).first()
            cur_netdisks = existing.exclude_netdisks if existing else []
            cur_keywords = ",".join(existing.exclude_keywords) if (existing and existing.exclude_keywords) else ""
            cur_tags = ",".join(existing.exclude_tags) if (existing and existing.exclude_tags) else ""
            cur_enabled = existing.enabled if existing else True

        with st.form("rule_form"):
            ex_netdisks = st.multiselect("排除的网盘类型", NETDISK_OPTIONS, default=cur_netdisks)
            ex_keywords = st.text_input("排除的关键词（逗号分隔）", value=cur_keywords)
            ex_tags = st.text_input("排除的标签（逗号分隔，不含#）", value=cur_tags)
            enabled = st.checkbox("启用该规则", value=cur_enabled)
            submitted = st.form_submit_button("保存规则")
            if submitted:
                kws = [s.strip() for s in ex_keywords.split(',') if s.strip()]
                tags = [s.strip().lstrip('#') for s in ex_tags.split(',') if s.strip()]
                if existing:
                    existing.exclude_netdisks = ex_netdisks
                    existing.exclude_keywords = kws
                    existing.exclude_tags = tags
                    existing.enabled = enabled
                else:
                    session.add(ChannelRule(channel=sel_chan, exclude_netdisks=ex_netdisks, exclude_keywords=kws, exclude_tags=tags, enabled=enabled))
                session.commit()
                st.success("已保存规则")
                # 触发规则刷新
                try:
                    with open("rules_refresh.flag", "w") as f:
                        f.write("refresh")
                except Exception as e:
                    st.warning(f"触发规则刷新失败: {e}")
                st.rerun()
        # 删除规则
        if existing and st.button("删除该频道规则"):
            session.delete(existing)
            session.commit()
            try:
                with open("rules_refresh.flag", "w") as f:
                    f.write("refresh")
            except Exception as e:
                st.warning(f"触发规则刷新失败: {e}")
            st.success("已删除规则")
            st.rerun()

        # 展示所有已添加规则及其对应频道（分页）
        st.markdown("---")
        st.subheader("已配置规则列表")
        # 初始化分页状态
        if 'rules_page_num' not in st.session_state:
            st.session_state['rules_page_num'] = 1
        rules_page_num = st.session_state['rules_page_num']

        total_rules = session.query(ChannelRule).count()
        max_rules_page = (total_rules + RULES_PAGE_SIZE - 1) // RULES_PAGE_SIZE if total_rules else 1
        if rules_page_num < 1:
            rules_page_num = 1
        if rules_page_num > max_rules_page:
            rules_page_num = max_rules_page
            st.session_state['rules_page_num'] = rules_page_num
        start_idx = (rules_page_num - 1) * RULES_PAGE_SIZE
        page_rules = session.query(ChannelRule).order_by(ChannelRule.updated_at.desc()).offset(start_idx).limit(RULES_PAGE_SIZE).all()

        if not total_rules:
            st.caption("暂无规则")
        else:
            for r in page_rules:
                title_status = "✅ 启用" if r.enabled else "⛔ 禁用"
                with st.expander(f"{r.channel} · {title_status}", expanded=False):
                    st.write(f"- 排除网盘类型: {', '.join(r.exclude_netdisks or []) if r.exclude_netdisks else '（无）'}")
                    st.write(f"- 排除关键词: {', '.join(r.exclude_keywords or []) if r.exclude_keywords else '（无）'}")
                    st.write(f"- 排除标签: {', '.join(r.exclude_tags or []) if r.exclude_tags else '（无）'}")
                    if r.updated_at:
                        st.caption(f"最后更新: {r.updated_at}")
                    cols = st.columns([1,1,3])
                    with cols[0]:
                        if st.button("载入编辑", key=f"load_rule_{r.id}"):
                            # 不直接改写 rule_sel_chan，先写入 pending，下一次渲染前再应用
                            st.session_state['rule_sel_chan_pending'] = r.channel
                            st.rerun()
                    with cols[1]:
                        if st.button("删除", key=f"delete_rule_{r.id}"):
                            session.delete(r)
                            session.commit()
                            try:
                                with open("rules_refresh.flag", "w") as f:
                                    f.write("refresh")
                            except Exception as e:
                                st.warning(f"触发规则刷新失败: {e}")
                            st.success("已删除该规则")
                            st.rerun()

            # 分页控件
            colp1, colp2, colp3 = st.columns([1,2,1])
            with colp1:
                if st.button('上一页', disabled=rules_page_num==1, key='rules_prev_page'):
                    st.session_state['rules_page_num'] = max(1, rules_page_num-1)
                    st.rerun()
            with colp2:
                st.markdown(f"<div style='text-align:center;line-height:38px;'>共 {total_rules} 条，当前第 {rules_page_num} / {max_rules_page} 页</div>", unsafe_allow_html=True)
            with colp3:
                if st.button('下一页', disabled=rules_page_num==max_rules_page, key='rules_next_page'):
                    st.session_state['rules_page_num'] = min(max_rules_page, rules_page_num+1)
                    st.rerun()

# ▶️⏸ 监控开关（无重启）
st.header("监控运行控制（无重启）")
CONTROL_FILE = "monitor_control.json"

def read_paused():
    try:
        if os.path.exists(CONTROL_FILE):
            import json
            with open(CONTROL_FILE, "r", encoding="utf-8") as f:
                return bool(json.load(f).get("paused", False))
    except Exception:
        pass
    return False

paused = read_paused()
colp, colr = st.columns([1,2])
with colp:
    st.write(f"当前状态：{'⏸ 已暂停' if paused else '▶️ 运行中'}")
with colr:
    c1, c2 = st.columns(2)
    if c1.button("暂停监控", disabled=paused):
        try:
            with open(CONTROL_FILE, "w", encoding="utf-8") as f:
                json.dump({"paused": True}, f, ensure_ascii=False)
            st.success("已暂停（无需重启）")
        except Exception as e:
            st.error(f"操作失败: {e}")
        st.rerun()
    if c2.button("恢复监控", disabled=not paused):
        try:
            with open(CONTROL_FILE, "w", encoding="utf-8") as f:
                json.dump({"paused": False}, f, ensure_ascii=False)
            st.success("已恢复（无需重启）")
        except Exception as e:
            st.error(f"操作失败: {e}")
        st.rerun()

st.markdown("---")

# 复用前端/监控的严格网盘白名单与清洗逻辑（后台侧兜底）
STRICT_NETDISK_PATTERNS = {
    "百度网盘": r"https://pan\.baidu\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?",
    "夸克网盘": r"https://pan\.quark\.cn/s/[A-Za-z0-9_-]+",
    "阿里云盘": r"https://www\.aliyundrive\.com/s/[A-Za-z0-9_-]+",
    "115网盘": r"https://115\.com/s/[A-Za-z0-9_-]+",
    "迅雷网盘": r"https://pan\.xunlei\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?(?:#)?",
    "UC网盘": r"https://drive\.uc\.cn/s/[A-Za-z0-9]+(?:\?public=1)?",
    "123网盘": r"https://www\.123pan\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?|https://www\.123684\.com/s/[A-Za-z0-9_-]+(?:\?pwd=[A-Za-z0-9]+)?",
    "天翼云盘": r"https://cloud\.189\.cn/t/[A-Za-z0-9]+",
    "移动云盘": r"https://caiyun\.139\.com/w/i/[A-Za-z0-9]+",
}

_NOISE_LINES = re.compile(r"^(?:[\uD800-\uDBFF\uDC00-\uDFFF\U00010000-\U0010ffff\W]{0,3})\s*(?:来自|来 自|频道|频 道|群组|群 组|投稿|搜资源)\s*[:：].*$", re.IGNORECASE)
_HANDLE = re.compile(r"@\w+")

def extract_netdisk_links_strict(text: str) -> dict:
    links = {}
    for name, pattern in STRICT_NETDISK_PATTERNS.items():
        m = re.findall(pattern, text or '')
        if m:
            links[name] = m[0] if isinstance(m, list) else m
    return links

def clean_channel_noise(text: str) -> str:
    lines = [ln for ln in (text or '').split('\n')]
    cleaned = []
    for ln in lines:
        lns = ln.strip()
        if not lns:
            continue
        if _NOISE_LINES.match(lns):
            continue
        lns = _HANDLE.sub('', lns)
        lns = re.sub(r"\s{2,}", " ", lns).strip()
        if lns:
            cleaned.append(lns)
    return '\n'.join(cleaned)

st.info("后台已启用网盘白名单与频道署名清洗兜底：非白名单网盘链接或仅含推广署名的内容不会被写入数据库。")
# 若此文件存在创建消息的入口，请确保在写入前调用：
# text = clean_channel_noise(text)
# links = extract_netdisk_links_strict(text)
# if not links: st.warning("未检测到白名单网盘链接，已忽略写入")
# else: 正常构造 Message(...) 并提交