# routes_forward.py  (VERSION = 2025-08-29T11:40Z polling)
# 多群→多群；按 Google Sheet 控制；相册合并；匿名管理员/群身份(sender_chat)。
# 文本/媒体优先 copyMessage（无 forward 徽章），失败回退 sendMessage/send<Media>。
# ✅ routes 表按钮：btn1_text/btn1_url … btn3_text/btn3_url（空对忽略；至少一对就出按钮）
# ✅ 诊断接口：/version /diag /diag_buttons /diag_stats /routes_refresh
# ✅ 新增：长轮询 getUpdates（无需公网 HTTPS）。设置环境变量 BOT_POLLING=1 即启用。
#    webhook 与 polling 共享同一处理逻辑 handle_update(update)

import os, re, time, threading, logging
from typing import Dict, List, Any, Optional, Tuple
from flask import Flask, request, jsonify, request as flask_request
import requests

VERSION = "routes_forward/2025-08-29T11:40Z-polling"

# ========= 可配项 =========
BOT_TOKEN = os.getenv("BOT_TOKEN", "7922193485:AAE2-TkKgW-AP9kJFsU-65enVr0yp1PoFlA")
SHEET_ID  = os.getenv("SHEET_ID",  "1k62SfELYmpN-Alo1UxBdc3muBCfcFYAIhm6EJIB6pnI")
SHEET_TAB = os.getenv("SHEET_TAB", "routes")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "/home/IDTETE/service_account.json")

ALBUM_FLUSH_MS    = int(os.getenv("ALBUM_FLUSH_MS", "1200"))
ROUTE_REFRESH_SEC = int(os.getenv("ROUTE_REFRESH_SEC", "60"))

ALWAYS_SEND_TEXT_VIA_SENDMESSAGE = False
DEDUP_SAME_TARGET = True
DEDUP_TTL_SEC     = 600

MEDIA_WHITELIST_KEYS = ("text", "caption", "photo", "video", "document", "animation", "audio", "voice")
TG_MAX_RETRIES = 4

BOT_USERNAME = os.getenv("BOT_USERNAME", "exfunbot")

# ✅ 新增：是否启用长轮询
BOT_POLLING = os.getenv("BOT_POLLING", "0").lower() in ("1","true","yes","y")

# ============================================================

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
app.logger = logging.getLogger("routes_forward")

API = f"https://api.telegram.org/bot{BOT_TOKEN}"
BOT_ID = int(BOT_TOKEN.split(":")[0]) if ":" in BOT_TOKEN else None

# ---------------- Telegram API（含429退避） ----------------
def tg(method: str, **data):
    url = f"{API}/{method}"
    attempt = 0
    while True:
        attempt += 1
        try:
            r = requests.post(url, json=data, timeout=60)
            try:
                out = r.json()
            except Exception:
                out = {"ok": False, "text": r.text}
            if not out.get("ok") and out.get("error_code") == 429:
                retry_after = int((out.get("parameters") or {}).get("retry_after", 1))
                app.logger.warning(f"TG 429 {method}, retry_after={retry_after}s, attempt={attempt}")
                if attempt < TG_MAX_RETRIES:
                    time.sleep(retry_after + 0.2); continue
            if not out.get("ok"):
                app.logger.warning(f"TG {method} fail: {out}")
            return out
        except Exception as e:
            app.logger.error(f"TG {method} exception: {e}")
            if attempt < TG_MAX_RETRIES:
                time.sleep(min(2 ** attempt, 10)); continue
            return {"ok": False, "error": str(e)}

# ---------------- Google Sheet 读取 ----------------
import gspread
from oauth2client.service_account import ServiceAccountCredentials

_sheet_lock = threading.Lock()
_routes_cache: List[dict] = []
_routes_cache_ts = 0

# 规整表头
NAME_CLEAN_RE = re.compile(r"[^a-z0-9_]")
def _normalize_keys(d: dict) -> dict:
    out = {}
    for k, v in d.items():
        nk = str(k).strip().lower().replace(" ", "_")
        nk = NAME_CLEAN_RE.sub("", nk)
        out[nk] = v
    return out

def _build_reply_markup_from_row(row: dict) -> Optional[dict]:
    r = _normalize_keys(row)
    pairs = []
    for i in (1, 2, 3):
        t = str(r.get(f"btn{i}_text", "") or "").strip()
        u = str(r.get(f"btn{i}_url", "") or "").strip()
        if t and u:
            pairs.append({"text": t, "url": u})
    if pairs:
        return {"inline_keyboard": [pairs]}
    return None

def _load_routes_from_sheet() -> List[dict]:
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    ws = client.open_by_key(SHEET_ID).worksheet(SHEET_TAB)
    rows = ws.get_all_records()
    routes: List[dict] = []
    with_btn = 0
    for r in rows:
        try:
            rn = _normalize_keys(r)
            if str(rn.get("enabled", "true")).strip().lower() not in ("true","1","yes","y"):
                continue

            source_chat_id = int(str(rn.get("source_chat_id")).strip())

            # --- 关键兼容：0 => ANY（源）
            stid_raw = str(rn.get("source_thread_id", "")).strip().upper()
            if stid_raw in ("", "NONE"):
                source_thread_id = None
            elif stid_raw in ("0", "ANY"):
                source_thread_id = "ANY"
            else:
                source_thread_id = int(stid_raw)

            target_chat_id = int(str(rn.get("target_chat_id")).strip())

            # --- 关键兼容：0 => 跟随源话题（目标）
            ttid_raw = str(rn.get("target_thread_id", "")).strip().upper()
            if ttid_raw in ("", "NONE", "NULL"):
                target_thread_id = None
            elif ttid_raw == "0":
                target_thread_id = -1    # 内部标记：跟随源话题
            else:
                target_thread_id = int(ttid_raw)

            delay_seconds = int(str(rn.get("delay_seconds", "0")).strip() or "0")

            reply_markup = _build_reply_markup_from_row(r)
            if reply_markup: with_btn += 1

            routes.append({
                "source_chat_id": source_chat_id,
                "source_thread_id": source_thread_id,
                "target_chat_id": target_chat_id,
                "target_thread_id": target_thread_id,
                "delay_seconds": delay_seconds,
                "reply_markup": reply_markup,
            })
        except Exception as e:
            app.logger.warning(f"[routes] skip bad row {r}: {e}")
    app.logger.info(f"[routes] loaded {len(routes)} rules; with_buttons={with_btn}")
    return routes


def get_routes() -> List[dict]:
    global _routes_cache, _routes_cache_ts
    now = time.time()
    with _sheet_lock:
        if now - _routes_cache_ts > ROUTE_REFRESH_SEC or not _routes_cache:
            try:
                _routes_cache = _load_routes_from_sheet()
                _routes_cache_ts = now
            except Exception as e:
                app.logger.error(f"[routes] load error: {e}")
    return _routes_cache

def match_routes(source_chat_id: int, source_thread_id: Optional[int]) -> List[dict]:
    rules = get_routes()
    matched: List[dict] = []
    for r in rules:
        if r["source_chat_id"] != source_chat_id:
            continue
        stid_rule = r["source_thread_id"]
        if stid_rule == "ANY":
            matched.append(r)
        elif stid_rule is None:
            if source_thread_id is None:
                matched.append(r)
        else:
            if source_thread_id == stid_rule:
                matched.append(r)
    return matched

def _routes_autorefresh_loop():
    global _routes_cache, _routes_cache_ts
    while True:
        try:
            new_routes = _load_routes_from_sheet()
            with _sheet_lock:
                _routes_cache = new_routes
                _routes_cache_ts = time.time()
            app.logger.info(f"[routes] auto-poll refreshed: {len(new_routes)} rules")
        except Exception as e:
            app.logger.error(f"[routes] auto-poll error: {e}")
        time.sleep(max(5, ROUTE_REFRESH_SEC))

# ---------------- 过滤器 ----------------
SERVICE_FIELDS = (
    "new_chat_members","left_chat_member","new_chat_member","pinned_message",
    "group_chat_created","supergroup_chat_created","channel_chat_created",
    "migrate_to_chat_id","migrate_from_chat_id","successful_payment",
    "connected_website","message_auto_delete_timer_changed","boost_added","boost_removed"
)

def is_service_message(msg: dict) -> bool:
    return any(f in msg for f in SERVICE_FIELDS)

def is_human_message(msg: dict) -> bool:
    if BOT_ID and msg.get("from") and int(msg["from"].get("id", 0)) == BOT_ID:
        return False
    if msg.get("sender_chat"):
        if msg.get("via_bot"):
            return False
        return True
    if msg.get("from") and msg["from"].get("is_bot"):
        return False
    if msg.get("via_bot"):
        return False
    return True

def has_allowed_content(msg: dict) -> bool:
    for k in MEDIA_WHITELIST_KEYS:
        if k in msg and msg[k]:
            return True
    return False

# ---------------- 延迟调度 ----------------
def schedule_after_delay(delay_sec: int, fn, *args, **kwargs):
    if int(delay_sec or 0) <= 0:
        try: fn(*args, **kwargs)
        except Exception as e: app.logger.error(f"run immediate task error: {e}")
        return None
    t = threading.Timer(max(0, delay_sec), fn, args=args, kwargs=kwargs)
    t.daemon = True; t.start(); return t

# ---------------- 去重缓存 ----------------
_dedup_lock = threading.Lock()
_dedup: Dict[Tuple[int,int,int,int], float] = {}

def _dedup_hit(from_chat: int, msg_id: int, tgt_chat: int, tgt_thread: Optional[int]) -> bool:
    if not DEDUP_SAME_TARGET: return False
    key = (from_chat, msg_id, tgt_chat, tgt_thread or 0)
    now = time.time()
    with _dedup_lock:
        for k, ts in list(_dedup.items()):
            if now - ts > DEDUP_TTL_SEC: _dedup.pop(k, None)
        if key in _dedup: return True
        _dedup[key] = now
    return False

# ---------------- 相册收集器 ----------------
album_lock = threading.Lock()
album_cache: Dict[str, Dict[str, Any]] = {}

def _album_key(chat_id: int, mgid: str) -> str: return f"{chat_id}:{mgid}"

def build_input_media(item: dict) -> Optional[dict]:
    if "photo" in item:
        photos = item["photo"]; fid = photos[-1]["file_id"] if photos else None
        if not fid: return None
        media = {"type":"photo","media":fid}
        if item.get("caption"): media["caption"] = item["caption"]
        if item.get("caption_entities"): media["caption_entities"] = item["caption_entities"]
        return media
    if "video" in item:
        fid = item["video"]["file_id"]; media = {"type":"video","media":fid}
        if item.get("caption"): media["caption"] = item["caption"]
        if item.get("caption_entities"): media["caption_entities"] = item["caption_entities"]
        return media
    if "document" in item:
        fid = item["document"]["file_id"]; media = {"type":"document","media":fid}
        if item.get("caption"): media["caption"] = item["caption"]
        if item.get("caption_entities"): media["caption_entities"] = item["caption_entities"]
        return media
    return None

def _dispatch_album_send_for_rule(input_medias, reply_markup, rule: dict, source_thread_id: Optional[int]):
    tgt = rule["target_chat_id"]
    tgt_thread = rule.get("target_thread_id")
    if tgt_thread == -1:  # 跟随源话题
        tgt_thread = source_thread_id

    params = {"chat_id": tgt, "media": input_medias}
    if tgt_thread:
        params["message_thread_id"] = tgt_thread
    tg("sendMediaGroup", **params)

    if reply_markup:
        btn = {"chat_id": tgt, "text": "🔗", "reply_markup": reply_markup}
        if tgt_thread:
            btn["message_thread_id"] = tgt_thread
        tg("sendMessage", **btn)


def flush_album(key: str):
    with album_lock:
        entry = album_cache.pop(key, None)
    if not entry:
        return

    items = entry["items"]
    entry_reply_markup = entry.get("reply_markup")
    source_thread_id = entry.get("thread_id")
    source_chat_id   = entry.get("chat_id")

    # 正确构建 media 列表（只保留第一条的 caption）
    input_medias: List[dict] = []
    first_caption_used = False
    for msg in items:
        m = build_input_media(msg)
        if not m:
            continue
        if "caption" in m:
            if first_caption_used:
                m.pop("caption", None)
                m.pop("caption_entities", None)
            else:
                first_caption_used = True
        input_medias.append(m)

    rules = match_routes(source_chat_id, source_thread_id)
    if not rules:
        app.logger.info(f"[album:{key}] no routes matched");
        return

    seen: set = set()
    for r in rules:
        tgt = (r["target_chat_id"], r.get("target_thread_id") or 0)
        if DEDUP_SAME_TARGET and tgt in seen:
            continue
        seen.add(tgt)
        delay_sec = int(r.get("delay_seconds", 0) or 0)
        rm = r.get("reply_markup") or entry_reply_markup
        schedule_after_delay(delay_sec, _dispatch_album_send_for_rule, input_medias, rm, r, source_thread_id)


def schedule_album_flush(key: str):
    def _run():
        try: flush_album(key)
        except Exception as e: app.logger.error(f"flush_album error: {e}")
    t = threading.Timer(ALBUM_FLUSH_MS/1000.0, _run); t.daemon = True; t.start(); return t

def ingest_album_piece(message: dict):
    mgid = message.get("media_group_id")
    if not mgid: return False
    chat_id = int(message["chat"]["id"]); key = _album_key(chat_id, mgid)
    with album_lock:
        entry = album_cache.get(key)
        if not entry:
            entry = {"items": [], "first_ts": time.time(), "timer": schedule_album_flush(key),
                     "reply_markup": message.get("reply_markup"),
                     "thread_id": message.get("message_thread_id"), "chat_id": chat_id}
            album_cache[key] = entry
        if any(k in message for k in ("photo","video","document")):
            entry["items"].append(message)
    app.logger.info(f"[album] buffer chat={chat_id} mgid={mgid} size={len(album_cache[key]['items'])}")
    return True

# ---------------- 链接提取 ----------------
A_TAG_RE = re.compile(r'<a\s+href=["\'](https?://[^"\']+)["\']\s*>(.*?)</a>', re.I | re.S)
MD_LINK_RE = re.compile(r'\[([^\]]+?)\]\((https?://[^\s)]+)\)')

def build_text_and_entities_from_links(text: str) -> Tuple[str, List[dict]]:
    if not isinstance(text, str) or not text: return text, []
    segments: List[str] = []; entities: List[dict] = []; cursor = 0; plain = 0
    matches: List[Tuple[int,int,str,str]] = []
    for m in A_TAG_RE.finditer(text): matches.append((m.start(), m.end(), m.group(1), m.group(2)))
    for m in MD_LINK_RE.finditer(text): matches.append((m.start(), m.end(), m.group(2), m.group(1)))
    if not matches: return text, []
    matches.sort(key=lambda x: x[0])
    for start, end, url, label in matches:
        if start < cursor: continue
        prefix = text[cursor:start]; segments.append(prefix); plain += len(prefix)
        display = label; segments.append(display)
        entities.append({"type":"text_link","offset":plain,"length":len(display),"url":url})
        plain += len(display); cursor = end
    tail = text[cursor:]; segments.append(tail)
    return "".join(segments), entities

# ---------------- 核心处理：统一入口 ----------------
def dispatch_normal_copy(message):
    try:
        src_chat_id   = int(message["chat"]["id"])
        src_thread_id = message.get("message_thread_id")
        rules = match_routes(src_chat_id, src_thread_id)
        if not rules:
            app.logger.info("[route] no match; skip"); return

        text     = message.get("text") or message.get("caption")
        entities = message.get("entities") or message.get("caption_entities")
        has_text = bool(isinstance(text, str) and text.strip())
        msg_reply_markup = message.get("reply_markup")
        from_chat = src_chat_id
        msg_id    = message["message_id"]

        app.logger.info(f"[entities] has_text={has_text} entities_present={bool(entities)}; matched_rules={len(rules)}")

        def _payload_text_with_entities(base_chat_id, base_thread_id, base_reply_markup, text_in, entities_in):
            payload = {"chat_id": base_chat_id, "text": text_in or "", "disable_web_page_preview": False, "allow_sending_without_reply": True}
            used_entities = entities_in or []
            if not used_entities:
                new_text, built = build_text_and_entities_from_links(text_in or "")
                if built: payload["text"] = new_text; used_entities = built
            if used_entities: payload["entities"] = used_entities
            if base_thread_id: payload["message_thread_id"] = int(base_thread_id)
            if base_reply_markup: payload["reply_markup"] = base_reply_markup
            return payload

        def _payload_caption_media(kind, file_id, base_chat_id, base_thread_id, base_reply_markup, text_in, entities_in):
            payload = {"chat_id": base_chat_id, kind: file_id}
            if text_in:
                used = entities_in or []
                if not used:
                    new_text, built = build_text_and_entities_from_links(text_in)
                    if built: payload["caption"] = new_text; used = built
                    else:     payload["caption"] = text_in
                else:
                    payload["caption"] = text_in
                if used: payload["caption_entities"] = used
            if base_thread_id: payload["message_thread_id"] = int(base_thread_id)
            if base_reply_markup: payload["reply_markup"] = base_reply_markup
            return payload

        # ---------- 把发送逻辑作为内嵌函数（可访问上面的局部变量） ----------
        def _send_to_one_rule(rule: dict):
            tgt_chat   = int(rule["target_chat_id"])
            tgt_thread = rule.get("target_thread_id")

            # 1) 先把 -1 映射为“跟随源话题”
            if tgt_thread == -1:
                tgt_thread = message.get("message_thread_id")

            # 2) 再去重（用映射后的 tgt_thread）
            if _dedup_hit(from_chat, msg_id, tgt_chat, tgt_thread):
                app.logger.info("dedup hit, skip")
                return

            reply_markup = rule.get("reply_markup") or msg_reply_markup
            app.logger.info(f"[rule] dst={tgt_chat}/{tgt_thread} has_buttons={bool(reply_markup)} delay={rule.get('delay_seconds',0)}")

            # 文本：copyMessage → 回退 sendMessage
            if message.get("text") is not None and has_text:
                cp = {"chat_id": tgt_chat, "from_chat_id": from_chat, "message_id": msg_id}
                if tgt_thread:   cp["message_thread_id"] = int(tgt_thread)
                if reply_markup: cp["reply_markup"]      = reply_markup
                r_cp = tg("copyMessage", **cp)
                if r_cp.get("ok"):
                    app.logger.info("[text] copyMessage ok"); return
                app.logger.warning(f"[text] copyMessage fail -> {r_cp}")

                r_sm = tg("sendMessage", **_payload_text_with_entities(tgt_chat, tgt_thread, reply_markup, text, entities))
                app.logger.info(f"[text] fallback sendMessage ok={r_sm.get('ok')}"); return

            # 媒体：copyMessage → 回退 send<Media>/sendMessage
            cp = {"chat_id": tgt_chat, "from_chat_id": from_chat, "message_id": msg_id}
            if tgt_thread:   cp["message_thread_id"] = int(tgt_thread)
            if reply_markup: cp["reply_markup"]      = reply_markup
            r_cp = tg("copyMessage", **cp)
            if r_cp.get("ok"):
                app.logger.info("[media] copyMessage ok"); return
            app.logger.warning(f"[media] copyMessage fail -> {r_cp}")

            if has_text and message.get("text") is not None:
                r_sm = tg("sendMessage", **_payload_text_with_entities(tgt_chat, tgt_thread, reply_markup, text, entities))
                app.logger.info(f"[media->text] sendMessage ok={r_sm.get('ok')}"); return

            if "photo" in message:
                try:
                    fid = message["photo"][-1]["file_id"]
                    r = tg("sendPhoto", **_payload_caption_media("photo", fid, tgt_chat, tgt_thread, reply_markup, text if has_text else None, entities))
                    app.logger.info(f"[photo fallback] ok={r.get('ok')}"); return
                except Exception as e:
                    app.logger.error(f"[sendPhoto error] {e}")

            if "video" in message:
                try:
                    fid = message["video"]["file_id"]
                    r = tg("sendVideo", **_payload_caption_media("video", fid, tgt_chat, tgt_thread, reply_markup, text if has_text else None, entities))
                    app.logger.info(f"[video fallback] ok={r.get('ok')}"); return
                except Exception as e:
                    app.logger.error(f"[sendVideo error] {e}")

            if "document" in message:
                try:
                    fid = message["document"]["file_id"]
                    r = tg("sendDocument", **_payload_caption_media("document", fid, tgt_chat, tgt_thread, reply_markup, text if has_text else None, entities))
                    app.logger.info(f"[document fallback] ok={r.get('ok')}"); return
                except Exception as e:
                    app.logger.error(f"[sendDocument error] {e}")

            app.logger.info(f"[fallback failed] dst={tgt_chat}")

        # 调度每条规则
        for rule in rules:
            delay = int(rule.get("delay_seconds", 0) or 0)
            schedule_after_delay(delay, _send_to_one_rule, rule)

    except Exception as e:
        app.logger.exception(f"[dispatch_normal_copy fatal] {e}")



def handle_update(update: dict) -> bool:
    """webhook 与 polling 共用的入口"""
    try:
        message = (
            update.get("message")
            or update.get("edited_message")
            or update.get("channel_post")
            or update.get("edited_channel_post")
        )
        if not message:
            app.logger.info("[non-message update] passthrough"); return True

        app.logger.info(
            f"[hit] chat={message.get('chat',{}).get('id')} "
            f"type={message.get('chat',{}).get('type')} "
            f"text={(message.get('text') or message.get('caption') or '')[:80]} "
            f"mgid={message.get('media_group_id')}"
        )

        # /id
        text_cmd = (message.get("text") or "").strip()
        def is_id_cmd(t: str) -> bool:
            t = t.lower()
            return t == "/id" or (BOT_USERNAME and t == f"/id@{BOT_USERNAME.lower()}")
        if text_cmd and is_id_cmd(text_cmd):
            chat_id  = message["chat"]["id"]
            thread_id= message.get("message_thread_id")
            title    = (message.get("chat") or {}).get("title")
            info = f"chat_id: {chat_id}\nmessage_thread_id: {thread_id}\nchat_title: {title}"
            params = {"chat_id": chat_id, "text": info}
            if thread_id: params["message_thread_id"] = thread_id
            tg("sendMessage", **params)
            return True

        # 过滤
        if is_service_message(message):
            app.logger.info("[skip] service message"); return True

        chat_type = message.get("chat", {}).get("type")
        if chat_type in ("group","supergroup"):
            if not is_human_message(message):
                app.logger.info("[skip] non-human (bot/via_bot) or disallowed sender_chat]"); return True
        else:
            if not is_human_message(message):
                app.logger.info("[skip] non-human (private/channel via bot]"); return True

        if not has_allowed_content(message):
            app.logger.info("[skip] no allowed content"); return True

        # 相册
        if message.get("media_group_id"):
            ingest_album_piece(message); return True

        if not message.get("message_id"): return True

        dispatch_normal_copy(message)
        return True
    except Exception as e:
        app.logger.exception(f"[handle_update fatal] {e}")
        return False

# ---------------- Flask 路由 ----------------
@app.route("/", methods=["GET"])
def health(): return "OK"

@app.route("/version", methods=["GET"])
def version(): return jsonify({"version": VERSION, "polling": BOT_POLLING})

@app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    raw = request.get_data(as_text=True)
    app.logger.info(f"[update] len={len(raw)} body_head={raw[:300]}")
    update = request.get_json(force=True, silent=True) or {}
    ok = handle_update(update)
    return jsonify({"ok": ok})

# 设置 webhook 自检/刷新/诊断
@app.route(f"/webhook/{BOT_TOKEN}/set_webhook", methods=["POST","GET"])
def set_webhook_endpoint():
    base_url = request.args.get("base_url", "").strip()
    if not base_url:
        return jsonify({"ok": False, "error": "missing ?base_url=https://your.domain"})
    url = f"{base_url.rstrip('/')}/webhook/{BOT_TOKEN}"
    payload = {"url": url, "allowed_updates": [], "max_connections": 1, "drop_pending_updates": True}
    r = requests.post(f"{API}/setWebhook", json=payload, timeout=25)
    try: out = r.json()
    except Exception: out = {"ok": False, "text": r.text}
    return jsonify(out)

@app.route(f"/webhook/{BOT_TOKEN}/diag", methods=["GET"])
def diag():
    rules = get_routes()
    try:
        offset = int(flask_request.args.get("offset", "0"))
        limit  = int(flask_request.args.get("limit", "5"))
    except Exception:
        offset, limit = 0, 5
    offset = max(0, offset); limit = max(1, min(50, limit))
    return jsonify({"count": len(rules), "offset": offset, "limit": limit, "sample": rules[offset:offset+limit]})

@app.route(f"/webhook/{BOT_TOKEN}/diag_buttons", methods=["GET"])
def diag_buttons():
    rules = get_routes()
    with_btn = [r for r in rules if r.get("reply_markup")]
    slim = []
    for r in with_btn[:20]:
        slim.append({
            "source_chat_id": r["source_chat_id"],
            "source_thread_id": r["source_thread_id"],
            "target_chat_id": r["target_chat_id"],
            "target_thread_id": r["target_thread_id"],
            "delay_seconds": r["delay_seconds"],
            "buttons": r["reply_markup"]["inline_keyboard"][0] if r.get("reply_markup") else []
        })
    return jsonify({"total": len(rules), "with_buttons": len(with_btn), "sample": slim})

@app.route(f"/webhook/{BOT_TOKEN}/diag_stats", methods=["GET"])
def diag_stats():
    rules = get_routes()
    idx = [i for i, r in enumerate(rules) if r.get("reply_markup")]
    return jsonify({"total": len(rules), "with_buttons": len(idx), "indexes_with_buttons": idx[:100]})

@app.route(f"/webhook/{BOT_TOKEN}/routes_refresh", methods=["POST","GET"])
def routes_refresh():
    global _routes_cache_ts
    _routes_cache_ts = 0
    get_routes()
    return jsonify({"ok": True, "refreshed": True, "count": len(_routes_cache)})

# 别名
@app.route("/diag", methods=["GET"])
def diag_alias(): return diag()

@app.route("/routes_refresh", methods=["POST","GET"])
def routes_refresh_alias(): return routes_refresh()

@app.route(f"/webhook/{BOT_TOKEN}/ping", methods=["GET"])
def ping(): return jsonify({"ok": True, "ts": int(time.time())})

# ---------------- 长轮询线程 ----------------
def polling_loop():
    app.logger.info("[poll] starting getUpdates loop")
    offset = None
    while True:
        try:
            params = {"timeout": 50,
                      "allowed_updates": ["message","edited_message","channel_post","edited_channel_post"]}
            if offset is not None:
                params["offset"] = offset
            resp = tg("getUpdates", **params)
            if not resp.get("ok"):
                app.logger.warning(f"[poll] getUpdates fail: {resp}")
                time.sleep(2); continue
            updates = resp.get("result", [])
            for upd in updates:
                offset = int(upd.get("update_id", 0)) + 1
                handle_update(upd)
        except Exception as e:
            app.logger.error(f"[poll] exception: {e}")
            time.sleep(2)

if __name__ == "__main__":
    # 后台自动轮询 routes 表
    if os.getenv("ROUTE_AUTOPOLL_DISABLED", "0") not in ("1","true","True"):
        threading.Thread(target=_routes_autorefresh_loop, daemon=True).start()
        app.logger.info(f"[routes] background auto-poll started; interval={ROUTE_REFRESH_SEC}s")

    # ✅ 如需长轮询，开线程（无需公网）
    if BOT_POLLING:
        threading.Thread(target=polling_loop, daemon=True).start()
        app.logger.info("[boot] polling mode enabled (BOT_POLLING=1)")

    # 起个本地 HTTP 端口用于自检
    host = "0.0.0.0"; port = 9090
    app.logger.info(f"[boot] starting Flask on {host}:{port} ; VERSION={VERSION} ; POLLING={BOT_POLLING}")
    app.run(host=host, port=port)
