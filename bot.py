#!/usr/bin/env python3
"""
Homework / Conspect Bot (stable single-file version) — updated

Fixes:
- admin pending handling (no accidental reset)
- robust delete_user_data() to remove DB records and files
- improved Excel report styling (green Google Sheets style)
- dotenv usage for secrets
"""
import random
import os
import io
import time
import zipfile
import logging
import shutil
import sqlite3
import asyncio
import unicodedata
from datetime import datetime, date
from typing import List, Dict, Any, Optional

from dotenv import load_dotenv
load_dotenv()
# --- mention helper ---
def make_mention(user):
    if user.username:
        return f"<a href='tg://user?id={user.id}'>@{user.username}</a>"
    return f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"



import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, InputFile
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# for excel styling
from openpyxl import load_workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("homework-bot")

# ---------------- CONFIG ----------------
API_TOKEN = os.environ.get("API_TOKEN")
if not API_TOKEN:
    raise ValueError("API_TOKEN is missing. Set it in environment variables!")

try:
    ADMIN_ID = int(os.environ.get("ADMIN_ID") or 0)
except Exception:
    ADMIN_ID = 0

CONSPECTS_DIR = r"C:\BOT\conspects"
DB_FILE = r"C:\BOT\bot.db"


os.makedirs(CONSPECTS_DIR, exist_ok=True)

# ---------------- BOT / DISPATCHER / SCHEDULER ----------------
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())
scheduler = AsyncIOScheduler()

# in-memory flows
pending: Dict[str, Dict[str, Any]] = {}          # user_id_str -> {"type":"dz"/"conspect", "section":..., "topic":...}
admin_pending: Dict[str, Dict[str, Any]] = {}    # admin_id_str -> {"action":..., ...}
media_groups: Dict[str, Dict[str, Any]] = {}     # key = f"{uid}|{mgid}"
reasons_pending: Dict[str, str] = {}             # uid -> date_str (awaiting a reason reply)

# ---------------- UTIL ----------------
def today_str() -> str:
    return date.today().isoformat()

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def slugify_filename(s: str) -> str:
    s = unicodedata.normalize('NFKD', s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.replace("/", "_").replace("\\", "_")
    allowed = "-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
    return "".join(c for c in s if c in allowed)[:200] or "file"

def parse_date(text: str) -> Optional[date]:
    text = (text or "").strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            pass
    return None

# ---------------- DB ----------------
conn = sqlite3.connect(DB_FILE, check_same_thread=False)
cursor = conn.cursor()

def init_db():
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        type TEXT,
        section TEXT,
        topic_id TEXT,
        topic_title TEXT,
        content_type TEXT,
        content_summary TEXT,
        photo_file_id TEXT,
        date TEXT,
        ts TEXT,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS miss_reasons (
        user_id INTEGER,
        date TEXT,
        reason TEXT,
        PRIMARY KEY (user_id, date)
    )
    ''')
    conn.commit()

init_db()

# ---------------- TOPICS ----------------
SECTIONS = {
    "Основы Питона": [
        {"id": "op1", "title": "Вводный урок", "dz_allowed": True},
        {"id": "op2", "title": "Условия и целочисленные операторы", "dz_allowed": False},
        {"id": "op3", "title": "Цикл for", "dz_allowed": True},
        {"id": "op4", "title": "Цикл while", "dz_allowed": True},
        {"id": "op5", "title": "Практика: цикл for и while", "dz_allowed": False},
        {"id": "op6", "title": "Строки и срезы", "dz_allowed": False},
        {"id": "op7", "title": "Списки и генераторы", "dz_allowed": True},
    ],
    "ЕГЭ 1-27": [
        {"id": f"ege{n}", "title": f"Задание {n}", "dz_allowed": True} for n in range(1, 28)
    ]
}

# USER / SUBMISSION HELPERS
def ensure_user_record_obj(user: types.User):
    try:
        cursor.execute('INSERT OR IGNORE INTO users (id, username, first_name) VALUES (?, ?, ?)',
                       (user.id, user.username or "", user.first_name or ""))
        cursor.execute('UPDATE users SET username = ?, first_name = ? WHERE id = ?',
                       (user.username or "", user.first_name or "", user.id))
        conn.commit()
    except Exception:
        logger.exception("ensure_user_record_obj error")

def ensure_user_record_by_id(uid: int, username: str = "", first_name: str = ""):
    try:
        cursor.execute('INSERT OR IGNORE INTO users (id, username, first_name) VALUES (?, ?, ?)', (uid, username, first_name))
        cursor.execute('UPDATE users SET username = ?, first_name = ? WHERE id = ?', (username or "", first_name or "", uid))
        conn.commit()
    except Exception:
        logger.exception("ensure_user_record_by_id error")

def add_submission_obj(user: types.User, submission: dict):
    ensure_user_record_obj(user)
    try:
        cursor.execute('''
            INSERT INTO submissions (user_id, type, section, topic_id, topic_title, content_type, content_summary, photo_file_id, date, ts)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (user.id, submission['type'], submission['section'], submission['topic_id'], submission['topic_title'],
              submission['content_type'], submission.get('content_summary', ''), submission.get('photo_file_id', ''),
              submission['date'], submission['ts']))
        conn.commit()
    except Exception:
        logger.exception("add_submission_obj error")

# FILE SAVE
async def download_file_bytes(file_id: str) -> Optional[bytes]:
    try:
        f = await bot.get_file(file_id)
        file_path = f.file_path
        b = await bot.download_file(file_path)
        if hasattr(b, "read"):
            return b.read()
        return b
    except Exception:
        logger.exception("download error")
        return None

def save_conspect_files(user_id: str, section: str, topic_id: str, files: List[bytes], filenames: List[str]):
    base = os.path.join(CONSPECTS_DIR, str(user_id), f"{slugify_filename(section)}_{slugify_filename(topic_id)}")
    ensure_dir(base)
    saved = []
    for content, name in zip(files, filenames):
        safe = slugify_filename(name)
        path = os.path.join(base, safe)
        try:
            with open(path, "wb") as f:
                f.write(content)
            saved.append(path)
        except Exception:
            logger.exception("Failed to save file %s", path)
    return saved

def save_conspect_text(user_id: str, section: str, topic_id: str, text: str):
    base = os.path.join(CONSPECTS_DIR, str(user_id), f"{slugify_filename(section)}_{slugify_filename(topic_id)}")
    ensure_dir(base)
    fname = datetime.utcnow().strftime("%Y%m%d_%H%M%S") + ".txt"
    path = os.path.join(base, fname)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(text)
        return path
    except Exception:
        logger.exception("Failed to save text file %s", path)
        return None

# ---------------- KEYBOARDS ----------------
def make_main_kb(is_admin: bool):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📚 Сдать ДЗ"))
    kb.add(KeyboardButton("📘 Сдать конспект"))
    kb.row(KeyboardButton("📁 Мои конспекты"))
    kb.row(KeyboardButton("📌 Главное меню"))
    if is_admin:
        kb.row(KeyboardButton("🛠️ Админ-панель"))
    return kb

def section_keyboard():
    kb = InlineKeyboardMarkup()
    for sec in SECTIONS.keys():
        kb.add(InlineKeyboardButton(sec, callback_data=f"sec|{sec}"))
    return kb

def topics_keyboard(section_name):
    kb = InlineKeyboardMarkup(row_width=2)
    topics = SECTIONS.get(section_name, [])
    for t in topics:
        kb.add(InlineKeyboardButton(t["title"], callback_data=f"topic|{section_name}|{t['id']}"))
    kb.add(InlineKeyboardButton("Отмена", callback_data="cancel"))
    return kb

def admin_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("📋 Дневной отчёт (подробный)", callback_data="admin|daily_full"))
    kb.add(InlineKeyboardButton("📤 Выслать таблицу сейчас", callback_data="admin|send_daily_now"))
    kb.add(InlineKeyboardButton("👤 Выгрузить ученика (Excel + фото ZIP)", callback_data="admin|export_user"))
    kb.add(InlineKeyboardButton("🗑️ Удалить отправления ученика", callback_data="admin|delete_user"))
    kb.add(InlineKeyboardButton("♻️ Сбросить все статусы", callback_data="admin|reset_all"))
    kb.add(InlineKeyboardButton("Отмена", callback_data="cancel"))
    return kb

# PRAISE
generic_praise = ["Молодец, отличная работа!","Здорово, так держать!","Круто, ты справился!","Умница, ДЗ принято!"]
context_praise_templates = ["Отлично поработал над «{topic}» — заметен прогресс!","Круто! Тема «{topic}» покоряется тебе всё лучше.","Хорошая работа по «{topic}» — продолжай в том же духе!"]
photo_praise = ["Фото принято — выглядит аккуратно!","Классный снимок, спасибо!","Фото получено — спасибо за старание!"]

def get_praise(user_id: int, section: str, topic_title: str, content_type: str) -> str:
    try:
        cursor.execute('SELECT COUNT(*) FROM submissions WHERE user_id = ?', (user_id,))
        total_subs = cursor.fetchone()[0]
    except Exception:
        total_subs = 0
    messages = []
    if topic_title:
        messages.append(random.choice(context_praise_templates).format(topic=topic_title))
    if content_type == "photo":
        messages.append(random.choice(photo_praise))
    else:
        messages.append(random.choice(generic_praise))
    if total_subs >= 10:
        messages.append("Ты постоянный участник — это впечатляет! 🔥")
    elif total_subs >= 3:
        messages.append("Отлично, ты активно сдаёшь. Продолжай!")
    n = 2 if random.random() > 0.4 else 1
    return " ".join(messages[:n])

# MAKING EXCEL
def submissions_for_date(target_date: date) -> List[dict]:
    dstr = target_date.isoformat()
    try:
        cursor.execute('SELECT id, username, first_name FROM users ORDER BY id')
        users = cursor.fetchall()
        result = []
        for u in users:
            uid, uname, fname = u
            username = uname or fname or ""
            cursor.execute('SELECT type FROM submissions WHERE user_id = ? AND date = ?', (uid, dstr))
            types = {r[0] for r in cursor.fetchall()}
            dz = 'dz' in types
            cons = 'conspect' in types
            cursor.execute('SELECT reason FROM miss_reasons WHERE user_id = ? AND date = ?', (uid, dstr))
            rr = cursor.fetchone()
            reason = rr[0] if rr else ""
            result.append({
                "user_id": uid,
                "username": username,
                "date": dstr,
                "dz_submitted": int(dz),
                "conspect_submitted": int(cons),
                "miss_reason": reason
            })
        return result
    except Exception:
        logger.exception("submissions_for_date build error")
        return []

def style_worksheet(ws):
    header_fill = PatternFill(start_color="A7F3D0", end_color="A7F3D0", fill_type="solid")
    thin = Side(border_style="thin", color="000000")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    # header row styling
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = Font(bold=True)
        cell.border = border
        cell.alignment = Alignment(horizontal="center", vertical="center")

    # apply border to body and compute column widths
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.border = border
            # align text for cells with longer content
            if cell.row > 1:
                cell.alignment = Alignment(wrap_text=True, vertical="top")

    # auto width
    for i, col in enumerate(ws.columns, 1):
        max_length = 0
        col_letter = get_column_letter(i)
        for cell in col:
            try:
                if cell.value is not None:
                    length = len(str(cell.value))
                    if length > max_length:
                        max_length = length
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_length + 5, 60)

def make_daily_excel(target_date: date) -> io.BytesIO:
    # build summary rows
    rows = submissions_for_date(target_date)
    df_summary = pd.DataFrame(rows)

    # build raw submissions
    try:
        cursor.execute('''
            SELECT s.id, s.user_id, u.username, u.first_name, s.type, s.section, s.topic_id, s.topic_title, s.content_type, s.content_summary, s.photo_file_id, s.date, s.ts
            FROM submissions s JOIN users u ON s.user_id = u.id
            WHERE s.date = ?
            ORDER BY s.ts
        ''', (target_date.isoformat(),))
        raw = cursor.fetchall()
        raw_rows = [{
            "id": r[0],
            "user_id": r[1],
            "username": r[2] or r[3],
            "type": r[4],
            "section": r[5],
            "topic_id": r[6],
            "topic_title": r[7],
            "content_type": r[8],
            "content_summary": r[9],
            "photo_file_id": r[10],
            "date": r[11],
            "ts": r[12]
        } for r in raw]
        df_raw = pd.DataFrame(raw_rows)
    except Exception:
        logger.exception("make_daily_excel raw fetch error")
        df_raw = pd.DataFrame(columns=["id","user_id","username","type","section","topic_id","topic_title","content_type","content_summary","photo_file_id","date","ts"])

    bio = io.BytesIO()
    # use pandas writer then style with openpyxl
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        if df_summary.empty:
            df_summary = pd.DataFrame(columns=["user_id","username","date","dz_submitted","conspect_submitted","miss_reason"])
        df_summary.to_excel(writer, sheet_name="daily_summary", index=False)
        if df_raw.empty:
            df_raw = pd.DataFrame(columns=["id","user_id","username","type","section","topic_id","topic_title","content_type","content_summary","photo_file_id","date","ts"])
        df_raw.to_excel(writer, sheet_name="raw_submissions", index=False)
    bio.seek(0)

    # open with openpyxl and style
    wb = load_workbook(filename=io.BytesIO(bio.read()))
    if "daily_summary" in wb.sheetnames:
        style_worksheet(wb["daily_summary"])
    if "raw_submissions" in wb.sheetnames:
        style_worksheet(wb["raw_submissions"])
    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out

async def send_daily_excel_to_admin(target_date: date):
    try:
        bio = make_daily_excel(target_date)
        fname = f"daily_report_{target_date.isoformat()}.xlsx"
        await bot.send_document(ADMIN_ID, InputFile(bio, filename=fname))
        await bot.send_message(ADMIN_ID, f"Отчёт за {target_date.isoformat()} отправлен.")
    except Exception:
        logger.exception("send_daily_excel_to_admin error")
        try:
            await bot.send_message(ADMIN_ID, "Ошибка при формировании отчёта.")
        except Exception:
            pass

# HANDLERS
@dp.message_handler(commands=["start", "menu"])
async def cmd_start(message: types.Message):
    ensure_user_record_obj(message.from_user)
    is_admin = (message.from_user.id == ADMIN_ID)
    await message.answer("Привет! Я бот для сдачи ДЗ и конспектов.\nВыбери действие:", reply_markup=make_main_kb(is_admin))

@dp.message_handler(lambda m: m.text == "📚 Сдать ДЗ")
async def cmd_send_dz(message: types.Message):
    pending[str(message.from_user.id)] = {"type": "dz", "ts": datetime.utcnow().isoformat()}
    await message.answer("Выбери раздел:", reply_markup=section_keyboard())

@dp.message_handler(lambda m: m.text == "📘 Сдать конспект")
async def cmd_send_conspect(message: types.Message):
    pending[str(message.from_user.id)] = {"type": "conspect", "ts": datetime.utcnow().isoformat()}
    await message.answer("Выбери раздел:", reply_markup=section_keyboard())

@dp.message_handler(lambda m: m.text == "📁 Мои конспекты")
async def cmd_my_conspects(message: types.Message):
    uid = str(message.from_user.id)
    folder = os.path.join(CONSPECTS_DIR, uid)
    if not os.path.exists(folder):
        return await message.answer("У тебя пока нет сохранённых конспектов.", reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(folder):
            for f in files:
                full = os.path.join(root, f)
                arc = os.path.relpath(full, folder)
                zf.write(full, arc)
    buf.seek(0)
    await bot.send_document(message.chat.id, InputFile(buf, filename="my_conspects.zip"), reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))

@dp.message_handler(lambda m: m.text == "📌 Главное меню")
async def cmd_main_menu(message: types.Message):
    uid = str(message.from_user.id)
    # НЕ трогаем admin_pending!
    if uid in pending:
        pending.pop(uid)
    is_admin = (message.from_user.id == ADMIN_ID)
    await message.answer("Главное меню:", reply_markup=make_main_kb(is_admin))


@dp.message_handler(lambda m: m.text in ("🛠️ Админ-панель", "Админские инструменты"))
async def cmd_admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return await message.answer("Доступ запрещён.", reply_markup=make_main_kb(False))
    # show admin inline keyboard
    await message.answer("Админ-панель:", reply_markup=admin_kb())

# Section / topic callbacks
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("sec|"))
async def cb_section_choose(call: types.CallbackQuery):
    _, section = call.data.split("|", 1)
    uid = str(call.from_user.id)
    if uid not in pending:
        await call.answer("Нет активного действия. Нажми 'Сдать ДЗ' или 'Сдать конспект' в меню.", show_alert=True)
        return
    pending[uid]["section"] = section
    await bot.send_message(call.from_user.id, f"Выбран раздел: {section}\nВыбери тему:", reply_markup=topics_keyboard(section))
    await call.answer()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("topic|"))
async def cb_topic_choose(call: types.CallbackQuery):
    _, section, topic_id = call.data.split("|", 2)
    uid = str(call.from_user.id)
    if uid not in pending or "section" not in pending[uid]:
        await call.answer("Нет активного действия. Сначала нажми 'Сдать ДЗ' или 'Сдать конспект'.", show_alert=True)
        return
    topic = None
    for t in SECTIONS.get(section, []):
        if t["id"] == topic_id:
            topic = t
            break
    if not topic:
        await call.answer("Тема не найдена.", show_alert=True)
        return
    if pending[uid]["type"] == "dz" and not topic.get("dz_allowed", True):
        await call.answer("Для этой темы ДЗ не предусмотрено. Выбери другую тему или сдай конспект.", show_alert=True)
        return
    pending[uid]["topic"] = {"id": topic["id"], "title": topic["title"]}
    await bot.send_message(call.from_user.id, f"Тема: {topic['title']}\nТеперь отправь {('ДЗ' if pending[uid]['type']=='dz' else 'конспект')} текстом или фото. В подписи к фото можно указать подробности.", reply_markup=None)
    await call.answer()

@dp.callback_query_handler(lambda c: c.data == "cancel")
async def cb_cancel(call: types.CallbackQuery):
    uid = str(call.from_user.id)
    pending.pop(uid, None)
    admin_pending.pop(uid, None)
    await bot.send_message(call.from_user.id, "Операция отменена.", reply_markup=make_main_kb(call.from_user.id==ADMIN_ID))
    await call.answer()


@dp.message_handler(content_types=types.ContentType.TEXT)
async def handle_text(message: types.Message):
    uid = str(message.from_user.id)
    text = message.text.strip()

    if message.text in ("📊 Статистика",):
        return

    # don't accidentally wipe admin_pending by generic commands
    if message.text in ("📚 Сдать ДЗ", "📘 Сдать конспект", "📁 Мои конспекты", "📌 Главное меню"):
        if uid in pending:
            pending.pop(uid)

    if uid in reasons_pending:
        miss_date = reasons_pending.pop(uid)
        try:
            cursor.execute('INSERT OR REPLACE INTO miss_reasons (user_id, date, reason) VALUES (?, ?, ?)', (int(uid), miss_date, text))
            conn.commit()
        except Exception:
            logger.exception("saving miss reason failed")
        await message.answer(f"Спасибо — причина пропуска за {miss_date} сохранена.", reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        return

    # admin flows waiting for text
    if str(message.from_user.id) in admin_pending:
        # handled by admin_pending_text callback
        return

    if uid in pending and "section" in pending[uid] and "topic" in pending[uid]:
        p = pending.pop(uid)
        sub = {"type": p["type"], "section": p["section"], "topic_id": p["topic"]["id"], "topic_title": p["topic"]["title"],
               "content_type": "text", "content_summary": (text if len(text)<=300 else text[:297]+"..."),
               "date": today_str(), "ts": datetime.utcnow().isoformat()}
        add_submission_obj(message.from_user, sub)
        if sub["type"] == "conspect":
            try:
                save_conspect_text(uid, sub["section"].replace("/","_"), sub["topic_id"], text)
            except Exception:
                logger.exception("Failed to save conspect text")
        praise = get_praise(message.from_user.id, sub["section"], sub["topic_title"], "text")
        await message.answer(praise, reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        try:
            await bot.send_message(ADMIN_ID, f"✅ {make_mention(message.from_user)} прислал {sub['type'].upper()}: {sub['section']} — {sub['topic_title']}\n{sub['content_summary']}")
        except Exception:
            pass
        return

    low = text.lower()
    if low.startswith("дз") or low.startswith("конспект"):
        kind = "dz" if low.startswith("дз") else "conspect"
        sub = {"type": kind, "section": "Без раздела", "topic_id": "none", "topic_title": text.split("\n",1)[0][:50],
               "content_type": "text", "content_summary": (text if len(text)<=300 else text[:297]+"..."),
               "date": today_str(), "ts": datetime.utcnow().isoformat()}
        add_submission_obj(message.from_user, sub)
        if kind == "conspect":
            try:
                save_conspect_text(uid, "Без_раздела", "none", text)
            except Exception:
                logger.exception("Failed to save conspect text")
        praise = get_praise(message.from_user.id, sub["section"], sub["topic_title"], "text")
        await message.answer("Записал без выбора темы. В следующий раз выбери тему через меню.\n\n" + praise, reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        try:
            await bot.send_message(ADMIN_ID, f"✅ {make_mention(message.from_user)} прислал {sub['type'].upper()} (без темы): {sub['content_summary']}")
        except Exception:
            pass
        return

    await message.answer("Чтобы сдать ДЗ или конспект: нажми соответствующую кнопку в меню и выбери тему.", reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))

@dp.message_handler(content_types=types.ContentType.PHOTO)
async def handle_photo(message: types.Message):
    uid = str(message.from_user.id)
    caption = message.caption or ""
    mgid = getattr(message, "media_group_id", None)

    if mgid:
        key = f"{uid}|{mgid}"
        entry = media_groups.get(key)
        file_id = message.photo[-1].file_id
        if not entry:
            media_groups[key] = {"file_ids": [file_id], "caption": caption or "", "last_update": time.time(), "pending_snapshot": pending.get(uid), "uid": uid}
        else:
            entry["file_ids"].append(file_id)
            if caption:
                entry["caption"] = caption
            entry["last_update"] = time.time()
        await message.answer("Фото получено — обрабатываю альбом...", reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        return

    # single photo
    if uid in pending and "section" in pending[uid] and "topic" in pending[uid]:
        p = pending.pop(uid)
        file_id = message.photo[-1].file_id
        summary = (caption[:200] + "...") if len(caption)>200 else caption or "Фото"
        sub = {"type": p["type"], "section": p["section"], "topic_id": p["topic"]["id"], "topic_title": p["topic"]["title"],
               "content_type": "photo", "content_summary": summary, "photo_file_id": file_id, "date": today_str(), "ts": datetime.utcnow().isoformat()}
        add_submission_obj(message.from_user, sub)
        if sub["type"] == "conspect":
            try:
                fbytes = await download_file_bytes(file_id)
                if fbytes:
                    save_conspect_files(uid, sub["section"].replace("/","_"), sub["topic_id"], [fbytes], [f"photo_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"])
            except Exception:
                logger.exception("Failed to save conspect photo")
        praise = get_praise(message.from_user.id, sub["section"], sub["topic_title"], "photo")
        await message.answer(praise, reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        try:
            await bot.send_message(ADMIN_ID, f"📸 {make_mention(message.from_user)} прислал {sub['type'].upper()}: {sub['section']} — {sub['topic_title']} — {summary}")
            await bot.forward_message(ADMIN_ID, message.chat.id, message.message_id)
        except Exception:
            pass
        return

    # caption-start fallback
    if caption.lower().startswith("дз") or caption.lower().startswith("конспект"):
        kind = "dz" if caption.lower().startswith("дз") else "conspect"
        file_id = message.photo[-1].file_id
        summary = (caption[:200] + "...") if len(caption)>200 else caption
        sub = {"type": kind, "section": "Без раздела", "topic_id": "none", "topic_title": summary.split("\n",1)[0][:50],
               "content_type": "photo", "content_summary": summary, "photo_file_id": file_id, "date": today_str(), "ts": datetime.utcnow().isoformat()}
        add_submission_obj(message.from_user, sub)
        if kind == "conspect":
            try:
                fbytes = await download_file_bytes(file_id)
                if fbytes:
                    save_conspect_files(uid, "Без_раздела", "none", [fbytes], [f"photo_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"])
            except Exception:
                logger.exception("Failed to save conspect photo")
        praise = get_praise(message.from_user.id, sub["section"], sub["topic_title"], "photo")
        await message.answer("Принял (без выбора темы). " + praise, reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))
        try:
            await bot.forward_message(ADMIN_ID, message.chat.id, message.message_id)
        except Exception:
            pass
        return

    await message.answer("Чтобы сдать ДЗ: сначала нажми кнопку в меню, выбери тему, затем отправь фото.", reply_markup=make_main_kb(message.from_user.id==ADMIN_ID))

# media-groups finalizer (runs periodically)
async def process_media_groups():
    now = time.time()
    keys = list(media_groups.keys())
    for key in keys:
        entry = media_groups.get(key)
        if not entry:
            continue
        if now - entry['last_update'] > 1.5:
            try:
                uid = entry.get("uid")
                p_snapshot = entry.get("pending_snapshot") or {}
                p_type = p_snapshot.get("type", "dz")
                section = p_snapshot.get("section", "Без раздела")
                topic = p_snapshot.get("topic", {"id": "none", "title": "Альбом"})
                file_ids = entry.get("file_ids", [])
                caption = entry.get("caption", "") or f"Альбом из {len(file_ids)} фото"
                sub = {"type": p_type, "section": section, "topic_id": topic.get("id"), "topic_title": topic.get("title"),
                       "content_type": "photo_album", "content_summary": caption, "photo_file_id": ";".join(file_ids),
                       "date": today_str(), "ts": datetime.utcnow().isoformat()}
                ensure_user_record_by_id(int(uid))
                fake_user = types.User(id=int(uid), is_bot=False, first_name="", username="")
                add_submission_obj(fake_user, sub)
                # save files if conspect
                if sub['type'] == 'conspect':
                    files = []
                    names = []
                    for idx, fid in enumerate(file_ids, start=1):
                        b = await download_file_bytes(fid)
                        if b:
                            name = f"photo_{idx}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"
                            files.append(b); names.append(name)
                    if files:
                        save_conspect_files(uid, section.replace("/","_"), sub['topic_id'], files, names)
                try:
                    await bot.send_message(int(uid), get_praise(int(uid), sub['section'], sub['topic_title'], 'photo'))
                except Exception:
                    pass
                try:
                    await bot.send_message(ADMIN_ID, f"📸 user {uid} прислал {sub['type'].upper()} (альбом): {sub['section']} — {sub['topic_title']}")
                except Exception:
                    pass
            except Exception:
                logger.exception('Error finalizing media group %s', key)
            finally:
                media_groups.pop(key, None)

# ADMIN HELPERS
async def produce_and_send_user_export(admin_id: int, identifier: str):
    identifier = (identifier or "").lstrip('@').strip()
    target_uid = None
    if identifier.isdigit():
        cursor.execute('SELECT id FROM users WHERE id = ?', (int(identifier),))
        r = cursor.fetchone()
        if r:
            target_uid = str(r[0])
    if not target_uid:
        cursor.execute('SELECT id, username, first_name FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(first_name) = LOWER(?)', (identifier, identifier))
        r = cursor.fetchone()
        if r:
            target_uid = str(r[0])

    if not target_uid:
        await bot.send_message(admin_id, "Пользователь не найден.")
        return

    # fetch submissions
    cursor.execute('SELECT type, section, topic_id, topic_title, content_type, content_summary, photo_file_id, date, ts FROM submissions WHERE user_id = ?', (int(target_uid),))
    subs = cursor.fetchall()
    if not subs:
        await bot.send_message(admin_id, "У пользователя нет отправлений.")
        return

    cursor.execute('SELECT username, first_name FROM users WHERE id = ?', (int(target_uid),))
    row = cursor.fetchone()
    username = (row[0] or row[1]) if row else target_uid

    lines = [f"Выгрузка для @{username} (id: {target_uid}). Всего: {len(subs)}"]
    for s in subs:
        lines.append(f"- [{s[0].upper()}] {s[7]} {s[1]} — {s[3]} — {s[5]}")
    text = "\n".join(lines)
    for chunk in [text[i:i+3900] for i in range(0, len(text), 3900)]:
        await bot.send_message(admin_id, chunk)

    rows = []
    for s in subs:
        rows.append({"user_id": target_uid, "username": username, "type": s[0], "section": s[1], "topic_id": s[2], "topic_title": s[3], "content_type": s[4], "content_summary": s[5], "photo_file_id": s[6], "date": s[7], "ts": s[8]})
    excel_bio = io.BytesIO()
    df = pd.DataFrame(rows)
    with pd.ExcelWriter(excel_bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="submissions")
    excel_bio.seek(0)
    await bot.send_document(admin_id, InputFile(excel_bio, filename=f"user_{target_uid}_submissions.xlsx"))

    # prepare zip of saved files and download photos referenced in submissions
    zip_bio = io.BytesIO()
    with zipfile.ZipFile(zip_bio, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        user_dir = os.path.join(CONSPECTS_DIR, target_uid)
        if os.path.exists(user_dir):
            for root, _, files in os.walk(user_dir):
                for f in files:
                    full = os.path.join(root, f)
                    arc = os.path.relpath(full, user_dir)
                    zf.write(full, arc)
        for s in subs:
            pf = s[6]
            if pf:
                for fid in str(pf).split(";"):
                    fid = fid.strip()
                    if not fid:
                        continue
                    b = await download_file_bytes(fid)
                    if b:
                        zf.writestr(f"downloaded_{fid[:8]}.jpg", b)
    zip_bio.seek(0)
    if zip_bio.getbuffer().nbytes > 0:
        await bot.send_document(admin_id, InputFile(zip_bio, filename="user_files.zip"))
    else:
        await bot.send_message(admin_id, "У пользователя нет загруженных файлов/фото.")

def delete_user_data(user_id: int):
    # Remove all user's submissions/miss_reasons DB records and delete filesystem folders/files.
    # Returns a dict with details for admin feedback and logging.
    result = {
        "db_deleted_submissions": 0,
        "db_deleted_reasons": 0,
        "db_before_submissions": None,
        "db_after_submissions": None,
        "db_before_reasons": None,
        "db_after_reasons": None,
        "files_removed": [],
        "files_failed": [],
        "errors": []
    }

    # 1) Database cleanup with counts before/after
    try:
        con = sqlite3.connect(DB_FILE)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM submissions WHERE user_id = ?", (int(user_id),))
        result["db_before_submissions"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM miss_reasons WHERE user_id = ?", (int(user_id),))
        result["db_before_reasons"] = cur.fetchone()[0]

        cur.execute("DELETE FROM submissions WHERE user_id = ?", (int(user_id),))
        cur.execute("DELETE FROM miss_reasons WHERE user_id = ?", (int(user_id),))
        con.commit()

        cur.execute("SELECT COUNT(*) FROM submissions WHERE user_id = ?", (int(user_id),))
        result["db_after_submissions"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM miss_reasons WHERE user_id = ?", (int(user_id),))
        result["db_after_reasons"] = cur.fetchone()[0]

        result["db_deleted_submissions"] = (result["db_before_submissions"] or 0) - (result["db_after_submissions"] or 0)
        result["db_deleted_reasons"] = (result["db_before_reasons"] or 0) - (result["db_after_reasons"] or 0)
        con.close()
    except Exception as e:
        logger.exception("Error deleting DB records for user %s: %s", user_id, e)
        result["errors"].append(f"db_error: {str(e)}")

    # 2) Filesystem cleanup: remove CONSPECTS_DIR/<user_id> thoroughly
    user_folder = os.path.join(CONSPECTS_DIR, str(user_id))
    if os.path.exists(user_folder):
        for root, dirs, files in os.walk(user_folder, topdown=False):
            for name in files:
                fpath = os.path.join(root, name)
                try:
                    os.remove(fpath)
                    result["files_removed"].append(os.path.relpath(fpath, user_folder))
                except Exception as e:
                    logger.exception("Failed to remove file %s: %s", fpath, e)
                    result["files_failed"].append({"path": fpath, "error": str(e)})
            for name in dirs:
                dpath = os.path.join(root, name)
                try:
                    os.rmdir(dpath)
                except Exception:
                    # ignore; final rmtree will handle nested content
                    pass
        try:
            os.rmdir(user_folder)
        except Exception:
            try:
                shutil.rmtree(user_folder)
            except Exception as e:
                logger.exception("Failed to remove user folder %s: %s", user_folder, e)
                result["errors"].append(f"folder_rmtree_error: {str(e)}")
    return result

async def delete_user_submissions(admin_id: int, identifier: str):
    identifier = (identifier or "").lstrip('@').strip()
    target_uid = None
    try:
        if identifier.isdigit():
            cursor.execute('SELECT id, username, first_name FROM users WHERE id = ?', (int(identifier),))
            r = cursor.fetchone()
            if r:
                target_uid = str(r[0])
        if not target_uid:
            cursor.execute('SELECT id, username, first_name FROM users WHERE LOWER(username) = LOWER(?) OR LOWER(first_name) = LOWER(?)', (identifier, identifier))
            r = cursor.fetchone()
            if r:
                target_uid = str(r[0])

        if not target_uid:
            await bot.send_message(admin_id, "Пользователь не найден.")
            return

        await bot.send_message(admin_id, f"Начинаю удаление данных пользователя {target_uid} ...")

        res = delete_user_data(int(target_uid))

        # Compose human-readable report for admin
        parts = []
        if res.get("errors"):
            parts.append("В процессе удаления возникли ошибки:")
            parts.extend(res["errors"])
        else:
            parts.append(f"Удаление завершено для пользователя {target_uid}.")
            parts.append(f"Удалено записей submissions: {res.get('db_deleted_submissions',0)} (до={res.get('db_before_submissions')}, после={res.get('db_after_submissions')})")
            parts.append(f"Удалено записей miss_reasons: {res.get('db_deleted_reasons',0)} (до={res.get('db_before_reasons')}, после={res.get('db_after_reasons')})")
            if res.get("files_removed"):
                sample = ", ".join(res["files_removed"][:8])
                parts.append(f"Удалено файлов: {len(res['files_removed'])}. Примеры: {sample}")
            else:
                parts.append("Файлы не найдены или уже удалены.")
            if res.get("files_failed"):
                parts.append(f"Не удалось удалить {len(res['files_failed'])} файлов. Проверь логи сервера.")

        text = "\\n".join(parts)
        for chunk in [text[i:i+3900] for i in range(0, len(text), 3900)]:
            await bot.send_message(admin_id, chunk)
    except Exception as e:
        logger.exception("delete_user_submissions error: %s", e)
        try:
            await bot.send_message(admin_id, "Ошибка при удалении данных пользователя. Смотри логи на сервере.")
        except Exception:
            pass

# ADMIN HANDLERS (callbacks & pending)
@dp.callback_query_handler(lambda c: c.data and c.data.startswith("admin|"))
async def cb_admin(call: types.CallbackQuery):
    if call.from_user.id != ADMIN_ID:
        return await call.answer("Нет доступа", show_alert=True)

    action = call.data.split("|", 1)[1]
    aid = str(call.from_user.id)

    if action == "daily_full":
        await send_daily_excel_to_admin(date.today())
        await call.answer("Отправил дневной отчёт (подробный).")
        return

    if action == "send_daily_now":
        await send_daily_excel_to_admin(date.today())
        await call.answer("Отправил таблицу прямо сейчас.")
        return

    if action == "export_user":
        admin_pending[aid] = {"action": "export_user"}
        await bot.send_message(call.from_user.id, "Введи ID или username ученика для выгрузки (можно с @):")
        await call.answer()
        return

    if action == "delete_user":
        admin_pending[aid] = {"action": "delete_user"}
        await bot.send_message(call.from_user.id, "Введи ID или username ученика для удаления (можно с @):")
        await call.answer()
        return

    if action == "reset_all":
        try:
            cursor.execute('DELETE FROM submissions')
            cursor.execute('DELETE FROM miss_reasons')
            conn.commit()
            if os.path.exists(CONSPECTS_DIR):
                shutil.rmtree(CONSPECTS_DIR)
                os.makedirs(CONSPECTS_DIR, exist_ok=True)
            await bot.send_message(call.from_user.id, "Данные и файлы сброшены.")
        except Exception:
            logger.exception("reset_all error")
            await bot.send_message(call.from_user.id, "Ошибка при сбросе данных.")
        await call.answer()
        return

    if action == "cancel":
        await bot.send_message(call.from_user.id, "Операция отменена.", reply_markup=admin_kb())
        await call.answer()
        return

@dp.message_handler(lambda m: str(m.from_user.id) in admin_pending)
async def admin_pending_text(message: types.Message):
    aid = str(message.from_user.id)
    if message.from_user.id != ADMIN_ID:
        return
    task = admin_pending.pop(aid, None)
    if not task:
        return
    action = task.get("action")
    if action == "export_user":
        identifier = message.text.strip()
        await produce_and_send_user_export(message.from_user.id, identifier)
        await message.answer("Выгрузка завершена.", reply_markup=admin_kb())
        return
    if action == "delete_user":
        identifier = message.text.strip()
        await delete_user_submissions(message.from_user.id, identifier)
        await message.answer("Удаление выполнено.", reply_markup=admin_kb())
        return

# ---------------- SCHEDULER TASKS ----------------
async def daily_reminder():
    try:
        cursor.execute('SELECT id FROM users')
        rows = cursor.fetchall()
        for row in rows:
            uid = row[0]
            try:
                await bot.send_message(int(uid), "⏰ Напоминание: не забудьте сегодня сдать ДЗ и/или конспект. Нажми в меню 'Сдать ДЗ' или 'Сдать конспект'.")
            except Exception:
                pass
    except Exception:
        logger.exception("daily_reminder error")

async def ask_missed_reason():
    dstr = today_str()
    try:
        cursor.execute('SELECT DISTINCT user_id FROM submissions WHERE date = ?', (dstr,))
        submitted_uids = {r[0] for r in cursor.fetchall()}
        cursor.execute('SELECT id FROM users')
        all_uids = {r[0] for r in cursor.fetchall()}
        to_ask = all_uids - submitted_uids
        for uid in to_ask:
            try:
                reasons_pending[str(uid)] = dstr
                await bot.send_message(int(uid), f"Сегодня ({dstr}) ты ничего не сдал(а). Можешь коротко указать причину пропуска? (ответ будет сохранён)")
            except Exception:
                reasons_pending.pop(str(uid), None)
                pass
    except Exception:
        logger.exception("ask_missed_reason error")

async def daily_admin_report():
    d = today_str()
    try:
        cursor.execute('SELECT COUNT(*) FROM submissions WHERE date = ? AND type = ?', (d, 'dz'))
        dz = cursor.fetchone()[0]
        cursor.execute('SELECT COUNT(*) FROM submissions WHERE date = ? AND type = ?', (d, 'conspect'))
        cons = cursor.fetchone()[0]
        text = f"Ежедневный отчёт за {d}:\nДЗ: {dz}\nКонспект: {cons}\nДля подробностей нажми в админ-панели 'Дневной отчёт (подробный)'."
        await bot.send_message(ADMIN_ID, text)
    except Exception:
        logger.exception("daily_admin_report error")

async def on_startup(dispatcher):
    scheduler.add_job(daily_reminder, "cron", hour=18, minute=0)
    scheduler.add_job(daily_admin_report, "cron", hour=23, minute=55)
    scheduler.add_job(process_media_groups, "interval", seconds=2)
    scheduler.add_job(ask_missed_reason, "cron", hour=23, minute=57)
    scheduler.start()
    logger.info("Scheduler started")

#- MAIN
if __name__ == "__main__":
    print("Starting bot. Make sure API_TOKEN and ADMIN_ID are set in env.")

    executor.start_polling(dp, skip_updates=True, on_startup=on_startup)




