# -*- coding: utf-8 -*-
"""
FunPay -> Telegram notification bot (MTProto + FakeTLS).

Принцип работы:
  1. В фоновом потоке крутится FunPayAPI Runner: он опрашивает FunPay
     (по cookie golden_key) и ловит события: новые сообщения, заказы,
     подтверждения, отзывы.
  2. События складываются в asyncio-очередь и обрабатываются в основном
     (async) цикле: бот присылает тебе уведомление в Telegram.
  3. Под каждым уведомлением о сообщении есть кнопка "Ответить" -
     пишешь текст прямо боту, он отправляет его в чат FunPay.

Telegram-часть работает по MTProto (как сам клиент Telegram), VPN не нужен.
Если провайдер режет прямой доступ к серверам Telegram, бот подключается
через MTProto-прокси (FakeTLS) — их можно указать несколько через ";".
Если прокси в .env перестанут работать — запусти update_proxies.py,
он сам найдёт свежие и пропишет их в .env.
"""

import asyncio
import datetime
import html

# Таймзоны: FunPay отдаёт время в московском (UTC+3), владелец — Самара (UTC+4).
# У обоих нет перехода на летнее время, поэтому фиксированные offset'ы.
MSK_TZ = datetime.timezone(datetime.timedelta(hours=3), "Europe/Moscow")
SAMARA_TZ = datetime.timezone(datetime.timedelta(hours=4), "Europe/Samara")
import json
import logging
import os
import re
from urllib.parse import urlsplit
import sys
import threading
import time
from logging.handlers import RotatingFileHandler

import requests as _requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telethon import Button, TelegramClient, events
from telethon.errors import MessageNotModifiedError
from telethon.network.connection import ConnectionTcpMTProxyRandomizedIntermediate
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.types import BotCommand, BotCommandScopeDefault
from TelethonFakeTLS import ConnectionTcpMTProxyFakeTLS

from FunPayAPI import Account, Runner, enums, types
from FunPayAPI.common import exceptions as fp_exceptions

load_dotenv()

# Браузерный User-Agent — FunPay режет запросы с дефолтным python-requests.
DEFAULT_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

GOLDEN_KEY = os.getenv("GOLDEN_KEY", "").strip()
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TG_USER_ID = int(os.getenv("TG_USER_ID", "0"))
REQUESTS_DELAY = float(os.getenv("REQUESTS_DELAY", "5"))
USER_AGENT = os.getenv("USER_AGENT", "").strip() or None

# MTProto: api_id/api_hash (по умолчанию — публичные ключи Telegram Desktop).
_api_id = (os.getenv("API_ID", "") or "2040").strip()
_api_hash = (os.getenv("API_HASH", "") or "b18441a1ff607e10a989891a5462e627").strip()
API_ID = int(_api_id)
API_HASH = _api_hash

# MTProto-прокси (формат host:port:secret), несколько через ";"
TG_MTPROXY = os.getenv("TG_MTPROXY", "").strip()

# Автоответы покупателю (пустая строка = выключить автоответ)
AUTO_REPLY_CONFIRM = os.getenv(
    "AUTO_REPLY_CONFIRM",
    "Спасибо, что подтвердили заказ! Если не сложно, оставьте отзыв — очень поможет",
).strip()
AUTO_REPLY_REVIEW = os.getenv(
    "AUTO_REPLY_REVIEW",
    "Огромное спасибо за отзыв! Поставлю свечку за вас",
).strip()

# Напоминание покупателю, если заказ не подтверждён
REMINDER_TEXT = os.getenv(
    "REMINDER_TEXT",
    "Привет, незабудь подтвердить заказ и по возможности оставить отзыв❤️",
).strip()
REMINDER_INTERVAL_HOURS = float(os.getenv("REMINDER_INTERVAL_HOURS", "4"))

# Заявка в поддержку, если покупатель не подтвердил заказ за это время
SUPPORT_CONFIRM_HOURS = float(os.getenv("SUPPORT_CONFIRM_HOURS", "24"))
# Как часто (в минутах) проверять: не перевалил ли какой-то заказ за срок
SUPPORT_CHECK_MINUTES = float(os.getenv("SUPPORT_CHECK_MINUTES", "10"))
# {order} — номер заказа, {buyer} — ник покупателя, {hours} — срок в часах
SUPPORT_TEXT = os.getenv(
    "SUPPORT_TEXT",
    "Здравствуйте! Заказ #{order} (покупатель {buyer}) оплачен, но не подтверждён "
    "уже больше {hours:.0f} ч. Покупатель не отвечает на напоминания в чате. "
    "Прошу подтвердить заказ вручную. Спасибо!",
).strip()

# Юзеры-боты-помощники (автоответчики), чьи сообщения не уведомляем.
# Через запятую, дополняется в .env: NOTIFY_IGNORE_USERS=SellNode,OtherBot,21131351
# Поддерживаются ники и числовые ID авторов (ID надёжнее — ники меняются)
NOTIFY_IGNORE_USERS = {
    u.strip().lower() for u in
    os.getenv("NOTIFY_IGNORE_USERS", "sellnode,21131351").split(",") if u.strip()
}
NOTIFY_IGNORE_IDS = {
    int(u) for u in NOTIFY_IGNORE_USERS if u.isdigit()
}

# Какие системные события FunPay присылать в Telegram
NOTIFY_SYSTEM_TYPES = {
    enums.MessageTypes.ORDER_PURCHASED: "🛒 Заказ оплачен",
    enums.MessageTypes.ORDER_CONFIRMED: "✅ Заказ подтверждён",
    enums.MessageTypes.ORDER_CONFIRMED_BY_ADMIN: "✅ Заказ подтверждён (админом)",
    enums.MessageTypes.NEW_FEEDBACK: "⭐ Новый отзыв",
    enums.MessageTypes.FEEDBACK_CHANGED: "🔄 Отзыв изменён",
    enums.MessageTypes.FEEDBACK_DELETED: "🗑 Отзыв удалён",
    enums.MessageTypes.NEW_FEEDBACK_ANSWER: "💬 Новый ответ на отзыв",
    enums.MessageTypes.ORDER_REOPENED: "⚠️ Заказ открыт повторно",
    enums.MessageTypes.REFUND: "💰 Возврат средств",
    enums.MessageTypes.PARTIAL_REFUND: "💰 Частичный возврат",
    enums.MessageTypes.DISCORD: "⚠️ Предупреждение FunPay",
}

_LOG_FMT = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
_root = logging.getLogger()
_root.setLevel(logging.INFO)

# Консоль — только если она есть (при автозапуске через pythonw её нет)
if sys.stderr is not None:
    _sh = logging.StreamHandler()
    _sh.setFormatter(_LOG_FMT)
    _root.addHandler(_sh)

# Дублируем лог в файл (нужно при скрытом автозапуске без консоли)
try:
    LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
    os.makedirs(LOG_DIR, exist_ok=True)
    LOG_FILE = os.path.join(LOG_DIR, "bot.log")
    _fh = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8",
    )
    _fh.setFormatter(_LOG_FMT)
    _root.addHandler(_fh)
except Exception:
    LOG_FILE = None

logging.getLogger("telethon").setLevel(logging.WARNING)
log = logging.getLogger("funpay-tg")

# ---------------------------------------------------------------------------- #
#                              Глобальные объекты                              #
# ---------------------------------------------------------------------------- #

acc: Account | None = None          # аккаунт FunPay
funpay_runner: Runner | None = None # Runner (нужен, чтобы помечать свои ответы)
events_queue: asyncio.Queue | None = None
chat_names: dict[int, str] = {}     # {ID чата: название} — запоминаем при уведомлениях
pending_reply: dict[str, int] = {}  # {"chat_id": N} — ждём текст ответа от владельца

# Заготовленные ответы (по частым вопросам покупателей): (подпись кнопки, текст).
PRESET_REPLIES: list[tuple[str, str]] = [
    ("🛒 Как купить",
     "Здравствуйте! Всё просто: нажимаете «Купить» на лоте и оплачиваете — данные аккаунта "
     "приходят автоматически сразу после оплаты в этот чат (автовыдача)."),
    ("🔑 Пароль не подходит",
     "Проверьте ввод: буквы l и I очень похожи (маленькая L и большая i) — копируйте пароль "
     "прямо из сообщения, без пробелов. Если всё равно не входит — напишите сюда, разберёмся!"),
    ("⭐ Как оставить отзыв",
     "Спасибо! После подтверждения заказа FunPay сам предложит оставить отзыв в заказе — "
     "буду очень благодарен 🙏"),
    ("✅ Напомнить подтвердить",
     "Привет! Если аккаунт проверили и всё в порядке — пожалуйста, нажми «Подтвердить "
     "выполнение заказа» в заказе на FunPay. Это очень важно для меня)"),
    ("💰 Про цену/скидку",
     "Цена уже минимальная, без торга — извиняй) Ещё лоты по разным серверам — в моём профиле."),
    ("🧹 Чистота аккаунта",
     "Аккаунт чистый: привязок нет (почта/VK/TG не привязаны), промокоды не использовались, "
     "всё ровно как в описании лота."),
    ("📊 Характеристики",
     "Уровень, баланс и имущество — точно как в описании лота. Нужен другой сервер? "
     "Посмотри все варианты в моём профиле."),
]
tg: TelegramClient | None = None
bot_username: str = ""              # @username нашего бота (для подсказок)

# Страховочный обходчик: {(ID чата: текст последнего сообщения)}
sweep_seen: dict[int, str] = {}
SWEEP_INTERVAL = 60  # сек между проверками непрочитанных чатов

# Последние известные ID сообщений по чатам (для дельта-запросов к FunPay)
node_last_ids: dict[int, int] = {}

# Журнал автоответов: [(chat_id, сигнатура системного сообщения)]
auto_reply_sent: list[tuple[int, str]] = []

# Забаненные пользователи FunPay: {chat_id: {"name": ..., "ts": ...}} — bans.json
banned_users: dict[int, dict] = {}
BANS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bans.json")

# Сообщения, накопившиеся ПОКА БОТ БЫЛ ВЫКЛЮЧЕН (показываются по кнопке)
offline_hits: list = []


def _load_bans():
    global banned_users
    try:
        with open(BANS_FILE, encoding="utf-8") as f:
            banned_users = {int(k): v for k, v in json.load(f).items()}
    except Exception:
        banned_users = {}


def _save_bans():
    try:
        with open(BANS_FILE, "w", encoding="utf-8") as f:
            json.dump({str(k): v for k, v in banned_users.items()}, f,
                      ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Не удалось сохранить bans.json: %s", e)


def is_banned(chat_id: int) -> bool:
    return chat_id in banned_users


def ban_user(chat_id: int, name: str | None):
    banned_users[chat_id] = {"name": name or str(chat_id), "ts": time.time()}
    _save_bans()
    log.info("🚫 Пользователь забанен: %s (%s)", name or chat_id, chat_id)


def unban_user(chat_id: int) -> bool:
    if chat_id in banned_users:
        name = banned_users.pop(chat_id).get("name")
        _save_bans()
        log.info("✅ Пользователь разбанен: %s (%s)", name, chat_id)
        return True
    return False

# Напоминания о подтверждении: {order_id: unix_time последнего напоминания}
reminders_last: dict[str, float] = {}
REMINDERS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reminders.json")

# --- Управление лотами ---
lots_selected: set[int] = set()   # выбранные ID лотов
lots_cache: list[dict] = []       # [{id, title, price}]
pending_price: list[int] | None = None  # ждём ввод новой цены для этих лотов
LOTS_LIMIT = 30
MARK_OFF = "➕"
MARK_ON = "✔️"

# Защита от двойного запуска (два бота на один аккаунт FunPay = ошибки и капчи)
LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")
_lock_owned = False


# ---------------------------------------------------------------------------- #
#                       Патчи под новый формат FunPay                          #
# ---------------------------------------------------------------------------- #
# 1) Старая FunPayAPI шлёт запросы через голый requests с ручной кукой
#    golden_key. FunPay теперь выдаёт cookie "golden_seal" и требует её для
#    POST /runner/ — поэтому все запросы идут через requests.Session, которая
#    сама хранит и обновляет куки, плюс блокировка от параллельных всплесков.
# 2) FunPay изменил формат chat_node: messages теперь список объектов
#    {id, author, html}, текст в div.chat-msg-text. Штатные парсер историй и
#    отправка сообщений падают — ниже свои реализации под новый формат.

_fp_lock = threading.Lock()
_last_request_ts = {"t": 0.0}
MIN_REQUEST_INTERVAL = 1.0  # сек между POST /runner/ (антифлуд FunPay)
MIN_GET_INTERVAL = 0.2      # сек между остальными запросами (страницы лотов и т.п.)


def patch_account_method(acc: Account):
    sess = _requests.Session()
    sess.headers["user-agent"] = acc.user_agent or DEFAULT_UA
    sess.cookies.set("golden_key", acc.golden_key, domain="funpay.com")

    def method(request_method, api_method, headers, payload,
               exclude_phpsessid=False, raise_not_200=False):
        with _fp_lock:
            link = api_method if api_method.startswith("https://funpay.com") else "https://funpay.com/" + api_method
            interval = MIN_REQUEST_INTERVAL if "runner/" in link else MIN_GET_INTERVAL
            wait = interval - (time.time() - _last_request_ts["t"])
            if wait > 0:
                time.sleep(wait)
            if exclude_phpsessid:
                sess.cookies.pop("PHPSESSID", None)
            response = sess.request(
                request_method, link, headers=dict(headers or {}), data=payload,
                timeout=acc.requests_timeout, proxies=acc.proxy or {},
            )
            _last_request_ts["t"] = time.time()
        if response.status_code == 403:
            raise fp_exceptions.UnauthorizedError(response)
        if response.status_code != 200 and raise_not_200:
            raise fp_exceptions.RequestFailedError(response)
        return response

    acc.method = method


def _parse_msg_html(raw_html: str) -> tuple[str | None, str | None]:
    """(текст, ссылка на изображение) из html сообщения нового формата."""
    soup = BeautifulSoup(raw_html, "html.parser")
    img_link = None
    if (a := soup.find("a", {"class": "chat-img-link"})) is not None:
        img_link = a.get("href")
    elif soup.find("img") is not None:
        img_link = ""
    text_div = soup.find("div", {"class": "chat-msg-text"})
    if text_div is not None:
        text = text_div.get_text(" ", strip=True) or None
    else:
        text = None  # картинка/медиа без текста
    # ВАЖНО: заглушка должна совпадать с превью в списке чатов FunPay
    # ("Изображение"), иначе ломается дедупликация Runner'а (init_messages)
    # и после рестарта возможен спам старыми сообщениями.
    if text is None:
        text = "Изображение"
    return text, img_link


def patch_account_parsers(acc: Account):
    bot_char = getattr(acc, "_Account__bot_character", "⁤")

    def patched_get_chats_histories(chats_data: dict) -> dict:
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
        }
        result = {}
        # Каждый чат — отдельным POST-ом, с last_message = ID последнего
        # известного сообщения (как у актуальных клиентов FunPay). Пустой
        # ответ в таком формате означает "нет нового" — это нормально.
        for chat_id in chats_data:
            chat_name = chats_data.get(chat_id)
            last_id = node_last_ids.get(chat_id, -1)
            objects = [{"type": "chat_node", "id": chat_id, "tag": "00000000",
                        "data": {"node": chat_id, "last_message": last_id, "content": ""}}]
            payload = {"objects": json.dumps(objects), "request": False, "csrf_token": acc.csrf_token}

            messages_raw = None
            # Один запрос; пустой ответ = "нет нового" (норма для дельта-режима).
            # Ошибки HTTP/сети прокинутся выше — их Runner ретраит сам.
            response = acc.method("post", "runner/", headers, payload, raise_not_200=True)
            json_response = response.json()
            data = None
            for obj in json_response.get("objects") or []:
                if obj.get("type") == "chat_node":
                    data = obj.get("data")
                    break
            if data is not None and data.get("messages") is not None:
                messages_raw = data["messages"]

            messages = []
            prev_author_name, prev_author_id = chat_name, None
            for m in messages_raw or []:
                raw_html = m.get("html") or ""
                soup = BeautifulSoup(raw_html, "html.parser")
                text, img_link = _parse_msg_html(raw_html)
                if text and text.startswith(bot_char):
                    text = text.replace(bot_char, "", 1)  # невидимый префикс бота
                author_id = int(m.get("author") or 0)
                author_link = soup.find("a", {"class": "chat-msg-author-link"})
                if author_link is not None:
                    author_name = author_link.text  # у сообщения есть "шапка"
                elif author_id == acc.id:
                    author_name = acc.username
                elif prev_author_id == author_id:
                    author_name = prev_author_name  # продолжение серии сообщений
                else:
                    author_name = chat_name if chat_name else str(author_id or "?")
                prev_author_name, prev_author_id = author_name, author_id
                messages.append(types.Message(
                    int(m.get("id") or 0), text, chat_id, chat_name,
                    author_name, author_id, raw_html, img_link,
                ))
            if messages_raw:
                node_last_ids[chat_id] = max(int(m.get("id") or 0) for m in messages_raw)
            result[chat_id] = messages
        return result

    def patched_send_message(chat_id, text=None, chat_name=None, image_id=None,
                             add_to_ignore_list=True, update_last_saved_message=False):
        headers = {
            "accept": "*/*",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
        }
        request = {
            "action": "chat_message",
            "data": {"node": chat_id, "last_message": -1,
                     "content": f"{bot_char}{text}" if text else ""},
        }
        if image_id is not None:
            request["data"]["image_id"] = image_id
            request["data"]["content"] = ""
        objects = [{
            "type": "chat_node", "id": chat_id, "tag": "00000000",
            "data": {"node": chat_id, "last_message": -1, "content": ""},
        }]
        payload = {
            "objects": json.dumps(objects),
            "request": json.dumps(request),
            "csrf_token": acc.csrf_token,
        }
        response = acc.method("post", "runner/", headers, payload, raise_not_200=True)
        json_response = response.json()
        if not json_response.get("response"):
            raise fp_exceptions.MessageNotDeliveredError(response, None, chat_id)
        if (error_text := json_response["response"].get("error")) is not None:
            raise fp_exceptions.MessageNotDeliveredError(response, error_text, chat_id)

        mes = json_response["objects"][0]["data"]["messages"][-1]
        raw_html = mes.get("html") or ""
        text_parsed, img_link = _parse_msg_html(raw_html)
        if text_parsed:
            text_parsed = text_parsed.replace(bot_char, "", 1)
        message_obj = types.Message(int(mes.get("id") or 0), text_parsed, chat_id, chat_name,
                                    acc.username, acc.id, raw_html, img_link)
        if isinstance(chat_id, int) and message_obj.id:
            node_last_ids[chat_id] = max(node_last_ids.get(chat_id, 0), message_obj.id)
        if acc.runner and isinstance(chat_id, int):
            if add_to_ignore_list:
                acc.runner.mark_as_by_bot(chat_id, message_obj.id)
            if update_last_saved_message:
                acc.runner.update_last_message(chat_id, text_parsed)
        return message_obj

    acc.get_chats_histories = patched_get_chats_histories
    acc.send_message = patched_send_message

    # --- Патч редактирования лотов: FunPay отдаёт форму лота как HTML,
    #     а старый код ждёт JSON. Плюс checkout новых полей. ---
    def patched_get_lot_fields(lot_id):
        headers = {"accept": "*/*", "content-type": "application/json",
                   "x-requested-with": "XMLHttpRequest"}
        response = acc.method("get", f"lots/offerEdit?offer={lot_id}", headers, {}, raise_not_200=True)
        html_text = response.text
        try:
            html_text = response.json().get("html") or html_text
        except Exception:
            pass
        bs = BeautifulSoup(html_text, "html.parser")
        result = {"active": "", "deactivate_after_sale": ""}
        result.update({field["name"]: field.get("value") or "" for field in bs.find_all("input")
                       if field["name"] not in ["active", "deactivate_after_sale"]})
        result.update({field["name"]: field.text or "" for field in bs.find_all("textarea")})
        result.update({field["name"]: field.find("option", selected=True)["value"]
                       for field in bs.find_all("select")})
        result.update({field["name"]: "on" for field in bs.find_all("input", {"type": "checkbox"}, checked=True)})
        return types.LotFields(lot_id, result)

    def patched_save_lot(lot_fields):
        headers = {
            "accept": "application/json, text/javascript, */*; q=0.01",
            "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
            "x-requested-with": "XMLHttpRequest",
            "referer": f"https://funpay.com/lots/offerEdit?offer={lot_fields.lot_id}",
        }
        fields = lot_fields.renew_fields().fields
        fields["location"] = "trade"
        response = acc.method("post", "lots/offerSave", headers, fields, raise_not_200=True)
        try:
            json_response = response.json()
            if json_response.get("error"):
                raise fp_exceptions.LotSavingError(
                    response, "; ".join(json_response.get("errors") or []) or "unknown", lot_fields.lot_id)
        except ValueError:
            pass  # не JSON — считаем успехом, если код 200
        return response

    acc.get_lot_fields = patched_get_lot_fields
    acc.save_lot = patched_save_lot


# ---------------------------------------------------------------------------- #
#                              Поток с FunPay                                   #
# ---------------------------------------------------------------------------- #

def funpay_worker(loop: asyncio.AbstractEventLoop, queue: asyncio.Queue):
    """Работает в отдельном потоке: слушает события FunPay и кидает их в очередь."""
    global funpay_runner
    while True:
        try:
            funpay_runner = Runner(acc)
            log.info("Runner запущен, опрос каждые %.1f сек", REQUESTS_DELAY)
            for event in funpay_runner.listen(requests_delay=REQUESTS_DELAY, ignore_exceptions=True):
                asyncio.run_coroutine_threadsafe(queue.put(event), loop)
        except Exception as e:
            log.error("FunPay Runner остановлен (%s). Перезапуск через 30 сек...", e)
            time.sleep(30)
            try:
                acc.get()  # обновит куки и данные аккаунта
            except Exception:
                log.exception("Не удалось переподключиться к FunPay")


def funpay_session_refresher():
    """Раз в 45 минут обновляет сессию FunPay (csrf/PHPSESSID), чтобы
    после многочасовой работы не начались 403/пустые ответы."""
    while True:
        time.sleep(45 * 60)
        try:
            acc.get()
            log.info("Сессия FunPay обновлена (csrf/PHPSESSID)")
        except Exception as e:
            log.warning("Не удалось обновить сессию FunPay: %s", e)


# ---------------------------------------------------------------------------- #
#                  Напоминания о подтверждении заказа                           #
# ---------------------------------------------------------------------------- #

def _load_reminders():
    global reminders_last
    try:
        with open(REMINDERS_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            now = time.time()
            reminders_last = {
                str(k): float(v) for k, v in data.items()
                if k == "__last_run__" or now - float(v) < 7 * 24 * 3600
            }
    except Exception:
        reminders_last = {}


def _save_reminders():
    try:
        with open(REMINDERS_FILE, "w", encoding="utf-8") as f:
            json.dump(reminders_last, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning("Не удалось сохранить reminders.json: %s", e)


def reminder_delay() -> float:
    """Сколько секунд осталось до следующего цикла напоминаний."""
    last = reminders_last.get("__last_run__")
    if last is None:
        return 0.0
    return max(0.0, REMINDER_INTERVAL_HOURS * 3600 - (time.time() - last))


def _order_paid_ts(order) -> float | None:
    """Время оплаты заказа (из FunPay) как unix timestamp.
    FunPay отдаёт naive datetime в московском времени (UTC+3) —
    помечаем зоной явно, иначе системная таймзона ПК искажает время на часы."""
    date = getattr(order, "date", None)
    try:
        if date is None:
            return None
        if date.tzinfo is None:
            date = date.replace(tzinfo=MSK_TZ)
        return date.timestamp()
    except Exception:
        return None


def _ticket_list_ids() -> set[int]:
    """ID существующих тикетов на support.funpay.com (для проверки создания)."""
    ua = USER_AGENT or DEFAULT_UA
    s = _requests.Session()
    s.headers.update({"user-agent": ua, "accept": "text/html,application/xhtml+xml"})
    s.cookies.set("golden_key", acc.golden_key, domain="funpay.com")

    def isolate(host: str):
        for c in list(s.cookies):
            if c.domain.lstrip(".") != host:
                s.cookies.clear(c.domain, c.path, c.name)

    url = "https://funpay.com/support/sso?return_to=/tickets"
    for _ in range(4):
        sp = urlsplit(url)
        isolate(sp.netloc)
        r = s.get(url, timeout=15, allow_redirects=False)
        loc = r.headers.get("location", "")
        if r.status_code in (301, 302, 303, 307, 308) and loc:
            url = loc if loc.startswith("http") else f"{sp.scheme}://{sp.netloc}{loc}"
            continue
        break
    isolate("support.funpay.com")
    r = s.get("https://support.funpay.com/tickets", timeout=15)
    return {int(x) for x in re.findall(r'href="/tickets/(\d+)"', r.content.decode("utf-8", "replace"))}


def send_support_ticket(orders: list[tuple[str, str]]) -> int:
    """Создаёт тикет «Покупатель забыл подтвердить заказ» на support.funpay.com.

    orders: [(order_id, buyer_username), ...] — можно НЕСКОЛЬКО заказов одним
    тикетом: FunPay принимает список через запятую в поле «Номер заказа»
    (проверено тикетом #16123385) и подтверждает все перечисленные заказы.

    Диалога с саппортом в чатах FunPay нет, поэтому официальный путь:
    SSO funpay.com/support/sso (JWT) -> кука сессии support.funpay.com ->
    форма /tickets/new/1 (Проблема с заказом, роль «Продавец»).
    Лимит FunPay: одна такая заявка в сутки — при превышении сервер вернёт
    {"error": "Запросы на подтверждение заказов отправляются 1 раз в сутки."}
    Возвращает номер созданного тикета (проверено по списку заявок).
    """
    ua = USER_AGENT or DEFAULT_UA
    s = _requests.Session()
    s.headers.update({"user-agent": ua, "accept": "text/html,application/xhtml+xml",
                      "accept-language": "ru,en;q=0.9"})
    s.cookies.set("golden_key", acc.golden_key, domain="funpay.com")

    def isolate(host: str):
        # requests тащит куки funpay.com на поддомен, а чужая PHPSESSID
        # сбрасывает SSO-сессию поддержки — шлём только куки точного хоста
        for c in list(s.cookies):
            if c.domain.lstrip(".") != host:
                s.cookies.clear(c.domain, c.path, c.name)

    old_ids = _ticket_list_ids()

    url = "https://funpay.com/support/sso?return_to=/tickets/new"
    for _ in range(4):
        sp = urlsplit(url)
        isolate(sp.netloc)
        r = s.get(url, timeout=15, allow_redirects=False)
        loc = r.headers.get("location", "")
        if r.status_code in (301, 302, 303, 307, 308) and loc:
            url = loc if loc.startswith("http") else f"{sp.scheme}://{sp.netloc}{loc}"
            continue
        break

    isolate("support.funpay.com")
    r = s.get("https://support.funpay.com/tickets/new/1", timeout=15)
    html = r.content.decode("utf-8", "replace")
    token = re.search(r'name="ticket\[_token\]"[^>]*value="([^"]+)"', html)
    if not token:
        raise RuntimeError("не удалось получить CSRF-токен формы заявки")

    ids_str = ", ".join(oid for oid, _ in orders)
    if len(orders) == 1:
        body = SUPPORT_TEXT.format(order=orders[0][0], buyer=orders[0][1],
                                   hours=SUPPORT_CONFIRM_HOURS)
    else:
        # мульти-тикет: перечисляем все заказы с покупателями
        listed = "; ".join(f"#{oid} (покупатель {buyer})" for oid, buyer in orders)
        body = (f"Здравствуйте! Перечисленные заказы оплачены, но не подтверждены "
                f"уже больше {SUPPORT_CONFIRM_HOURS:.0f} ч. Покупатели не отвечают "
                f"на напоминания в чате. Прошу подтвердить заказы вручную: {listed}. Спасибо!")

    data = {
        "ticket[fields][1]": acc.username,
        "ticket[fields][2]": ids_str,
        "ticket[fields][3]": "2",   # роль: продавец
        "ticket[fields][5]": "201",  # тема: покупатель забыл подтвердить заказ
        "ticket[comment][body_html]": body,
        "ticket[_token]": token.group(1),
    }
    s.headers["referer"] = "https://support.funpay.com/tickets/new/1"
    isolate("support.funpay.com")
    r2 = s.post("https://support.funpay.com/tickets/create/1", data=data, timeout=15)
    if r2.status_code != 200:
        raise RuntimeError(f"тикет не создан (HTTP {r2.status_code})")
    try:
        err = json.loads(r2.content.decode("utf-8", "replace")).get("error")
    except ValueError:
        err = None
    if err:
        raise RuntimeError(f"поддержка отклонила заявку: {err}")

    # подтверждение: в списке заявок появился новый ID
    new_ids = _ticket_list_ids()
    created = new_ids - old_ids
    if not created:
        raise RuntimeError("тикет не появился в списке заявок")
    return created.pop()


def remind_unconfirmed() -> tuple[int, int]:
    """Напоминает покупателям о неподтверждённых заказах.
    Первое напоминание — не раньше чем через интервал после оплаты,
    дальше — каждые N часов, пока заказ не подтвердят.
    (Заявки в поддержку шлёт check_support_tickets — лимит 1 в сутки.)
    Возвращает (напоминаний отправлено, всего неподтверждённых)."""
    if not REMINDER_TEXT and not SUPPORT_TEXT:
        return 0, 0
    try:
        _, orders = acc.get_sells(state="paid")
    except Exception as e:
        log.warning("Напоминания: не удалось получить заказы: %s", e)
        return 0, 0

    now = time.time()
    interval = REMINDER_INTERVAL_HOURS * 3600
    sent = 0
    for order in orders:
        oid = str(order.id)
        buyer = getattr(order, "buyer_username", None)
        paid = _order_paid_ts(order)
        if not buyer:
            continue

        # напоминание покупателю
        last = reminders_last.get(oid)
        due = (now - last >= interval * 0.95) if last is not None else (
            paid is not None and now - paid >= interval * 0.95)
        if due:
            try:
                chat = acc.get_chat_by_name(buyer, True)
                if chat is not None:
                    send_funpay_message(chat.id, REMINDER_TEXT)
                    reminders_last[oid] = now
                    sent += 1
                    log.info("Напоминание о подтверждении отправлено %s (заказ %s)", buyer, oid)
            except Exception:
                log.exception("Не удалось отправить напоминание по заказу %s", oid)

    # чистим записи по уже подтверждённым/закрытым заказам
    paid_ids = {str(o.id) for o in orders}
    for oid in list(reminders_last):
        if oid.startswith("__"):
            continue
        base = oid[: -len(":support")] if oid.endswith(":support") else oid
        if base not in paid_ids:
            del reminders_last[oid]
    reminders_last["__last_run__"] = now
    _save_reminders()
    return sent, len(orders)


async def reminder_loop():
    """Раз в REMINDER_INTERVAL_HOURS часов напоминает о неподтверждённых заказах.
    Таймер хранится в reminders.json и переживает перезапуски бота."""
    if not REMINDER_TEXT and not SUPPORT_TEXT:
        log.info("Напоминания о подтверждении отключены (пустые REMINDER_TEXT и SUPPORT_TEXT)")
        return
    await asyncio.to_thread(_load_reminders)
    if "__last_run__" not in reminders_last:
        # самый первый запуск: начинаем отсчёт с текущего момента
        reminders_last["__last_run__"] = time.time()
        await asyncio.to_thread(_save_reminders)
    while True:
        delay = reminder_delay()
        if delay > 0:
            log.info("Напоминания о подтверждении: следующий запуск через %.1f ч", delay / 3600)
            await asyncio.sleep(delay)
        try:
            sent, total = await asyncio.to_thread(remind_unconfirmed)
            if total:
                log.info("Напоминания: отправлено %d, неподтверждённых заказов %d", sent, total)
        except Exception:
            log.exception("Ошибка цикла напоминаний")
            await asyncio.sleep(60)


def check_support_tickets() -> list[str]:
    """Ищет заказы, не подтверждённые дольше SUPPORT_CONFIRM_HOURS, и шлёт
    ОДИН тикет со ВСЕМИ зрелыми заказами сразу (FunPay принимает список через
    запятую в поле «Номер заказа» и подтверждает все перечисленные).
    Заявка в поддержку не чаще раза в сутки (лимит FunPay).
    Возвращает строку «заказы ..., тикет #Z» для уведомления."""
    if not SUPPORT_TEXT:
        return []
    try:
        _, orders = acc.get_sells(state="paid")
    except Exception as e:
        log.warning("Поддержка: не удалось получить заказы: %s", e)
        return []

    now = time.time()
    # заявка в поддержку не чаще раза в сутки (лимит FunPay)
    last_ticket = reminders_last.get("__last_support_ticket__", 0)
    if last_ticket and now - last_ticket < 24 * 3600 * 0.98:
        return []

    # кандидаты: перевалили за срок, тикет ещё не отправляли
    candidates = []
    for order in orders:
        oid = str(order.id)
        if reminders_last.get(f"{oid}:support"):
            continue
        paid = _order_paid_ts(order)
        if paid is None or now - paid < SUPPORT_CONFIRM_HOURS * 3600:
            continue
        buyer = getattr(order, "buyer_username", None)
        if not buyer:
            continue
        candidates.append((paid, oid, buyer))
    if not candidates:
        return []
    candidates.sort()  # самый старый заказ первым

    try:
        orders_arg = [(oid, buyer) for _, oid, buyer in candidates]
        ticket_id = send_support_ticket(orders_arg)
    except Exception as e:
        log.warning("Мульти-тикет в поддержку (%d заказов) не принят: %s", len(candidates), e)
        return []

    now = time.time()
    for _, oid, buyer in candidates:
        reminders_last[f"{oid}:support"] = now
    reminders_last["__last_support_ticket__"] = now
    _save_reminders()
    listing = ", ".join(f"#{oid}" for _, oid, _ in candidates)
    log.info("Мульти-тикет в поддержку отправлен (%d заказов: %s): тикет #%s",
             len(candidates), listing, ticket_id)
    return [f"{listing} (тикет <b>#{ticket_id}</b>)"]


async def support_loop():
    """Каждые SUPPORT_CHECK_MINUTES минут проверяет неподтверждённые заказы:
    как только заказ перевалил за SUPPORT_CONFIRM_HOURS — тикет сразу,
    не дожидаясь планового прохода напоминаний."""
    if not SUPPORT_TEXT:
        log.info("Тикеты в поддержку отключены (пустой SUPPORT_TEXT)")
        return
    await asyncio.to_thread(_load_reminders)
    while True:
        try:
            # авточистка пустых лотов (amount=0) вместе с проверкой тикетов
            removed = await asyncio.to_thread(purge_empty_lots)
            if removed:
                log.info("Автоочистка: удалено пустых лотов: %d", len(removed))
            done = await asyncio.to_thread(check_support_tickets)
            if done:
                await notify(
                    f"⏰ Заказы не подтверждены дольше {SUPPORT_CONFIRM_HOURS:.0f} ч — "
                    f"мульти-заявка в поддержку отправлена: {done[0]}",
                    buttons=Button.clear(),
                )
        except Exception:
            log.exception("Ошибка цикла заявок в поддержку")
            await asyncio.sleep(60)
        await asyncio.sleep(SUPPORT_CHECK_MINUTES * 60)


def send_funpay_message(chat_id: int, text: str):
    """Блокирующая отправка сообщения в FunPay (вызывается в отдельном потоке)."""
    msg = acc.send_message(chat_id, text, chat_name=chat_names.get(chat_id))
    sweep_seen[chat_id] = _norm(msg.text or "")
    return msg


def _get_balance_safe():
    """Баланс через страницу оффера СВОЕГО лота: библиотечный get_balance()
    по умолчанию дёргает чужой лот 18853876 — FunPay отдаёт 404."""
    lot_ids = [int(item["id"]) for item in lots_cache if item.get("id")]
    if not lot_ids:
        try:
            user = acc.get_user(acc.id)
            lot_ids = [int(l.id) for l in user.get_lots()[:3]]
        except Exception as e:
            log.warning("Статистика: не удалось получить лоты для баланса: %s", e)
    for lid in lot_ids[:3]:
        try:
            return acc.get_balance(lid)
        except Exception as e:
            log.warning("Статистика: баланс через лот %s не удался: %s", lid, e)
    return None


def collect_stats() -> dict:
    """Баланс + заказы за сегодня и за 7 дней + все неподтверждённые.
    Блокирующая — вызывать в отдельном потоке."""
    balance = _get_balance_safe()

    midnight = datetime.datetime.now(SAMARA_TZ).replace(
        hour=0, minute=0, second=0, microsecond=0).timestamp()
    week_start = time.time() - 7 * 24 * 3600

    week: list = []
    early_stop = False
    next_from = None
    for _ in range(25):  # предохранитель от бесконечной пагинации
        nf, batch = acc.get_sells(start_from=next_from)
        if not batch:
            break
        older = False
        for o in batch:
            ts = _order_paid_ts(o)
            if ts is not None and ts >= week_start:
                week.append(o)
            elif ts is not None:
                # список идёт от свежих к старым — дальше все старше недели
                older = True
        if older:
            break
        if nf is None:
            break
        next_from = nf
    else:
        early_stop = True  # упёрлись в лимит страниц, а данные ещё есть

    return _finish_stats(balance, week, midnight, early_stop)


def _bucket(orders: list) -> dict:
    """Счётчики по списку заказов + выручка.
    Выручка = подтверждённые + неподтверждённые − возвраты."""
    closed = [o for o in orders if o.status == types.OrderStatuses.CLOSED]
    paid = [o for o in orders if o.status == types.OrderStatuses.PAID]
    refunded = [o for o in orders if o.status == types.OrderStatuses.REFUNDED]
    earned = sum(float(o.price or 0) for o in closed) + sum(float(o.price or 0) for o in paid)
    refund = sum(float(o.price or 0) for o in refunded)
    sold = len(closed) + len(paid)
    return {
        "total": len(orders),
        "closed": len(closed),
        "paid": len(paid),
        "refunded": len(refunded),
        "sold": sold,
        "earned": earned,
        "refund": refund,
        "revenue": earned - refund,
    }


def _finish_stats(balance, week: list, midnight: float, early_stop: bool = False) -> dict:
    week_stats = _bucket(week)
    today_stats = _bucket([o for o in week
                           if (ts := _order_paid_ts(o)) is not None and ts >= midnight])

    # все неподтверждённые сейчас, независимо от даты
    pending = []
    try:
        _, pending = acc.get_sells(state="paid")
    except Exception as e:
        log.warning("Статистика: не удалось получить неподтверждённые: %s", e)
    day_ago = time.time() - 24 * 3600
    pending_old = 0     # старше 24 ч всего
    pending_ripe = 0    # старше 24 ч, тикет ещё не отправляли
    pending_due = None  # когда ближайший висяк перевалит за 24 ч
    for o in pending:
        ts = _order_paid_ts(o)
        if ts is None:
            continue
        if ts < day_ago:
            pending_old += 1
            if not reminders_last.get(f"{o.id}:support"):
                pending_ripe += 1
        due = ts + SUPPORT_CONFIRM_HOURS * 3600
        if pending_due is None or due < pending_due:
            pending_due = due

    # покупатели и рекорды за неделю
    buyers: dict[str, int] = {}
    top_sale = None  # (цена, описание)
    for o in week:
        buyer = getattr(o, "buyer_username", None)
        if buyer:
            buyers[buyer] = buyers.get(buyer, 0) + 1
        price = float(o.price or 0)
        if top_sale is None or price > top_sale[0]:
            top_sale = (price, str(o.description or ""))

    return {
        "balance": balance,
        "today": today_stats,
        "week": week_stats,
        "buyers_total": len(buyers),
        "top_buyer": max(buyers.items(), key=lambda kv: kv[1]) if buyers else None,
        "top_sale": top_sale,
        "pending_total": len(pending),
        "pending_old": pending_old,
        "pending_ripe": pending_ripe,
        "pending_due": pending_due,
        "early_stop": early_stop,
    }


def _fmt_hm(seconds: float) -> str:
    m = max(0, int(seconds // 60))
    h, m = divmod(m, 60)
    return f"{h} ч {m:02d} мин" if h else f"{m} мин"


def _order_server(order) -> str:
    """Имя сервера из описания заказа: '№24 SPB, 🔥 SPB | ...' -> 'SPB'."""
    m = re.match(r"\s*№\d+\s+([^,|·]+)", str(order.description or ""))
    name = m.group(1).strip() if m else ""
    name = re.sub(r"[^\w\s\-\.]", "", name).strip()
    return name.upper()[:24] if name else "—"


def collect_top(days: int = 30) -> list[dict]:
    """Продажи по серверам за N дней: кол-во, выручка, мин/медиана/макс цена.
    Блокирующая — вызывать в отдельном потоке."""
    since = time.time() - days * 24 * 3600
    orders: list = []
    next_from = None
    for _ in range(25):  # предохранитель от бесконечной пагинации
        nf, batch = acc.get_sells(start_from=next_from)
        if not batch:
            break
        older = False
        for o in batch:
            ts = _order_paid_ts(o)
            if ts is not None and ts >= since:
                orders.append(o)
            elif ts is not None:
                older = True
        if older:
            break
        if nf is None:
            break
        next_from = nf

    by_server: dict[str, list[float]] = {}
    for o in orders:
        by_server.setdefault(_order_server(o), []).append(float(o.price or 0))

    def median(vals: list[float]) -> float:
        s = sorted(vals)
        n = len(s)
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2

    rows = []
    for server, prices in by_server.items():
        rows.append({
            "server": server,
            "count": len(prices),
            "revenue": sum(prices),
            "min": min(prices),
            "med": median(prices),
            "max": max(prices),
        })
    rows.sort(key=lambda r: (-r["count"], -r["revenue"]))
    return rows


def top_text(rows: list[dict], days: int) -> str:
    lines = [f"🏆 <b>Продажи по серверам</b> (за {days} дн.)"]
    if not rows:
        lines.append("\n<i>Пока нет данных.</i>")
        return "\n".join(lines)

    # текущая цена моего лота по серверу, если лоты загружены
    my_price: dict[str, float] = {}
    for item in lots_cache:
        sp = lot_server(item.get("title", "")).upper()
        if sp and sp != "ТОВАР" and item.get("price") is not None:
            my_price[sp] = float(item["price"])

    total = sum(r["count"] for r in rows)
    lines.append(f"Всего заказов: <b>{total}</b>, серверов: <b>{len(rows)}</b>\n")
    for i, r in enumerate(rows, 1):
        share = r["count"] / total * 100
        price = (f"цена {fmt_price(r['med'])}₽"
                 if r["min"] == r["max"]
                 else f"цена {fmt_price(r['min'])}–{fmt_price(r['max'])}₽ (мед. {fmt_price(r['med'])}₽)")
        mine = my_price.get(r["server"])
        mine_s = f" • мой лот: {fmt_price(mine)}₽" if mine is not None else ""
        lines.append(
            f"{i}. <b>{esc(r['server'])}</b> — {r['count']} шт ({share:.0f}%), "
            f"выручка {fmt_price(r['revenue'])}₽\n"
            f"    {price}{mine_s}")
    return "\n".join(lines)


def support_eta_line(s: dict) -> str | None:
    """Строка про следующую заявку в поддержку для /stats.
    Учитывает суточный лимит FunPay (1 заявка) и «дозревание» заказов."""
    now = time.time()
    last_ticket = reminders_last.get("__last_support_ticket__", 0)
    quota_free_at = last_ticket + 24 * 3600 * 0.98 if last_ticket else 0
    pending = s["pending_total"]
    if not pending:
        return None

    # зрелые кандидаты: старше 24ч, тикет ещё не отправляли
    ripe = s["pending_ripe"] > 0
    if ripe and now >= quota_free_at:
        return "📮 Заявка в поддержку: уйдёт при ближайшей проверке (в течение 10 мин)"

    # ближайший момент, когда что-то реально уйдёт
    eta = max(quota_free_at, s["pending_due"] or now)
    if eta <= now:
        return "📮 Заявка в поддержку: уйдёт при ближайшей проверке (в течение 10 мин)"

    parts = [f"следующая заявка в поддержку — через <b>{_fmt_hm(eta - now)}</b>"]
    if not quota_free_at:
        parts.append("лимит FunPay — 1 раз в сутки")
    elif now < quota_free_at:
        parts.append("лимит 1 раз в сутки")
    if s["pending_due"] and s["pending_due"] > now and not ripe:
        parts.append(f"ближайший заказ дозреет за {_fmt_hm(s['pending_due'] - now)}")
    return "📮 " + ", ".join(parts)


def stats_text(s: dict) -> str:
    b = s["balance"]
    lines = [
        "📊 <b>Статистика FunPay</b>",
        "",
    ]
    if b is not None:
        lines.append(
            f"💰 Баланс: <b>{fmt_price(b.total_rub)}₽</b>"
            f" (доступно к выводу: {fmt_price(b.available_rub)}₽)")
    else:
        lines.append("💰 Баланс: <i>недоступен</i>")
    td, wk = s["today"], s["week"]
    lines += [
        "",
        "📦 <b>Заказы за сегодня</b> (с 00:00):",
        f"• Всего: <b>{td['total']}</b>",
        f"• ✅ Подтверждено: <b>{td['closed']}</b>",
        f"• ⏳ Не подтверждено: <b>{td['paid']}</b>",
    ]
    if td["refunded"]:
        lines.append(f"• ↩️ Возвратов: <b>{td['refunded']}</b>")
    lines.append(f"• Выручка за день: <b>{fmt_price(td['revenue'])}₽</b>")
    if td["sold"]:
        lines.append(f"• Средний чек: <b>{fmt_price(td['revenue'] / td['sold'])}₽</b>")

    week_line = (f"• Всего: <b>{wk['total']}</b> — ✅ {wk['closed']}, "
                 f"⏳ {wk['paid']}, ↩️ {wk['refunded']}")
    lines += [
        "",
        "📅 <b>За 7 дней</b>:",
        week_line,
        f"• Выручка: <b>{fmt_price(wk['revenue'])}₽</b>",
    ]
    if wk["sold"]:
        lines.append(f"• Средний чек: <b>{fmt_price(wk['revenue'] / wk['sold'])}₽</b>")
    if s.get("top_buyer"):
        tb, cnt = s["top_buyer"]
        lines.append(f"• Покупателей: <b>{s['buyers_total']}</b> — самый активный: "
                     f"<b>{esc(tb)}</b> ({cnt} шт)")
    if s.get("top_sale"):
        price, desc = s["top_sale"]
        lines.append(f"• Самая дорогая продажа: <b>{fmt_price(price)}₽</b> — "
                     f"<i>{esc(short_title(desc, 40))}</i>")
    lines += [
        "",
        f"⏳ Всего висит неподтверждённых: <b>{s['pending_total']}</b>"
        f" (старше 24 ч: <b>{s['pending_old']}</b>)",
    ]
    eta = support_eta_line(s)
    if eta:
        lines.append(eta)
    if s["early_stop"]:
        lines.append("\n<i>⚠️ Заказов за день много — показана статистика по последним страницам.</i>")
    return "\n".join(lines)


# ---------------------------------------------------------------------------- #
#                               Помощники/формат                                #
# ---------------------------------------------------------------------------- #

def esc(text: str) -> str:
    return html.escape(str(text or ""))


def chat_url(chat_id: int) -> str:
    return f"https://funpay.com/chat/?request={chat_id}"


def author_name(message) -> str:
    """Имя автора сообщения с запасными вариантами."""
    for attr in ("author_username", "author", "chat_name"):
        val = getattr(message, attr, None)
        if val:
            return str(val)
    return str(getattr(message, "author_id", "?"))


def chat_name(message) -> str:
    val = getattr(message, "chat_name", None) or getattr(message, "chat_title", None)
    return str(val) if val else author_name(message)


def remember_chat(chat_id: int, name: str):
    if name:
        chat_names[chat_id] = name


def short(text: str, limit: int = 3500) -> str:
    text = (text or "").strip()
    return text if len(text) <= limit else text[: limit - 1] + "…"


def fit_text(text: str, limit: int) -> str:
    """Безопасная обрезка HTML-текста (по границе слова, чтобы не порвать сущность)."""
    if len(text) <= limit:
        return text
    cut = text[: max(0, limit - 1)]
    if " " in cut:
        cut = cut.rsplit(" ", 1)[0]
    return cut + "…"


# ---------------------------------------------------------------------------- #
#                              Отзывы (страница заказа)                         #
# ---------------------------------------------------------------------------- #

REVIEW_EVENT_TYPES = {
    enums.MessageTypes.NEW_FEEDBACK,
    enums.MessageTypes.FEEDBACK_CHANGED,
    enums.MessageTypes.NEW_FEEDBACK_ANSWER,
    enums.MessageTypes.FEEDBACK_ANSWER_CHANGED,
}

CONFIRM_EVENT_TYPES = {
    enums.MessageTypes.ORDER_CONFIRMED,
    enums.MessageTypes.ORDER_CONFIRMED_BY_ADMIN,
}

# Своё определение системных сообщений: regex'ы библиотеки требуют ID заказа
# ровно из 8 символов, а у FunPay бывают и 7-символьные ID — из-за этого
# подтверждения/отзывы определялись как обычные сообщения.
_SYS_PATTERNS = [
    (re.compile(r"Покупатель \S+ подтвердил успешное выполнение заказа"),
     enums.MessageTypes.ORDER_CONFIRMED),
    (re.compile(r"Администратор \S+ подтвердил успешное выполнение заказа"),
     enums.MessageTypes.ORDER_CONFIRMED_BY_ADMIN),
    (re.compile(r"Покупатель \S+ написал отзыв к заказу"),
     enums.MessageTypes.NEW_FEEDBACK),
    (re.compile(r"Покупатель \S+ изменил отзыв к заказу"),
     enums.MessageTypes.FEEDBACK_CHANGED),
    (re.compile(r"Покупатель \S+ удалил отзыв к заказу"),
     enums.MessageTypes.FEEDBACK_DELETED),
    (re.compile(r"Продавец \S+ ответил на отзыв к заказу"),
     enums.MessageTypes.NEW_FEEDBACK_ANSWER),
    (re.compile(r"Покупатель \S+ оплатил заказ"),
     enums.MessageTypes.ORDER_PURCHASED),
    (re.compile(r"Продавец \S+ вернул деньги покупателю"),
     enums.MessageTypes.REFUND),
    (re.compile(r"Часть средств по заказу"),
     enums.MessageTypes.PARTIAL_REFUND),
    (re.compile(r"Заказ #\S+ открыт повторно"),
     enums.MessageTypes.ORDER_REOPENED),
]


def effective_type(text: str | None, fallback) -> enums.MessageTypes:
    """Уточняет тип системного сообщения по тексту (обходит баг {8})."""
    if fallback and fallback is not enums.MessageTypes.NON_SYSTEM:
        return fallback
    if not text:
        return fallback
    for rx, mtype in _SYS_PATTERNS:
        if rx.search(text):
            return mtype
    return fallback


def _auto_already_sent(chat_id: int, signature: str) -> bool:
    return any(cid == chat_id and _same_msg(sig, signature)
               for cid, sig in auto_reply_sent[-200:])


def _mark_auto_sent(chat_id: int, signature: str):
    auto_reply_sent.append((chat_id, _norm(signature)))
    if len(auto_reply_sent) > 500:
        del auto_reply_sent[:250]


_seen_feedback_ids: set[int] = set()   # системные сообщения об отзывах, уже показанные


async def _notify_missed_feedback(chat_id: int) -> int:
    """Ловит отзывы, «проглоченные» автоответом: мы отправляем сообщение,
    FunPay помечает чат прочитанным, и системное событие отзыва не доходит
    ни через Runner, ни через обходчик. Смотрим историю чата перед ответом.
    Возвращает число показанных отзывов."""
    try:
        h = await asyncio.to_thread(acc.get_chats_histories, {chat_id: chat_names.get(chat_id)})
    except Exception:
        return 0
    msgs = h.get(chat_id) or []
    shown = 0
    for m in reversed(msgs):
        if m.id in _seen_feedback_ids:
            break
        mtype = effective_type(m.text, getattr(m, "type", enums.MessageTypes.NON_SYSTEM))
        if mtype is not enums.MessageTypes.NEW_FEEDBACK:
            continue
        _seen_feedback_ids.add(m.id)
        shown += 1
        stars = review_stars(m.text)
        body = ""
        oid_m = re.search(r"#([A-Za-z0-9]{5,})", m.text or "")
        if oid_m:
            block = await asyncio.to_thread(fetch_review_block, oid_m.group(1))
            body = esc(block) if block else ""
        header = "⭐ <b>Новый отзыв</b>\n📄 Чат: <i>" + esc(chat_names.get(chat_id, str(chat_id))) + "</i>"
        buttons = [[
            Button.inline("✉️ Ответить", f"reply:{chat_id}".encode()),
            Button.url("🔗 Открыть на FunPay", chat_url(chat_id)),
        ], [
            Button.inline("🚫 Забанить", f"ban:{chat_id}".encode()),
        ]]
        auto_status = ""
        if AUTO_REPLY_REVIEW and stars is not None and stars >= 4:
            res = await auto_reply(chat_id, AUTO_REPLY_REVIEW, m.text or "", "отзыв")
            auto_status += auto_reply_status_line(res, AUTO_REPLY_REVIEW)
        await notify(fit_text(f"{header}\n{body}{auto_status}", 4000), buttons)
        if shown:
            break  # обычно отзыв один; глубже не копаем
    return shown


_auto_reply_last: dict[tuple[int, str], float] = {}   # (chat_id, kind) -> ts


async def _funpay_reply_already_in_chat(chat_id: int, text: str) -> bool:
    """True, если последнее сообщение чата — наше, с тем же текстом.
    Кросс-инстанс защита: если бот запущен в двух местах (ПК + VPS),
    покупатель не должен получить автоответ дважды."""
    try:
        h = await asyncio.to_thread(acc.get_chats_histories, {chat_id: chat_names.get(chat_id)})
    except Exception:
        return False
    msgs = h.get(chat_id) or []
    if not msgs:
        return False
    m = msgs[-1]
    if getattr(m, "author_id", None) != getattr(acc, "id", None):
        return False
    bot_char = getattr(acc, "_Account__bot_character", "⁤")
    sent = _norm((m.text or "").replace(bot_char, "", 1))
    return bool(sent) and sent == _norm(text)


async def auto_reply(chat_id: int, text: str, signature: str, kind: str) -> str:
    """Отправляет автоответ в чат FunPay (по подтверждению/отзыву).
    Возвращает: 'sent' | 'skipped' | 'error: ...'."""
    if not text:
        return "skipped"
    if _auto_already_sent(chat_id, signature):
        return "skipped"
    # тайм-гейт против гонки Runner/обходчик: одно событие — один автоответ
    now = time.monotonic()
    if now - _auto_reply_last.get((chat_id, kind), -999) < 90:
        log.info("Автоответ (%s) в чат %s подавлен: уже отправляли за последние 90 сек", kind, chat_id)
        return "skipped"
    # кросс-инстанс дедуп: в чате уже стоит наш такой же ответ (вторая копия бота)
    if await _funpay_reply_already_in_chat(chat_id, text):
        _auto_reply_last[(chat_id, kind)] = now
        log.info("Автоответ (%s) в чат %s подавлен: идентичное сообщение уже в чате", kind, chat_id)
        return "skipped"
    # перед своим ответом ловим отзывы, которые могли быть «проглочены»
    try:
        await _notify_missed_feedback(chat_id)
    except Exception:
        log.exception("Не удалось проверить пропущенные отзывы в чате %s", chat_id)
    try:
        await asyncio.to_thread(send_funpay_message, chat_id, text)
        _mark_auto_sent(chat_id, signature)
        _auto_reply_last[(chat_id, kind)] = now
        log.info("Автоответ (%s) отправлен в чат %s: %s", kind, chat_id, text)
        return "sent"
    except Exception as e:
        log.exception("Не удалось отправить автоответ (%s) в чат %s", kind, chat_id)
        return f"error: {e}"


def auto_reply_status_line(result: str, text: str) -> str:
    """Строка-отметка об автоответе для уведомления в Telegram."""
    if result == "sent":
        return f"\n\n✅ Автоответ отправлен: <i>{esc(text)}</i>"
    if result.startswith("error"):
        return "\n\n⚠️ Автоответ не отправлен (подробности в логе)"
    return ""


def review_stars(system_text: str | None) -> int | None:
    """Оценка из системного сообщения об отзыве: 'Оценка: ★★★☆☆ (3/5)' -> 3."""
    m = re.search(r"\((\d)\s*/\s*5\)", system_text or "")
    return int(m.group(1)) if m else None


def fetch_review_block(order_id: str) -> str:
    """Тянет отзыв со страницы заказа. Возвращает готовый текстовый блок."""
    try:
        order = acc.get_order(order_id)
        review = order.review
        if review is None:
            return "\n(отзыв ещё не видно на странице заказа)"
        stars = review.stars or 0
        stars_line = "★" * stars + "☆" * max(0, 5 - stars)
        lines = [f"{'⭐' if stars >= 4 else '⚠️'} Оценка: {stars_line} ({stars}/5)"]
        if review.text:
            lines.append(f"📝 Отзыв: {short(review.text, 1000)}")
        if review.reply:
            lines.append(f"↩️ Ответ на отзыв: {short(review.reply, 500)}")
        return "\n" + "\n".join(lines)
    except Exception as e:
        log.warning("Не удалось получить отзыв к заказу %s: %s", order_id, e)
        return ""


# ---------------------------------------------------------------------------- #
#                Страховочный обходчик непрочитанных чатов                     #
# ---------------------------------------------------------------------------- #
# Runner иногда не может достать историю чата (FunPay отдаёт ошибку) и
# молчит. Обходчик каждые SWEEP_INTERVAL сек. смотрит список чатов напрямую:
# у непрочитанных чатов с новым превью шлёт уведомление. Также он ловит
# сообщения, пришедшие ДО запуска бота.

def _norm(text: str) -> str:
    return " ".join(str(text or "").split())


def _same_msg(a: str, b: str) -> bool:
    """Совпадают ли тексты (превью из списка чатов усечено, сравниваем префиксы)."""
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return False
    return a == b or a.startswith(b) or b.startswith(a)


def funpay_sweep() -> list:
    """Проверяет список чатов и возвращает непрочитанные с новым текстом.
    Текст без флага непрочитанности не триггерит: это может быть твоё
    собственное сообщение, отправленное из браузера.
    """
    bot_char = getattr(acc, "_Account__bot_character", "⁤")
    hits = []
    for c in acc.request_chats():
        text = _norm(c.last_message_text)
        if text.startswith(bot_char):
            text = text.replace(bot_char, "", 1)
        old = sweep_seen.get(c.id)
        sweep_seen[c.id] = text
        if not c.unread:
            continue
        if old is not None and _same_msg(old, text):
            continue  # это сообщение мы уже видели/присылали
        hits.append(c)
    return hits


async def notify_sweep_hit(c, prefix: str = "💬 <b>Новое сообщение на FunPay</b>"):
    """Уведомление по чату из списка (обходчик/стартовый обход)."""
    # Автоответы ботов-помощников покупателей — не уведомляем
    if is_ignored_bot_user(c.name):
        sweep_seen[c.id] = _norm(c.last_message_text)
        return
    # Забаненные пользователи — сообщения не уведомляем
    if is_banned(c.id):
        sweep_seen[c.id] = _norm(c.last_message_text)
        return
    remember_chat(c.id, c.name)
    mtype = effective_type(c.last_message_text, c.last_message_type)

    # После рестарта память пуста и своё сообщение можно принять за чужое:
    # проверяем автора последнего сообщения чата — своё или бота-помощника
    # (SellNode и т.п.) не уведомляем
    if mtype is enums.MessageTypes.NON_SYSTEM:
        try:
            hist = await asyncio.to_thread(acc.get_chats_histories, {c.id: c.name})
            msgs = hist.get(c.id) or []
            if msgs:
                last_author = msgs[-1].author_id
                if last_author == acc.id or last_author in NOTIFY_IGNORE_IDS:
                    sweep_seen[c.id] = _norm(c.last_message_text)
                    return
        except Exception as e:
            log.warning("Обходчик: не удалось проверить автора в чате %s: %s", c.id, e)

    label = NOTIFY_SYSTEM_TYPES.get(mtype) if mtype else None
    header = label or prefix
    body = f"👤 От: <b>{esc(c.name)}</b>\n\n{esc(short(c.last_message_text))}"
    buttons = [[
        Button.inline("✉️ Ответить", f"reply:{c.id}".encode()),
        Button.url("🔗 Открыть на FunPay", chat_url(c.id)),
    ], [
        Button.inline("🚫 Забанить", f"ban:{c.id}".encode()),
    ]]
    # Для событий с отзывом — тянем сам отзыв (оценку и текст)
    review_block = ""
    if mtype in REVIEW_EVENT_TYPES:
        oid_m = re.search(r"#([A-Za-z0-9]{5,})", c.last_message_text or "")
        if oid_m:
            review_block = await asyncio.to_thread(fetch_review_block, oid_m.group(1))
            if review_block:
                body += esc(review_block)
    # Автоответы (страховка, если Runner пропустил событие) — до отправки
    # уведомления, чтобы показать статус в самом сообщении
    auto_status = ""
    if mtype in CONFIRM_EVENT_TYPES and AUTO_REPLY_CONFIRM:
        res = await auto_reply(c.id, AUTO_REPLY_CONFIRM, c.last_message_text or "", "подтверждение")
        auto_status += auto_reply_status_line(res, AUTO_REPLY_CONFIRM)
    if mtype is enums.MessageTypes.NEW_FEEDBACK and AUTO_REPLY_REVIEW:
        stars = review_stars(c.last_message_text) or review_stars(review_block)
        if stars is not None and stars >= 4:  # благодарим только за 4★ и 5★
            res = await auto_reply(c.id, AUTO_REPLY_REVIEW, c.last_message_text or "", "отзыв")
            auto_status += auto_reply_status_line(res, AUTO_REPLY_REVIEW)

    await notify(fit_text(f"{header}\n{body}{auto_status}", 4000), buttons)


async def sweep_loop():
    """Периодическая проверка непрочитанных чатов (страховка Runner'а)."""
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        try:
            hits = await asyncio.to_thread(funpay_sweep)
        except fp_exceptions.RequestFailedError as e:
            # FunPay временно лежит (502/503) — это не наша ошибка, просто ждём
            first_line = str(e).splitlines()[0] if str(e) else e.__class__.__name__
            log.warning("Обходчик: FunPay временно недоступен, пропускаю цикл (%s)", first_line)
            continue
        except Exception:
            log.exception("Обходчик чатов ошибся (продолжаю)")
            continue
        for c in hits:
            log.info("Обходчик: непрочитанное в чате %s (%s)", c.id, c.name)
            try:
                await notify_sweep_hit(c)
            except Exception:
                log.exception("Не удалось отправить уведомление обходчика")


# ---------------------------------------------------------------------------- #
#                          Telegram (MTProto / Telethon)                        #
# ---------------------------------------------------------------------------- #

HELP_TEXT = (
    "🟢 <b>Бот на связи!</b>\n\n"
    "Я присылаю уведомления из FunPay:\n"
    "💬 новые сообщения (кнопка «Ответить» — есть заготовленные ответы)\n"
    "✅ подтверждения заказов\n"
    "⭐ отзывы\n"
    "🛒 новые заказы\n\n"
    "Статистика:\n"
    "/stats — баланс, заказы за день, выручка\n"
    "/top — какие сервера покупают чаще\n\n"
    "Управление товарами:\n"
    "📦 «Мои товары» — кнопка ниже или /lots.\n\n"
    "Команды:\n"
    "/presets — показать заготовленные ответы\n"
    "/status — статус бота\n"
    "/cancel — отменить ввод ответа/цены"
)


def strip_html(text: str) -> str:
    """Грубое удаление HTML-тегов для аварийной отправки без разметки."""
    return re.sub(r"<[^>]+>", "", html.unescape(text or ""))


# Дедуп системных уведомлений: Runner и страховочный обходчик могут
# обработать одно событие с разницей в секунды — шлём только первое.
_recent_notify: dict[str, float] = {}


def _is_dup_system_notify(text: str) -> bool:
    headers = tuple(NOTIFY_SYSTEM_TYPES.values())
    if not text.startswith(headers):
        return False  # обычные сообщения не дедупим: повторы легитимны
    key = _norm(text)[:400]
    now = time.monotonic()
    if now - _recent_notify.get(key, -9999) < 600:
        return True
    if len(_recent_notify) > 400:
        cutoff = now - 700
        for k in [k for k, ts in _recent_notify.items() if ts < cutoff]:
            del _recent_notify[k]
    _recent_notify[key] = now
    return False


async def _tg_already_sent(text: str, window: int = 120) -> bool:
    """True, если точно такое сообщение уже ушло в Telegram за последние window сек.
    Кросс-инстанс дедуп: вторая копия бота (ПК/VPS) уже уведомила владельца."""
    try:
        flat = _norm(strip_html(text))[:400]
        msgs = await tg.get_messages(TG_USER_ID, limit=5)
    except Exception as e:
        log.debug("TG-дедуп недоступен: %s", e)
        return False
    now = datetime.datetime.now(datetime.timezone.utc)
    for m in msgs:
        if not m.out:
            continue
        if _norm(m.message or "")[:400] != flat:
            continue
        try:
            age = (now - m.date).total_seconds()
        except Exception:
            continue
        if 0 <= age < window:
            return True
    return False


async def notify(text: str, buttons: list | None = None):
    """Шлёт уведомление владельцу; до 3 попыток с паузами.
    При проблемах с разметкой/длиной — повторяет без HTML."""
    if _is_dup_system_notify(text):
        log.info("Дубликат системного уведомления подавлен: %.60s", _norm(text))
        return
    if await _tg_already_sent(text):
        log.info("Уведомление уже в TG (отправил другой экземпляр?) — подавляю: %.60s", _norm(text))
        return
    for attempt in range(3):
        try:
            await tg.send_message(TG_USER_ID, text, buttons=buttons, link_preview=False)
            return
        except Exception as e:
            msg = str(e)
            if "Could not find the input entity" in msg:
                log.error(
                    "Не могу написать тебе в Telegram: ты ещё не писал этому боту. "
                    "Открой @%s в Telegram и отправь /start — после этого "
                    "уведомления пойдут.", bot_username or "своему боту"
                )
                return
            if "can't parse entities" in msg or "too long" in msg:
                # аварийная отправка без разметки
                try:
                    await tg.send_message(TG_USER_ID, strip_html(text)[:4000],
                                          buttons=buttons, link_preview=False)
                    return
                except Exception:
                    pass
            log.error("Уведомление не ушло (попытка %d/3): %s", attempt + 1, e)
            await asyncio.sleep(10)


MAX_IMAGE_BYTES = 9 * 1024 * 1024  # лимит фото Telegram ~10 МБ


def download_image(url: str) -> bytes | None:
    """Скачивает картинку из FunPay (CDN sfunpay.com)."""
    if not url:
        return None
    try:
        if url.startswith("/"):
            url = "https://funpay.com" + url
        r = _requests.get(url, headers={"user-agent": DEFAULT_UA}, timeout=25)
        if r.status_code == 200 and r.content and len(r.content) <= MAX_IMAGE_BYTES:
            ctype = (r.headers.get("content-type") or "").lower()
            if ctype and not ctype.startswith("image/"):
                log.warning("Не картинка (%s): %s", ctype, url)
                return None
            return r.content
        if r.status_code == 200:
            log.warning("Картинка слишком большая (%d байт) — пришлю текстом", len(r.content))
    except Exception as e:
        log.warning("Не удалось скачать картинку %s: %s", url, e)
    return None


async def notify_photo(text: str, image_url: str, buttons: list | None = None):
    """Шлёт уведомление с фотографией; при любом сбое — обычным текстом."""
    data = await asyncio.to_thread(download_image, image_url)
    if data:
        try:
            import io
            bio = io.BytesIO(data)
            bio.name = "funpay.jpg"
            await tg.send_file(TG_USER_ID, bio, caption=text, buttons=buttons)
            return
        except Exception as e:
            log.error("Фото не ушло в Telegram: %s", e)
    await notify(text, buttons)


# ---------------------------------------------------------------------------- #
#                        Управление лотами (цены)                              #
# ---------------------------------------------------------------------------- #

def fmt_price(p) -> str:
    try:
        val = float(p)
    except (TypeError, ValueError):
        return "?"
    return str(int(val)) if val.is_integer() else f"{val:.2f}".rstrip("0").rstrip(".")


def short_title(title, limit: int = 28) -> str:
    text = " ".join(str(title or "").replace("\n", " ").split())
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _delete_empty_offer(node: int, offer_id: int) -> bool:
    """Удаляет пустой (amount=0) оффер: страница trade -> форма offerEdit
    -> submit с deleted=1 (FunPay не даёт удалять пустые через /offer/delete).
    True — оффер удалён."""
    try:
        hdrs = {"accept": "application/json, text/javascript, */*; q=0.01",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "x-requested-with": "XMLHttpRequest",
                "referer": f"https://funpay.com/lots/offerEdit?node={node}&offer={offer_id}"}
        rr = acc.method("get", f"lots/offerEdit?node={node}&offer={offer_id}", {"accept": "*/*"}, {})
        html = rr.content.decode("utf-8", "replace")
        try:
            html = rr.json().get("html") or html
        except Exception:
            pass
        bs = BeautifulSoup(html, "html.parser")
        form = bs.find("form", class_="form-offer-editor")
        if form is None:
            return False  # оффера уже нет
        payload = {}
        for field in form.find_all("input"):
            name = field.get("name")
            if not name or name == "deleted":
                continue
            if field.get("type") == "checkbox":
                if field.has_attr("checked"):
                    payload[name] = "on"
            else:
                payload[name] = field.get("value") or ""
        for field in form.find_all("textarea"):
            payload[field["name"]] = field.text or ""
        for field in form.find_all("select"):
            sel = field.find("option", selected=True)
            payload[field["name"]] = sel["value"] if sel else ""
        payload["deleted"] = "1"
        r2 = acc.method("post", "lots/offerSave", hdrs, payload)
        j = r2.json()
        return bool(j.get("done"))
    except Exception as e:
        log.warning("Не удалось удалить пустой оффер %s: %s", offer_id, e)
        return False


def purge_empty_lots() -> list[int]:
    """Чистит нулевые лоты: страница «мои предложения» -> удаление amount=0.
    Возвращает список удалённых offer_id."""
    try:
        # узел берём со своего профиля (кнопка-карандаш «управление предложениями»)
        r = acc.method("get", f"users/{acc.id}/", {"accept": "text/html"}, {})
        h = r.content.decode("utf-8", "replace")
        node_m = re.search(r'href="https://funpay\.com/lots/(\d+)/trade"', h)
        if not node_m:
            return []
        node = int(node_m.group(1))
        r = acc.method("get", f"lots/{node}/trade", {"accept": "text/html"}, {})
    except Exception:
        return []
    h = r.content.decode("utf-8", "replace")
    rows = re.findall(rf'offerEdit\?node={node}&amp;offer=(\d+)"[\s\S]{{0,1500}}?'
                      r'<div class="tc-amount hidden-xxs">(\d+)</div>', h)
    empty = [int(oid) for oid, amt in rows if int(amt) == 0]
    if not empty:
        return []
    removed = []
    for oid in empty:
        if _delete_empty_offer(node, oid):
            removed.append(oid)
            log.info("Пустой лот %s удалён с FunPay (количество 0)", oid)
    return removed


def fetch_my_lots(progress=None) -> list[dict]:
    """Список моих лотов (только выставленные) + актуальная цена редактирования.
    progress(i, total) вызывается после каждого обработанного лота."""
    purge_empty_lots()
    user = acc.get_user(acc.id)
    items = []
    lots = user.get_lots()[:LOTS_LIMIT]
    total = len(lots)
    for idx, lot in enumerate(lots, 1):
        lid = int(lot.id)
        title = lot.description or ""
        price = lot.price
        try:
            fields = acc.get_lot_fields(lid)
            title = fields.title_ru or title
            if fields.price is not None:
                price = fields.price
            # количество 0 после продажи — удаляем оффер совсем
            if fields.amount is not None and int(fields.amount) == 0:
                try:
                    hdrs = {"accept": "*/*", "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                            "x-requested-with": "XMLHttpRequest",
                            "referer": f"https://funpay.com/lots/offer?id={lid}"}
                    acc.method("post", "offer/delete", hdrs,
                               {"csrf_token": acc.csrf_token, "action": "delete",
                                "type": "lot", "offer_id": lid})
                    log.info("Лот %s удалён с FunPay (количество 0)", lid)
                except Exception:
                    log.exception("Не удалось удалить пустой лот %s — деактивирую", lid)
                    if fields.active:
                        fields.active = False
                        fields.renew_fields()
                        acc.save_lot(fields)
                continue  # пустой лот в список не показываем
        except Exception as e:
            log.warning("Не удалось прочитать лот %s: %s", lid, e)
        items.append({"id": lid, "title": title, "price": price})
        if progress is not None:
            try:
                progress(idx, total)
            except Exception:
                pass
    return items


def lot_server(title: str) -> str:
    """Короткое имя лота: первый сегмент до '|' без эмодзи."""
    first = re.split(r"[|·]", str(title or ""))[0]
    first = re.sub(r"[^\w\s\-\.]", "", first).strip()
    return first[:24] or "товар"


def lots_text() -> str:
    lines = [f"📦 <b>Мои товары</b> ({len(lots_cache)})", ""]
    for i, item in enumerate(lots_cache, 1):
        mark = MARK_ON if item["id"] in lots_selected else MARK_OFF
        lines.append(
            f"{mark} <b>{i}.</b> {esc(short_title(item['title'], 90))} — "
            f"<b>{fmt_price(item['price'])}₽</b>"
        )
    if not lots_cache:
        lines.append("Лотов не найдено.")
    lines.append("")
    lines.append("Нажми на товар ниже — ➕ станет ✔️. Можно отметить несколько "
                 "(не обязательно все), потом «💰 Цена выбранным».")
    return "\n".join(lines)


def lots_buttons() -> list:
    rows = []
    visible = {item["id"] for item in lots_cache}
    for i, item in enumerate(lots_cache, 1):
        mark = MARK_ON if item["id"] in lots_selected else MARK_OFF
        rows.append([
            Button.inline(
                f"{mark} {i}. {lot_server(item['title'])} · {fmt_price(item['price'])}₽",
                f"lot:tg:{item['id']}".encode(),
            ),
        ])
    selected = len(lots_selected & visible)
    all_selected = bool(visible) and selected == len(visible)
    rows.append([
        Button.inline(("➖ Снять все" if all_selected else "➕ Отметить все"), b"lot:all"),
        Button.inline(f"💰 Цена выбранным ({selected})", b"lot:edit"),
    ])
    rows.append([
        Button.inline("🔄 Обновить", b"lot:refresh"),
        Button.inline("❌ Отменить выбор", b"lot:clear"),
    ])
    return rows


def price_prompt(ids: list[int]) -> str:
    what = "лота" if len(ids) == 1 else f"{len(ids)} лотов"
    return (
        f"💰 Введи новую цену для {what} — число, как в редактировании лота на FunPay "
        "(например 150 или 149.90).\n/cancel — отмена."
    )


def apply_prices(lot_ids: list[int], new_price: float,
                 progress=None) -> tuple[list, list, list]:
    """Меняет цену у списка лотов и ВЕРИФИЦИРУЕТ результат перечитыванием формы.
    Возвращает (успешные, [(id, ошибка)], [(id, ожидалось, стало)]).
    progress(done, total) — для отображения в Telegram."""
    ok, errors, mismatched = [], [], []
    total = len(lot_ids)
    for i, lid in enumerate(lot_ids, 1):
        try:
            fields = acc.get_lot_fields(lid)
            old = fields.price
            fields.price = new_price
            fields.renew_fields()
            acc.save_lot(fields)
            # FunPay иногда не применяет сохранение (гонки/лимиты) — сверяем
            check = acc.get_lot_fields(lid)
            if check.price is None or abs(float(check.price) - new_price) > 0.005:
                mismatched.append((lid, new_price, check.price))
                log.warning("Лот %s: цена НЕ применилась: ждали %s, на FunPay %s",
                            lid, new_price, check.price)
            else:
                ok.append(lid)
                log.info("Лот %s: цена %s -> %s (проверено)", lid, old, new_price)
        except Exception as e:
            log.warning("Лот %s: не удалось изменить цену: %s", lid, e)
            errors.append((lid, str(e)))
        if progress is not None:
            try:
                progress(i, total)
            except Exception:
                pass
    return ok, errors, mismatched


# Дебаунс перерисовки списка лотов: Telegram режет частые edit'ы сообщения,
# поэтому тапы меняют выбор мгновенно, а перерисовка идёт не чаще раза в 0.7с
_lots_edit_task: asyncio.Task | None = None


async def _safe_progress(note, done: int, total: int, count: int, price: float):
    """Обновляет сообщение прогресса смены цены (не падая на not modified)."""
    try:
        await note.edit(f"⏳ Меняю цену на {fmt_price(price)}₽: {done}/{count}...")
    except MessageNotModifiedError:
        pass
    except Exception:
        pass


def schedule_lots_edit(client, chat_id: int, msg_id: int):
    global _lots_edit_task
    if _lots_edit_task is not None and not _lots_edit_task.done():
        return  # перерисовка уже запланирована — она возьмёт свежие lots_selected
    async def _run():
        await asyncio.sleep(0.4)
        try:
            await client.edit_message(chat_id, msg_id, lots_text(), buttons=lots_buttons())
        except MessageNotModifiedError:
            pass
        except Exception as e:
            log.warning("Не удалось перерисовать список лотов: %s", e)
    _lots_edit_task = asyncio.create_task(_run())


async def safe_edit(event, text: str, buttons: list | None = None):
    """Редактирует сообщение, не падая на 'not modified'."""
    try:
        await event.edit(text, buttons=buttons)
    except MessageNotModifiedError:
        pass
    except Exception as e:
        log.warning("Не удалось обновить сообщение лотов: %s", e)


def ui_row(offline_count: int = 0) -> list:
    """Компактный ряд кнопок: меню товаров + FunPay (+ пропущенные сообщения)."""
    rows = [[
        Button.inline("📦 Мои товары", b"lot:menu"),
        Button.url("🔗 Открыть FunPay", "https://funpay.com/"),
    ], [
        Button.inline("📨 Проверить непрочитанные", b"offline:msgs"),
    ]]
    if offline_count:
        rows.append([Button.inline(
            f"📨 Сообщения за время простоя ({offline_count})", b"offline:msgs")])
    return rows


async def show_lots(event=None, edit: bool = False, via_tg: bool = False):
    """Показывает/обновляет список лотов (сообщение, callback или TG напрямую)."""
    global lots_cache
    note = None
    progress_enabled = True
    if edit and event is not None:
        await event.edit("⏳ Загружаю лоты с FunPay...")
        note = event
    elif lots_cache:
        # мгновенно показываем уже загруженный список, потом тихо обновляем
        try:
            note = await tg.send_message(TG_USER_ID, lots_text(), buttons=lots_buttons())
        except Exception:
            note = None
        progress_enabled = False
    else:
        note = await tg.send_message(TG_USER_ID, "⏳ Загружаю лоты с FunPay...")

    progress = {"i": 0, "total": 0}
    task = asyncio.create_task(asyncio.to_thread(
        fetch_my_lots, lambda i, total: progress.update(i=i, total=total)))
    while not task.done():
        await asyncio.sleep(0.7)
        if note is not None and progress_enabled and progress["total"]:
            try:
                await note.edit(f"⏳ Загружаю лоты… {progress['i']}/{progress['total']}")
            except MessageNotModifiedError:
                pass
            except Exception:
                pass
    try:
        lots_cache = await task
    except Exception as e:
        text = f"⚠️ Не удалось получить лоты: {esc(e)}"
        if note is not None:
            try:
                await note.edit(text)
            except Exception:
                pass
        else:
            await tg.send_message(TG_USER_ID, text)
        return

    lots_selected.intersection_update({item["id"] for item in lots_cache})
    text, buttons = lots_text(), lots_buttons()
    if note is not None:
        try:
            await note.edit(text, buttons=buttons)
        except MessageNotModifiedError:
            pass
    else:
        await tg.send_message(TG_USER_ID, text, buttons=buttons)


async def on_tg_message(event):
    global pending_price

    text = (event.raw_text or "").strip()

    if not is_allowed(event.sender_id):
        # не владелец: пусть введёт пароль доступа (если задан в .env)
        if BOT_PASSWORD and text and text == BOT_PASSWORD:
            authorized_users.add(event.sender_id)
            await event.reply("🔓 Доступ открыт. /start — меню.")
            return
        await event.reply("⛔ Бот приватный. Доступ только по паролю.")
        await notify_intruder(event.sender_id, text)
        return

    if not text:
        return

    if text == "/start":
        pending_reply.clear()
        pending_price = None
        await event.reply(HELP_TEXT, buttons=ui_row(len(offline_hits)))
        return

    if text == "/lots" or ("мои товары" in text.lower() and not pending_reply and pending_price is None):
        await show_lots(event)
        return

    if text == "/status":
        ok = acc is not None and acc.id is not None
        runner_ok = funpay_runner is not None
        local = _tgws_local_proxy()
        if local:
            proxy_desc = f"локальный TgWsProxy ({local[0]}:{local[1]})"
        elif TG_MTPROXY:
            proxy_desc = f"{len(TG_MTPROXY.split(';'))} шт."
        else:
            proxy_desc = "напрямую"
        await event.reply(
            "📊 <b>Статус</b>\n"
            f"FunPay аккаунт: {'🟢 ' + esc(acc.username) if ok else '🔴 не подключён'}\n"
            f"Опрос FunPay: {'🟢 работает' if runner_ok else '🔴 остановлен'}\n"
            f"Интервал опроса: {REQUESTS_DELAY} сек\n"
            f"MTProto-прокси: {proxy_desc}",
            buttons=ui_row(),
        )
        return

    if text == "/stats":
        note = await event.reply("⏳ Собираю статистику...")
        try:
            stats = await asyncio.to_thread(collect_stats)
            await note.edit(stats_text(stats), parse_mode="html")
        except Exception as e:
            log.exception("Ошибка /stats")
            await note.edit(f"⚠️ Не удалось собрать статистику: {esc(e)}")
        return

    if text in ("/presets", "/otvety"):
        rows = "\n\n".join(
            f"<b>{i + 1}. {esc(label)}</b>\n{esc(text_)}"
            for i, (label, text_) in enumerate(PRESET_REPLIES))
        await event.reply(
            "📋 <b>Заготовленные ответы</b>\n"
            "<i>Появляются плашками под кнопкой «Ответить». "
            "Перед отправкой всегда показываю превью — случайной отправки не будет.</i>\n\n"
            + rows)
        return

    if text == "/cancel":
        pending_price = None
        if pending_reply.pop("chat_id", None) is not None:
            await event.reply("❌ Отменено.")
        else:
            await event.reply("Нечего отменять.")
        return

    if text == "/bans" or text.startswith("/ban"):
        arg = text[4:].strip() if text.startswith("/ban ") else ""
        if arg:  # /ban ник — забанить по имени
            if text.startswith("/bans"):
                arg = ""  # /bans — это список, не ник
        if arg:
            target = arg
            note = await event.reply(f"⏳ Ищу {esc(target)}...")
            try:
                chat = await asyncio.to_thread(acc.get_chat_by_name, target, True)
            except Exception as e:
                chat = None
                log.warning("/ban: не удалось найти %s: %s", target, e)
            if chat is None:
                await note.edit(f"⚠️ Диалог с {esc(target)} не найден. "
                                "Пользователь должен был писать тебе в FunPay.")
                return
            ban_user(chat.id, chat_names.get(chat.id, target))
            await note.edit(f"🚫 Забанен: <b>{esc(chat_names.get(chat.id, target))}</b> "
                            f"(чат {chat.id}). Сообщения от него больше не приходят.")
            return

        # без аргумента — список банов с кнопками разбана
        if not banned_users:
            await event.reply("🚫 Забаненных нет. Кнопка «🚫 Забанить» есть в каждом уведомлении, "
                              "или /ban ник")
            return
        lines = ["🚫 <b>Забаненные пользователи</b>:", ""]
        for cid, info in sorted(banned_users.items(), key=lambda kv: -kv[1]["ts"]):
            dt = datetime.datetime.fromtimestamp(info["ts"], SAMARA_TZ)
            lines.append(f"• <b>{esc(info['name'])}</b> (id {cid}) — с {dt.strftime('%d.%m %H:%M')}")
        kb = [[Button.inline(f"✅ Разбанить: {info['name'][:20]}", f"unban:{cid}".encode())]
              for cid, info in sorted(banned_users.items(), key=lambda kv: -kv[1]["ts"])]
        await event.reply("\n".join(lines), buttons=kb)
        return

    if text == "/logs":
        if not LOG_FILE:
            await event.reply("⚠️ Логи в файл не включены.")
            return
        try:
            with open(LOG_FILE, encoding="utf-8", errors="replace") as f:
                tail = f.readlines()[-40:]
            await event.reply("📜 <b>Последние логи</b>:\n<pre>" +
                              esc("".join(tail))[-3500:] + "</pre>")
        except Exception as e:
            await event.reply(f"⚠️ Логи не читаются: {esc(e)}")
        return

    if text == "/top":
        note = await event.reply("⏳ Считаю продажи по серверам...")
        try:
            rows = await asyncio.to_thread(collect_top)
            await note.edit(top_text(rows, 30), parse_mode="html")
        except Exception as e:
            log.exception("Ошибка /top")
            await note.edit(f"⚠️ Не удалось собрать статистику: {esc(e)}")
        return

    # Неизвестные команды не должны уходить в FunPay как текст ответа
    if text.startswith("/"):
        await event.reply("🤖 Неизвестная команда. /start — справка, /cancel — отмена.")
        return

    # Ввод новой цены для выбранных лотов
    if pending_price is not None:
        try:
            price = float(text.replace(",", ".").strip())
            if price <= 0:
                raise ValueError
        except ValueError:
            await event.reply("⚠️ Нужна цена числом, например 150 или 149.90.\n"
                              "Попробуй ещё раз (/cancel — отмена).")
            return
        ids = pending_price
        pending_price = None
        note = await event.reply(f"⏳ Меняю цену у {len(ids)} лот(ов) на {fmt_price(price)}₽...")
        loop = asyncio.get_running_loop()

        def _progress(done, total):
            if done % 3 == 0 or done == total:
                loop.call_soon_threadsafe(
                    lambda d=done: asyncio.ensure_future(
                        _safe_progress(note, d, total, len(ids), price)))

        def _work():
            return apply_prices(ids, price, progress=_progress)

        ok, errors, mismatched = await loop.run_in_executor(None, _work)
        summary = f"✅ Цена обновлена: {len(ok)} из {len(ids)} → {fmt_price(price)}₽"
        if mismatched:
            bad = ", ".join(f"#{lid}" for lid, _, _ in mismatched[:8])
            summary += f"\n⚠️ FunPay не применил цену у: {bad} — попробуй ещё раз для них"
        if errors:
            failed = ", ".join(f"#{lid}" for lid, _ in errors[:10])
            summary += f"\n⚠️ Не получилось: {failed}"
        await note.edit(summary)

        # сразу показываем новые цены без полной перезагрузки всех лотов
        ok_set = set(ok)
        for item in lots_cache:
            if item["id"] in ok_set:
                item["price"] = price
        lots_selected.clear()
        try:
            await event.reply(lots_text(), buttons=lots_buttons())
        except Exception:
            pass
        return

    if "chat_id" in pending_reply:
        chat_id = pending_reply.pop("chat_id")
        note = await event.reply("⏳ Отправляю...")
        try:
            await asyncio.to_thread(send_funpay_message, chat_id, text)
            await note.edit("✅ Ответ отправлен на FunPay!")
        except Exception as e:
            log.exception("Ошибка отправки ответа")
            await note.edit(f"⚠️ Не удалось отправить: {esc(e)}")
        return

    await event.reply("🤖 Напиши /start, чтобы посмотреть, что я умею.")


# Дедупликация callback-тапов: через медленный MTProxy Telegram доставляет
# один и тот же тап повторно, и toggle срабатывает дважды (выбор слетает).
# Ретраи несут тот же query_id, легитимные нажатия — всегда новый: дедуп по нему.
_recent_callbacks: dict[str, float] = {}


def _is_duplicate_cb(query_id, msg_id, data: bytes) -> bool:
    key = str(query_id) if query_id else f"{msg_id}:{data!r}"
    now = time.monotonic()
    if now - _recent_callbacks.get(key, -99) < 10.0:
        return True  # уже видели этот тап (ретрай)
    # подрезаем словарь, чтобы не рос
    if len(_recent_callbacks) > 500:
        cutoff = now - 15.0
        for k in [k for k, ts in _recent_callbacks.items() if ts < cutoff]:
            del _recent_callbacks[k]
    _recent_callbacks[key] = now
    return False


async def on_reply_button(event):
    """Кнопки ответа: reply: — меню (пресеты + своё), own: — ввод текста,
    qr: — превью пресета, qrs: — отправка после подтверждения, qrc — отмена."""
    try:
        if not is_allowed(event.sender_id):
            await event.answer("⛔ Нет доступа", alert=True)
            return
        data = event.data or b""
        if not (data.startswith(b"reply:") or data.startswith(b"own:")
                or data.startswith(b"qrs:") or data.startswith(b"qr:")
                or data == b"qrc"):
            return  # не наша кнопка (лота обрабатывает on_lot_button)
        if _is_duplicate_cb(event.id, event.message_id, data):
            await event.answer()
            return

        if data == b"qrc":
            await event.answer()
            try:
                await event.edit("❌ Отменено.")
            except Exception:
                pass
            return

        m = re.match(rb"(qrs|qr|own|reply):(\d+)(?::(\d+))?", data)
        if not m:
            await event.answer()
            return
        action = m.group(1).decode()
        chat_id = int(m.group(2))
        cname = esc(chat_names.get(chat_id, chat_id))

        if action == "reply":
            await event.answer()
            rows = [[Button.inline("✍️ Своё сообщение", f"own:{chat_id}".encode())]]
            row = []
            for i, (label, _) in enumerate(PRESET_REPLIES):
                row.append(Button.inline(label, f"qr:{chat_id}:{i}".encode()))
                if len(row) == 2:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            rows.append([Button.inline("❌ Отмена", b"qrc")])
            await event.respond(
                f"💬 Ответ в чат FunPay <i>{cname}</i> — выбери готовый или напиши свой:",
                buttons=rows)
            log.info("Ответ запрошен: меню пресетов для чата %s", chat_id)
            return

        if action == "own":
            await event.answer()
            pending_reply["chat_id"] = chat_id
            await event.respond(
                f"✍️ Напиши ответ — он уйдёт в чат FunPay <i>{cname}</i> "
                "(или /cancel для отмены):"
            )
            log.info("Ответ запрошен: режим ввода для чата %s включён", chat_id)
            return

        if action == "qr":
            await event.answer()
            i = int(m.group(3))
            if not (0 <= i < len(PRESET_REPLIES)):
                return
            _, text = PRESET_REPLIES[i]
            await event.respond(
                f"Отправить в чат <i>{cname}</i>?\n\n{text}",
                buttons=[[
                    Button.inline("✅ Отправить", f"qrs:{chat_id}:{i}".encode()),
                    Button.inline("❌ Отмена", b"qrc"),
                ]])
            return

        if action == "qrs":
            i = int(m.group(3))
            if not (0 <= i < len(PRESET_REPLIES)):
                await event.answer()
                return
            _, text = PRESET_REPLIES[i]
            await event.answer("Отправляю...")
            try:
                await asyncio.to_thread(send_funpay_message, chat_id, text)
                log.info("Готовый ответ отправлен в чат %s", chat_id)
                try:
                    await event.edit(f"✅ Отправлено в чат <i>{cname}</i>:\n\n{text}")
                except Exception:
                    await event.respond(f"✅ Отправлено в чат <i>{cname}</i>:\n\n{text}")
            except Exception:
                log.exception("Ошибка отправки готового ответа")
                await event.respond("⚠️ Не удалось отправить: подробности в логе")
            return
    except Exception:
        log.exception("Ошибка в on_reply_button (тап «Ответить»)")
        try:
            await event.answer("⚠️ Ошибка, смотри лог", alert=True)
        except Exception:
            pass


async def on_lot_button(event):
    """Кнопки управления лотами: выбор, цена, обновление."""
    global pending_price, lots_cache
    if not is_allowed(event.sender_id):
        await event.answer("⛔ Нет доступа", alert=True)
        return
    data = event.data or b""

    # повторная доставка того же тапа (медленный прокси) — не toggle'им дважды
    if _is_duplicate_cb(event.id, event.message_id, data):
        await event.answer()
        return

    # Если список ещё не загружен в этом процессе — подгружаем
    if data.startswith(b"lot:") and data not in (b"lot:refresh", b"lot:clear") and not lots_cache:
        await event.answer("Загружаю список...")
        try:
            lots_cache = await asyncio.to_thread(fetch_my_lots)
        except Exception as e:
            await event.answer(f"Ошибка: {e}", alert=True)
            return

    if data == b"lot:menu":
        await event.answer("Загружаю...")
        await show_lots(via_tg=True)
        return

    if data == b"lot:clear":
        lots_selected.clear()
        await event.answer("Выбор сброшен")
        await safe_edit(event, lots_text(), lots_buttons())
        return

    if data == b"lot:refresh":
        await event.answer("Обновляю...")
        await show_lots(event, edit=True)
        return

    if data == b"lot:all":
        visible = {item["id"] for item in lots_cache}
        if visible and lots_selected >= visible:
            lots_selected.difference_update(visible)
        else:
            lots_selected.update(visible)
        await event.answer()
        await safe_edit(event, lots_text(), lots_buttons())
        return

    if data == b"lot:edit":
        ids = sorted(lots_selected)
        if not ids:
            await event.answer("Сначала отметь товары", alert=True)
            return
        pending_price = ids
        await event.answer()
        await event.respond(price_prompt(ids))
        return

    m = re.match(rb"lot:tg:(\d+)", data)
    if m:
        lid = int(m.group(1))
        if not any(item["id"] == lid for item in lots_cache):
            await event.answer("Товар не найден — нажми «Обновить»", alert=True)
            return
        if lid in lots_selected:
            lots_selected.discard(lid)
        else:
            lots_selected.add(lid)
        await event.answer(f"{'✅ Выбрано' if lid in lots_selected else '➖ Снято'}: {len(lots_selected)}")
        schedule_lots_edit(event.client, event.chat_id, event.message_id)
        return

    m = re.match(rb"lot:one:(\d+)", data)
    if m:
        lid = int(m.group(1))
        pending_price = [lid]
        await event.answer()
        await event.respond(price_prompt([lid]))
        return

    await event.answer()


# Пароль для доступа к боту (для себя/брата/друга): укажи в .env BOT_PASSWORD=...
# Пустой (по умолчанию) — никто, кроме владельца TG_USER_ID, доступа не имеет.
# НИКОГДА не используй в качестве пароля golden_key — это ключ от FunPay-аккаунта!
BOT_PASSWORD = os.getenv("BOT_PASSWORD", "").strip()
authorized_users: set[int] = set()   # допущенные по паролю (помимо владельца)
_intruder_last_notify: dict[int, float] = {}   # антиспам уведомлений о чужих


def is_allowed(sender_id: int) -> bool:
    return sender_id == TG_USER_ID or sender_id in authorized_users


async def notify_intruder(sender_id: int, text: str):
    """Чужой стучится в бота: пишем в лог и (не чаще раза в 30 мин на человека) владельцу."""
    who = str(sender_id)
    log.warning("⛔ Чужой запрос в бота: id=%s, текст: %.80s", who, text or "")
    now = time.monotonic()
    if now - _intruder_last_notify.get(sender_id, -9999) < 1800:
        return
    _intruder_last_notify[sender_id] = now
    try:
        await tg.send_message(
            TG_USER_ID,
            f"🚨 <b>Чужой запрос в бота</b>\nTG ID: <code>{esc(who)}</code>\n"
            f"Текст: <i>{esc((text or '')[:120])}</i>\n"
            f"Доступ закрыт. {'Пароль задан — может войти по паролю.' if BOT_PASSWORD else 'Пароль не задан — полный игнор.'}",
            link_preview=False)
    except Exception:
        pass


def create_client(proxy: tuple | None) -> TelegramClient:
    """Создаёт TelegramClient; прокси — (host, port, secret) или None."""
    kwargs = {}
    if proxy:
        host, port, secret = proxy
        conn_cls = (ConnectionTcpMTProxyFakeTLS if secret.lower().startswith("ee")
                    else ConnectionTcpMTProxyRandomizedIntermediate)
        # TelethonFakeTLS ожидает секрет без префикса 'ee'
        if secret.lower().startswith("ee"):
            secret = secret[2:]
        kwargs["proxy"] = (host, port, secret)
        kwargs["connection"] = conn_cls

    client = TelegramClient(
        "bot.session", API_ID, API_HASH,
        connection_retries=None, retry_delay=5,
        auto_reconnect=True, timeout=15,
        **kwargs,
    )
    client.parse_mode = "html"
    client.add_event_handler(on_tg_message, events.NewMessage(incoming=True, func=lambda e: e.is_private))
    client.add_event_handler(on_reply_button, events.CallbackQuery)
    client.add_event_handler(on_lot_button, events.CallbackQuery)
    client.add_event_handler(on_ban_button, events.CallbackQuery)
    return client


async def on_ban_button(event):
    """Кнопки бана/разбана и пропущенных сообщений."""
    if not is_allowed(event.sender_id):
        await event.answer("⛔ Нет доступа", alert=True)
        return
    data = event.data or b""

    if data == b"offline:msgs":
        hits, offline_hits[:] = offline_hits[:], []
        if not hits:
            # свежий опрос: все непрочитанные сейчас
            try:
                hits = await asyncio.to_thread(funpay_sweep)
            except Exception as e:
                await event.answer(f"Ошибка: {e}", alert=True)
                return
        if not hits:
            # непрочитанных нет — показываем последние ЧУЖИЕ сообщения в чатах,
            # чтобы кнопка была полезной даже при прочитанной переписке
            try:
                chats = await asyncio.to_thread(acc.request_chats)
            except Exception as e:
                log.exception("/offline:msgs: не удалось получить чаты")
                return
            recent = [c for c in chats if not is_ignored_bot_user(c.name) and not is_banned(c.id)][:8]
            if not recent:
                return
            # одним запросом тянем истории, чтобы показать последнее чужое сообщение
            histories = {}
            try:
                histories = await asyncio.to_thread(
                    acc.get_chats_histories, {c.id: c.name for c in recent})
            except Exception:
                log.exception("/offline:msgs: не удалось получить истории")
            lines = ["📨 <b>Последние сообщения</b> (непрочитанных нет)", ""]
            kb = []
            for c in recent:
                remember_chat(c.id, c.name)
                # ищем последнее чужое (покупатель/системное) сообщение в истории
                foreign = None
                for m in reversed(histories.get(c.id) or []):
                    if m.author_id != acc.id:
                        foreign = m
                        break
                # истории нет (чат давно прочитан, новых не присылает) — пропускаем:
                # last_message_text может быть нашим автоответом, его показывать нельзя
                if foreign is None:
                    continue
                body = short(foreign.text or "(изображение)", 90)
                mtype = effective_type(foreign.text, getattr(foreign, "type", None))
                label = NOTIFY_SYSTEM_TYPES.get(mtype) if mtype else None
                prefix = f"{label.split()[0]} " if label else ""
                lines.append(f"👤 <b>{esc(c.name)}</b>\n{prefix}{esc(body)}")
                kb.append([Button.inline(f"✉️ {c.name[:24]}", f"reply:{c.id}".encode())])
            if len(lines) <= 2:
                await event.answer("Сообщений от покупателей не найдено", alert=True)
                return
            await notify("\n\n".join(lines), kb)
            return
        await event.answer(f"Показываю {len(hits)}...")
        for c in hits:
            try:
                await notify_sweep_hit(c, "💬 <b>Непрочитанное сообщение на FunPay</b>")
            except Exception:
                log.exception("Не удалось отправить отложенное уведомление")
        return

    m = re.match(rb"(ban|unban):(\d+)", data)
    if not m:
        return
    if _is_duplicate_cb(event.id, event.message_id, data):
        await event.answer()
        return
    action, chat_id = m.group(1).decode(), int(m.group(2))
    if action == "ban":
        ban_user(chat_id, chat_names.get(chat_id))
        await event.answer(f"🚫 Забанен: {chat_names.get(chat_id, chat_id)}", alert=True)
    else:
        if unban_user(chat_id):
            await event.answer(f"✅ Разбанен: {chat_names.get(chat_id, chat_id)}", alert=True)
        else:
            await event.answer("Уже не забанен")
    return


def _tgws_local_proxy() -> tuple[str, int, str] | None:
    """Автоматически читает локальный прокси TgWsProxy из его конфига.
    Это спасает, если TgWsProxy переустановили и секрет сменился."""
    cfg_path = os.path.join(os.getenv("APPDATA", ""), "TgWsProxy", "config.json")
    try:
        with open(cfg_path, encoding="utf-8-sig") as f:
            cfg = json.load(f)
        host = cfg.get("host") or "127.0.0.1"
        port = int(cfg.get("port") or 1443)
        secret = str(cfg.get("secret") or "").strip()
        if secret:
            return host, port, secret
    except Exception:
        pass
    return None


async def set_bot_commands(client: TelegramClient):
    """Регистрирует список команд — при вводе «/» Telegram показывает синее меню."""
    commands = [
        BotCommand(command="start", description="Меню и справка"),
        BotCommand(command="stats", description="Баланс и заказы за день"),
        BotCommand(command="top", description="Продажи по серверам"),
        BotCommand(command="lots", description="Мои товары и цены"),
        BotCommand(command="status", description="Статус бота"),
        BotCommand(command="logs", description="Последние логи бота"),
        BotCommand(command="presets", description="Показать заготовленные ответы"),
        BotCommand(command="cancel", description="Отменить ввод ответа/цены"),
    ]
    try:
        await client(SetBotCommandsRequest(
            scope=BotCommandScopeDefault(), lang_code="", commands=commands,
        ))
        log.info("Список команд для меню «/» обновлён")
    except Exception as e:
        log.warning("Не удалось установить список команд: %s", e)


async def connect_telegram() -> bool:
    """Подключается к Telegram: локальный TgWsProxy -> прокси из .env -> напрямую."""
    global tg

    proxies: list[tuple | None] = []

    # 1) локальный TgWsProxy (самый надёжный канал в РФ)
    local = _tgws_local_proxy()
    if local:
        proxies.append(local)

    # 2) прокси из .env
    if TG_MTPROXY:
        for part in TG_MTPROXY.split(";"):
            part = part.strip()
            if not part:
                continue
            bits = part.split(":")
            if len(bits) != 3:
                log.warning("Пропускаю некорректный TG_MTPROXY: %s (формат host:port:secret)", part)
                continue
            try:
                proxies.append((bits[0], int(bits[1]), bits[2]))
            except ValueError:
                log.warning("Пропускаю некорректный порт в TG_MTPROXY: %s", part)

    # 3) без прокси (на случай, если доступ к Telegram когда-нибудь откроют)
    proxies.append(None)

    # убираем дубликаты, сохраняя порядок
    seen, uniq = set(), []
    for p in proxies:
        key = (p[0], p[1], p[2]) if p else None
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    proxies = uniq

    for proxy in proxies:
        host, port, secret = (proxy if proxy else (None, None, None))
        label = f"{host}:{port}" if host else "напрямую (без прокси)"
        client = None
        try:
            client = create_client(proxy)
            await asyncio.wait_for(client.start(bot_token=BOT_TOKEN), timeout=40)
            # Прогрев кеша диалогов: если владелец уже писал боту, его entity
            # попадёт в кеш и уведомления по TG_USER_ID сразу уйдут.
            try:
                await client.get_dialogs(limit=50)
            except Exception:
                pass
            await set_bot_commands(client)
            tg = client
            me = await tg.get_me()
            bot_username = me.username or ""
            log.info("Telegram подключён через %s (бот @%s)", label, bot_username)
            return True
        except Exception as e:
            log.warning("Не удалось подключиться через %s: %s", label, e)
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    if local:
        log.error(
            "Локальный TgWsProxy (%s:%s) не отвечает. "
            "Запусти TgWsProxy_windows.exe и подожди пару секунд.", local[0], local[1]
        )
    return False


# ---------------------------------------------------------------------------- #
#                          Обработка событий FunPay                             #
# ---------------------------------------------------------------------------- #

def is_ignored_bot_user(name: str | None, author_id: int | None = None) -> bool:
    """Боты-помощники покупателей (SellNode и т.п.) — их автоответы не уведомляем.
    Проверяем и имя, и ID автора: имена боты часто меняют."""
    if not name:
        name = ""
    if author_id is not None and author_id in NOTIFY_IGNORE_IDS:
        return True
    return name.strip().lower() in NOTIFY_IGNORE_USERS


async def handle_event(event):
    etype = event.type

    if etype is enums.EventTypes.NEW_MESSAGE:
        m = event.message
        mtype = effective_type(m.text, getattr(m, "type", enums.MessageTypes.NON_SYSTEM))

        # Запоминаем текст, чтобы страховочный обходчик не прислал дубль
        sweep_seen[m.chat_id] = _norm(m.text)

        # Свои обычные сообщения не уведомляем
        if mtype is enums.MessageTypes.NON_SYSTEM and m.author_id == acc.id:
            return

        # Автоответы ботов-помощников покупателей — не уведомляем
        # (проверяем ID автора: ники ботов меняются, ID стабильны)
        if mtype is enums.MessageTypes.NON_SYSTEM and (
                is_ignored_bot_user(author_name(m), m.author_id)
                or is_ignored_bot_user(chat_name(m))):
            return

        # Забаненные пользователи — обычные сообщения не уведомляем
        if mtype is enums.MessageTypes.NON_SYSTEM and is_banned(m.chat_id):
            return

        if mtype is enums.MessageTypes.NON_SYSTEM:
            remember_chat(m.chat_id, chat_name(m))
            buttons = [[
                Button.inline("✉️ Ответить", f"reply:{m.chat_id}".encode()),
                Button.url("🔗 Открыть на FunPay", chat_url(m.chat_id)),
            ], [
                Button.inline("🚫 Забанить", f"ban:{m.chat_id}".encode()),
            ]]
            header = (
                "💬 <b>Новое сообщение на FunPay</b>\n"
                f"👤 От: <b>{esc(author_name(m))}</b>\n"
                f"📄 Чат: <i>{esc(chat_name(m))}</i>"
            )
            image_url = getattr(m, "image_link", None)
            body = "" if (not m.text or m.text in ("Изображение", "🖼 Изображение")) else esc(short(m.text))
            text = header + (f"\n\n{body}" if body else "")
            if image_url:
                await notify_photo(fit_text(text, 1000), image_url, buttons)
            else:
                await notify(fit_text(text, 4000), buttons)
            return

        # Системное сообщение (подтверждение/отзыв и т.п.)
        label = NOTIFY_SYSTEM_TYPES.get(mtype)
        if label:
            remember_chat(m.chat_id, chat_name(m))
            buttons = [[
                Button.inline("✉️ Ответить", f"reply:{m.chat_id}".encode()),
                Button.url("🔗 Открыть на FunPay", chat_url(m.chat_id)),
            ]]
            text = (
                f"{label}\n"
                f"📄 Чат: <i>{esc(chat_name(m))}</i>\n\n"
                f"{esc(short(m.text))}"
            )
            # Для событий с отзывом — тянем сам отзыв (оценку и текст)
            review_block = ""
            if mtype in REVIEW_EVENT_TYPES:
                oid_m = re.search(r"#([A-Za-z0-9]{5,})", m.text or "")
                if oid_m:
                    review_block = await asyncio.to_thread(fetch_review_block, oid_m.group(1))
                    if review_block:
                        text += esc(review_block)

            # Автоответы покупателю: только по факту подтверждения/отзыва
            if mtype in CONFIRM_EVENT_TYPES and AUTO_REPLY_CONFIRM:
                res = await auto_reply(m.chat_id, AUTO_REPLY_CONFIRM, m.text or "", "подтверждение")
                text += auto_reply_status_line(res, AUTO_REPLY_CONFIRM)
            if mtype is enums.MessageTypes.NEW_FEEDBACK and AUTO_REPLY_REVIEW:
                stars = review_stars(m.text) or review_stars(review_block)
                if stars is not None and stars >= 4:  # благодарим только за 4★ и 5★
                    res = await auto_reply(m.chat_id, AUTO_REPLY_REVIEW, m.text or "", "отзыв")
                    text += auto_reply_status_line(res, AUTO_REPLY_REVIEW)

            await notify(fit_text(text, 4000), buttons)
        return

    if etype is enums.EventTypes.NEW_ORDER:
        o = event.order
        text = (
            "🛒 <b>Новый заказ на FunPay!</b>\n"
            f"📦 {esc(getattr(o, 'description', '?'))}\n"
            f"👤 Покупатель: <b>{esc(getattr(o, 'buyer_username', '?'))}</b>\n"
            f"💰 Цена: {esc(getattr(o, 'price', '?'))} {esc(getattr(o, 'currency', ''))}"
        )
        # Большая кнопка на страницу заказа
        buttons = [[Button.url("🛒 Открыть заказ на FunPay", f"https://funpay.com/orders/{o.id}/")]]
        await notify(fit_text(text, 4000), buttons)
        return

    if etype is enums.EventTypes.ORDER_STATUS_CHANGED:
        o = event.order
        status = getattr(getattr(o, "status", None), "name", str(getattr(o, "status", "?")))
        text = (
            "🔄 <b>Статус заказа изменился</b>\n"
            f"📦 {esc(getattr(o, 'description', '?'))}\n"
            f"👤 Покупатель: <b>{esc(getattr(o, 'buyer_username', '?'))}</b>\n"
            f"📌 Новый статус: {esc(status)}"
        )
        await notify(text)
        return


async def events_consumer(queue: asyncio.Queue):
    while True:
        event = await queue.get()
        try:
            await handle_event(event)
        except Exception:
            log.exception("Ошибка при обработке события")
        finally:
            queue.task_done()


# ---------------------------------------------------------------------------- #
#                                   Запуск                                      #
# ---------------------------------------------------------------------------- #

def _pid_alive(pid: int) -> bool:
    """Безопасная проверка: жив ли процесс (Windows — через WinAPI)."""
    if pid <= 0:
        return False
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        k = ctypes.windll.kernel32
        h = k.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not h:
            return False
        k.CloseHandle(h)
        return True
    except Exception:
        return True  # не смогли проверить — считаем живым


def acquire_lock() -> bool:
    """True, если запуск единственный. Защита от двойного запуска."""
    global _lock_owned
    try:
        if os.path.exists(LOCK_FILE):
            try:
                pid = int(open(LOCK_FILE).read().strip())
            except ValueError:
                os.remove(LOCK_FILE)
                pid = None
            if pid is not None:
                if _pid_alive(pid):
                    return False  # бот реально работает
                os.remove(LOCK_FILE)  # протухший lock
        with open(LOCK_FILE, "w") as f:
            f.write(str(os.getpid()))
        _lock_owned = True
        return True
    except Exception:
        return True  # не смогли проверить — разрешаем запуск


def release_lock():
    """Удаляет lock только если этот процесс его создал."""
    global _lock_owned
    if not _lock_owned:
        return
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass
    _lock_owned = False


async def main():
    global acc, events_queue

    if not acquire_lock():
        raise SystemExit(
            "Похоже, бот уже запущен (другое окно). "
            "Закрой его и запусти один раз. Если это не так — удали файл bot.lock"
        )

    if not GOLDEN_KEY or not BOT_TOKEN or not TG_USER_ID:
        raise SystemExit(
            "Заполни .env (GOLDEN_KEY, BOT_TOKEN, TG_USER_ID). "
            "См. .env.example и README.md"
        )

    log.info("Подключаюсь к FunPay...")
    acc = Account(GOLDEN_KEY, user_agent=USER_AGENT,
                  requests_timeout=float(os.getenv("FP_TIMEOUT", "25")))
    patch_account_method(acc)
    patch_account_parsers(acc)
    acc.get()
    log.info("FunPay аккаунт: id=%s username=%s", acc.id, acc.username)

    await asyncio.to_thread(_load_bans)
    if banned_users:
        log.info("Забаненных пользователей: %d", len(banned_users))

    events_queue = asyncio.Queue()

    # Поток с опросом FunPay
    threading.Thread(
        target=funpay_worker,
        args=(asyncio.get_running_loop(), events_queue),
        daemon=True,
        name="funpay-runner",
    ).start()

    # Поток обновления сессии FunPay (csrf/PHPSESSID) раз в 45 минут
    threading.Thread(target=funpay_session_refresher, daemon=True, name="funpay-refresh").start()

    asyncio.create_task(events_consumer(events_queue))

    # Напоминания о неподтверждённых заказах (раз в REMINDER_INTERVAL_HOURS часов)
    asyncio.create_task(reminder_loop())

    # Быстрая проверка: заказ перевалил за SUPPORT_CONFIRM_HOURS — тикет сразу
    asyncio.create_task(support_loop())

    log.info("Подключаюсь к Telegram (MTProto)...")
    sweep_task = None
    while True:
        if await connect_telegram():
            # Стартовый обход: непрочитанные, пришедшие ПОКА БОТ БЫЛ ВЫКЛЮЧЕН,
            # не шлём пачкой — прячем за кнопку «Сообщения за время простоя»
            try:
                hits = await asyncio.to_thread(funpay_sweep)
            except Exception:
                hits = []
                log.exception("Стартовый обход чатов не удался")
            if sweep_task is None or sweep_task.done():
                sweep_task = asyncio.create_task(sweep_loop())

            offline_hits.extend(hits)
            btn_row = []
            if offline_hits:
                btn_row.append(Button.inline(
                    f"📨 Сообщения за время простоя ({len(offline_hits)})",
                    b"offline:msgs"))
            await notify(
                f"🟢 Бот запущен! Слежу за аккаунтом <b>{esc(acc.username)}</b> на FunPay.\n"
                "Меню товаров — /start",
                buttons=[btn_row] if btn_row else Button.clear(),
            )
            await tg.run_until_disconnected()
            log.warning("Telegram-соединение разорвано. Переподключение через 15 сек...")
        else:
            log.error(
                "Ни один прокси не подошёл. Запусти update_proxies.py, чтобы найти свежие. "
                "Повтор через 60 сек..."
            )
            await asyncio.sleep(60)
        await asyncio.sleep(15)


if __name__ == "__main__":
    log.info("=== Старт процесса бота, PID=%s ===", os.getpid())
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit) as e:
        release_lock()
        if isinstance(e, SystemExit) and str(e):
            log.warning("Останов: %s", e)
            try:
                print(f"\n{e}")
            except UnicodeEncodeError:
                print("\n" + str(e).encode("ascii", "replace").decode("ascii"))
        else:
            print("\nБот остановлен.")
    except Exception:
        release_lock()
        log.exception("Крэш процесса бота")
        raise
