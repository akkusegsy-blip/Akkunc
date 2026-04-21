import logging
import asyncio
import os
import io
import random
import time
from typing import Set, Dict, List, Any

from telegram import Update, ReactionTypeEmoji
from telegram.constants import ChatAction
from telegram.error import RetryAfter, TimedOut, NetworkError, BadRequest, Forbidden
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from gtts import gTTS

# ================= CONFIGURATION =================
TOKENS = [
    "8549116394:AAGz0SlaBv9G0w3jURlm1EDpQmHAvPExzgo", "8641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo" "8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560"
]

OWNER_ID = 7879852447

TEAM_A_TOKENS = TOKENS[:max(1,8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560 len(TOKENS)//2)]
TEAM_B_TOKENS = TOKENS[max(18641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo, len(TOKENS)//2):]
active_shift = "A"

# ================= TEXTS & EMOJIS =================
NC_TEXTS = [
    "×~GAY×~", "~×BITCH×~", "~×LESBIAN×~", "~×CHAPRI×~", "~×TMKC×~",
    "~×TMR×~", "~×TMKB×~", "~×CHUS×~", "~×HAKLE×~", "~×GAREEB×~",
    "~×RANDY×~", "~×POOR×~", "~×TATTI×~", "~×CHOR×~", "~×CHAMAR×~",
    "~×SPERM COLLECTOR×~", "~×KALWA×~", "~×CHUD×~"
]

CUSTOMNC_TEXTS = ["~×*×~", "~×**×~", "~×***×~", "~×****×~", "~×*****×~", "~×######×~", "~×!!!!!×~"]

NC_TEMPLATES = [
    "🔥 {text} 🔥", "⚡ {text} ⚡", "👑 {text} 👑", "💀 {text} 💀", "✨ {text} ✨",
    "💥 {text} 💥", "❄️ {text} ❄️", "🍀 {text} 🍀", "🍄 {text} 🍄", "🌹 {text} 🌹"
]

SPAM_TEXTS = [
    "🚀 TEAM AKKU BOT - BY AKKU BHAGWAN 🚀",
    "👑 AKKU BHAGWAN KA JALWA 👑",
    "💀 AKKU X TUFAN AGAIN 💀",
    "⚡ POWERED BY VIHAAN ⚡",
]

# Valid Telegram reaction emojis
VALID_REACTIONS = ["👎", "💩", "🤮", "🤡", "👺"]

GODSPEED_NAMES = [
    "⚡GODSPEED⚡", "🔥ULTRA🔥", "💀DEATH💀", "👑KING👑", "✨FLASH✨",
    "💥BLAST💥", "❄️FROST❄️", "🌪️STORM🌪️", "🎯TARGET🎯", "🚀ROCKET🚀"
]

# ================= GLOBAL STATE =================
MIN_DELAY = 0.5
DEFAULT_SPEED = 1.0

running_tasks: Dict[str, asyncio.Task] = {}
SUDO_USERS: Set[int] = {OWNER_ID}
slide_reply_targets: Dict[int, str] = {}
speed_settings: Dict[int, float] = {}
known_chats: Set[int] = set()
global_mode: bool = False
bot_start_time = time.time()
max_threads: int = 5
spam_templates: Dict[int, str] = {1: "🔥 {text} 🔥", 2: "⚡ {text} ⚡", 3: "💀 {text} 💀"}
target_names: Dict[int, str] = {}
nc_speeds: Dict[int, float] = {}

prefixes: Dict[int, str] = {}
delete_targets: Dict[int, Set[str]] = {}
auto_react_targets: Dict[int, Set[int]] = {}
delay_nc_settings: Dict[int, float] = {}

# ================= HELPERS & ENGINE =================
def is_admin(user_id: int) -> bool:
    return user_id in SUDO_USERS or user_id == OWNER_ID

def get_delay(chat_id: int) -> float:
    return max(MIN_DELAY, speed_settings.get(chat_id, DEFAULT_SPEED))

def get_nc_delay(chat_id: int) -> float:
    return max(MIN_DELAY, delay_nc_settings.get(chat_id, get_delay(chat_id) + 1.0))

def get_task_key(chat_id: int, action: str) -> str:
    return f"{chat_id}_{action}"

def get_pref(chat_id: int) -> str:
    return prefixes.get(chat_id, "/")

async def stop_task(chat_id: int, action: str) -> bool:
    key = get_task_key(chat_id, action)
    if key in running_tasks:
        task = running_tasks[key]
        if not task.done():
            task.cancel()
        if key in running_tasks:
            del running_tasks[key]
        return True
    return False

async def safe_api_request(func, *args, chat_id=None, **kwargs):
    """THE CORE SHIFT BALANCER ENGINE"""
    global active_shift
    bot_team = "A"  # default
    try:
        bot = func.__self__
        bot_team = "A" if (not TEAM_B_TOKENS or bot.token not in TEAM_B_TOKENS) else "B"

        # Shift Lock System - only block if multiple teams exist
        if TEAM_A_TOKENS and TEAM_B_TOKENS:
            attempts = 0
            while bot_team != active_shift and attempts < 20:
                await asyncio.sleep(0.5)
                attempts += 1

        result = await func(*args, **kwargs)
        return True
    except RetryAfter as e:
        print(f"⚠️ Rate limit on Shift {bot_team}! Switching Shift...")
        active_shift = "B" if active_shift == "A" else "A"
        await asyncio.sleep(int(e.retry_after))
        return False
    except (TimedOut, NetworkError):
        await asyncio.sleep(1)
        return False
    except BadRequest as e:
        print(f"BadRequest: {e}")
        return False
    except Forbidden as e:
        print(f"Forbidden: {e}")
        return False
    except Exception as e:
        if "Flood control exceeded" in str(e):
            active_shift = "B" if active_shift == "A" else "A"
            await asyncio.sleep(2)
        else:
            print(f"API Error: {e}")
        return False

# ================= COMMANDS =================

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    p = get_pref(update.effective_chat.id)
    menu = f"""
▛▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀✜
▌ ⑆ ━━━ ⟨ 👑 TEAM AKKU BOT V3 👑 ⟩ ━━━ ⑆
▌ ▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰▰
▌ ⊳ Owner : Akku Bhagwan
▌ ⊳ Engine: vihaan ( author)
▌ ⊳ Prefix: `{p}`
▌
▌ ┏━ 👑 Admin & System
▌ ┣ ◈ {p}pre <char>  — Change Prefix
▌ ┣ ◈ {p}sudo <id>  — Add Sudo User
▌ ┣ ◈ {p}delsudo <id>  — Remove Sudo
▌ ┣ ◈ {p}listsudo  — List Sudo Users
▌ ┣ ◈ {p}status / {p}test / {p}dashboard
▌
▌ ┏━ 🩸 Cyber Warfare (Spam)
▌ ┣ ◈ {p}spam <msg>  — Start Spam
▌ ┣ ◈ {p}Stopspm  — Stop Spam
▌ ┣ ◈ {p}raidspam  — Raid Spam
▌ ┣ ◈ {p}imagespam  — Image Spam
▌ ┣ ◈ {p}slidespam  — Slide Spam
▌ ┣ ◈ {p}slidestop  — Stop Slide
▌ ┣ ◈ {p}auto  — Auto React (Reply)
▌ ┣ ◈ {p}stopauto  — Stop Auto React
▌ ┣ ◈ {p}del <user>  — Auto Delete
▌
▌ ┏━ 🎯 Name Changer Storm
▌ ┣ ◈ {p}nc <base>  — Start NC
▌ ┣ ◈ {p}stopnc  — Stop NC
▌ ┣ ◈ {p}gcnc <base>  — GC Name Changer
▌ ┣ ◈ {p}ncbaap <base>  — Max Speed NC
▌ ┣ ◈ {p}godspeed  — Godspeed NC
▌ ┣ ◈ {p}exoncgodspeed  — Extreme Godspeed
▌ ┣ ◈ {p}customnc <base>  — Custom NC
▌
▌ ┏━ 📸 Mix Combo & PFP
▌ ┣ ◈ {p}changepfp  — Change Group PFP
▌ ┣ ◈ {p}Stoppfp  — Stop PFP Changer
▌ ┣ ◈ {p}spnc <base>  — Spam + NC Combo
▌ ┣ ◈ {p}stopspnc  — Stop SPNC
▌ ┣ ◈ {p}ncpfp <base>  — NC + PFP Combo
▌ ┣ ◈ {p}all <base>  — ALL IN ONE
▌
▌ ┏━ 🛑 Control
▌ ┣ ◈ {p}speed <val>  — Set Global Speed
▌ ┣ ◈ {p}delaync <val>  — Set NC Delay
▌ ┣ ◈ {p}stop  — Stop All Tasks
♱▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▟
    """
    if update.message:
        await update.message.reply_text(menu)

# ---- ADMIN COMMANDS ----

async def set_prefix(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if context.args:
        new_pref = context.args[0]
        prefixes[update.effective_chat.id] = new_pref
        await update.message.reply_text(
            f"✅ Prefix updated to `{new_pref}`\n"
            f"Now use `{new_pref}spam`, `{new_pref}nc`, etc.\n"
            f"Note: `/pre` (with slash) always works to change prefix back."
        )
    else:
        p = get_pref(update.effective_chat.id)
        await update.message.reply_text(f"ℹ️ Current prefix: `{p}`\nUsage: /pre <char>  e.g. /pre .")

async def add_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("❌ Only Owner can add sudo users.")
    if not context.args:
        return await update.message.reply_text("Usage: /sudo <user_id>")
    try:
        uid = int(context.args[0])
        SUDO_USERS.add(uid)
        await update.message.reply_text(f"✅ User `{uid}` added to sudo list.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def del_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != OWNER_ID:
        return await update.message.reply_text("❌ Only Owner can remove sudo users.")
    if not context.args:
        return await update.message.reply_text("Usage: /delsudo <user_id>")
    try:
        uid = int(context.args[0])
        if uid == OWNER_ID:
            return await update.message.reply_text("❌ Cannot remove Owner from sudo.")
        SUDO_USERS.discard(uid)
        await update.message.reply_text(f"✅ User `{uid}` removed from sudo list.")
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")

async def list_sudo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    sudo_list = "\n".join([f"• `{uid}`" for uid in SUDO_USERS])
    await update.message.reply_text(f"👑 **Sudo Users:**\n{sudo_list}")

async def test_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    start = time.time()
    msg = await update.message.reply_text("⏳ Testing...")
    ping = round((time.time() - start) * 1000, 2)
    await msg.edit_text(f"✅ Bot is alive! Ping: `{ping}ms`\n🤖 Active Bots: {len(TOKENS)}\n⚙️ Shift: {active_shift}")

async def dashboard_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    active = [k.replace(f"{chat_id}_", "") for k in running_tasks if k.startswith(f"{chat_id}_")]
    uptime = int(time.time() - bot_start_time)
    spd = speed_settings.get(chat_id, DEFAULT_SPEED)
    nc_dly = delay_nc_settings.get(chat_id, spd + 1.0)
    text = f"""
📊 **DASHBOARD — TEAM AKKU  BOT V3**
━━━━━━━━━━━━━━━━━━━━
⏱️ Uptime: `{uptime}s`
🤖 Total Bots: `{len(8641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo/8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560)}`
⚙️ Shift: `{active_shift}`
⚡ Speed: `{spd}s`
🔢 NC Delay: `{nc_dly}s`
━━━━━━━━━━━━━━━━━━━━
🏃 Active Tasks: {len(active)}
{chr(10).join([f"  • {t}" for t in active]) if active else "  None"}
━━━━━━━━━━━━━━━━━━━━
👑 Sudo Users: {len(SUDO_USERS)}
🗑️ Delete Targets: {len(delete_targets.get(chat_id, set()))}
🤮 React Targets: {len(auto_react_targets.get(chat_id, set()))}
    """
    await update.message.reply_text(text)

# ---- SPAM COMMANDS ----

async def spam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    message = " ".join(context.args) if context.args else "TEAM AKKU   BOT V3"
    await stop_task(chat_id, "spam")
    async def worker():
        while True:
            await safe_api_request(context.bot.send_message, chat_id, message, chat_id=chat_id)
            await asyncio.sleep(get_delay(chat_id))
    running_tasks[get_task_key(chat_id, "spam")] = asyncio.create_task(worker())
    await update.message.reply_text("🚀 SPAM STARTED")

async def raidspam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    await stop_task(chat_id, "spam")
    async def worker():
        while True:
            msg = random.choice(SPAM_TEXTS)
            await safe_api_request(context.bot.send_message, chat_id, msg, chat_id=chat_id)
            await asyncio.sleep(get_delay(chat_id))
    running_tasks[get_task_key(chat_id, "spam")] = asyncio.create_task(worker())
    await update.message.reply_text("🚀 RAID SPAM STARTED")

async def imagespam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    await stop_task(chat_id, "imgspam")
    # Generate colored image bytes on the fly
    async def make_image():
        try:
            from PIL import Image
            import io as _io
            color = (random.randint(0,255), random.randint(0,255), random.randint(0,255))
            img = Image.new("RGB", (200, 200), color)
            buf = _io.BytesIO()
            img.save(buf, format="JPEG")
            buf.seek(0)
            return buf
        except Exception:
            return None

    async def worker():
        i = 0
        while True:
            buf = await make_image()
            if buf:
                caption = f"⚡ TEAM AKKU BOT V3 — #{i+1}"
                await safe_api_request(context.bot.send_photo, chat_id, buf, caption=caption, chat_id=chat_id)
            else:
                await safe_api_request(context.bot.send_message, chat_id, f"⚡ IMAGE SPAM #{i+1}", chat_id=chat_id)
            i += 1
            await asyncio.sleep(get_delay(chat_id))
    running_tasks[get_task_key(chat_id, "imgspam")] = asyncio.create_task(worker())
    await update.message.reply_text("📸 IMAGE SPAM STARTED")

async def stop_spam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    stopped = await stop_task(update.effective_chat.id, "spam")
    stopped2 = await stop_task(update.effective_chat.id, "imgspam")
    if stopped or stopped2:
        await update.message.reply_text("🛑 SPAM STOPPED")
    else:
        await update.message.reply_text("⚠️ No active spam task.")

async def slidespam(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    message = " ".join(context.args) if context.args else "TEAM AKKU BOT V3"
    await stop_task(chat_id, "slide")
    last_msg_id = None
    async def worker():
        nonlocal last_msg_id
        while True:
            try:
                if last_msg_id:
                    sent = await context.bot.send_message(chat_id, message, reply_to_message_id=last_msg_id)
                else:
                    sent = await context.bot.send_message(chat_id, message)
                last_msg_id = sent.message_id
            except Exception as e:
                print(f"Slide spam error: {e}")
                last_msg_id = None
            await asyncio.sleep(get_delay(chat_id))
    running_tasks[get_task_key(chat_id, "slide")] = asyncio.create_task(worker())
    await update.message.reply_text("🔁 SLIDE SPAM STARTED")

async def slidestop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await stop_task(update.effective_chat.id, "slide"):
        await update.message.reply_text("🛑 SLIDE SPAM STOPPED")
    else:
        await update.message.reply_text("⚠️ No active slide spam.")

# ---- AUTO REACT & DELETE ----

async def del_user_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        return await update.message.reply_text("Usage: /del <username>")
    chat_id = update.effective_chat.id
    target = context.args[0].replace("@", "").lower()
    if chat_id not in delete_targets:
        delete_targets[chat_id] = set()
    delete_targets[chat_id].add(target)
    await update.message.reply_text(f"🎯 @{target} ke messages auto-delete honge.")

async def auto_react_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not update.message.reply_to_message:
        return await update.message.reply_text("❌ Kisi message ko reply karo target ke liye!")
    chat_id = update.effective_chat.id
    target_id = update.message.reply_to_message.from_user.id
    if chat_id not in auto_react_targets:
        auto_react_targets[chat_id] = set()
    auto_react_targets[chat_id].add(target_id)
    await update.message.reply_text(f"🤮 Auto-Reaction Active on user `{target_id}`!")

async def stop_auto_react(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    auto_react_targets.pop(update.effective_chat.id, None)
    await update.message.reply_text("🛑 Auto-Reaction Stopped.")

# ---- NC COMMANDS ----

async def rename(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else ""
    await stop_task(chat_id, "nc")
    async def worker():
        i = 0
        while True:
            template = NC_TEMPLATES[i % len(NC_TEMPLATES)]
            nc_text = NC_TEXTS[i % len(NC_TEXTS)]
            name = template.format(text=f"{base} {nc_text}".strip())[:255]
            await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            i += 1
            await asyncio.sleep(get_nc_delay(chat_id))
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("🚀 NC STARTED")

async def gcnc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else "GC"
    await stop_task(chat_id, "nc")
    async def worker():
        i = 0
        while True:
            nc_text = NC_TEXTS[i % len(NC_TEXTS)]
            name = f"👑 {base} | {nc_text}"[:255]
            await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            i += 1
            await asyncio.sleep(get_nc_delay(chat_id))
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("🚀 GC NC STARTED")

async def ncbaap(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else ""
    await stop_task(chat_id, "nc")
    async def worker():
        while True:
            template = random.choice(NC_TEMPLATES)
            nc_text = random.choice(NC_TEXTS)
            name = template.format(text=f"{base} {nc_text}".strip())[:255]
            success = await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            await asyncio.sleep(0.5 if success else 1.0)
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("💀🔥 NCBAAP MAX SPEED ACTIVATED")

async def godspeed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    await stop_task(chat_id, "nc")
    async def worker():
        while True:
            name = random.choice(GODSPEED_NAMES)
            success = await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            await asyncio.sleep(0.5 if success else 1.0)
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("⚡ GODSPEED NC ACTIVATED")

async def exoncgodspeed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    await stop_task(chat_id, "nc")
    async def worker():
        while True:
            name1 = random.choice(GODSPEED_NAMES)
            name2 = random.choice(NC_TEXTS)
            name = f"{name1} {name2}"[:255]
            success = await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            await asyncio.sleep(0.3 if success else 0.8)
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("🔥💀 EXTREME GODSPEED ACTIVATED")

async def customnc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else ""
    await stop_task(chat_id, "nc")
    async def worker():
        i = 0
        while True:
            custom_text = CUSTOMNC_TEXTS[i % len(CUSTOMNC_TEXTS)]
            template = NC_TEMPLATES[i % len(NC_TEMPLATES)]
            name = template.format(text=f"{base} {custom_text}".strip())[:255]
            await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            i += 1
            await asyncio.sleep(get_nc_delay(chat_id))
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(worker())
    await update.message.reply_text("✨ CUSTOM NC STARTED")

async def stop_nc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await stop_task(update.effective_chat.id, "nc"):
        await update.message.reply_text("🛑 NC STOPPED")
    else:
        await update.message.reply_text("⚠️ No active NC task.")

# ---- COMBO COMMANDS ----

async def spnc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else ""
    await stop_task(chat_id, "spnc")
    async def worker():
        i = 0
        while True:
            spam_msg = random.choice(SPAM_TEXTS)
            template = NC_TEMPLATES[i % len(NC_TEMPLATES)]
            nc_text = NC_TEXTS[i % len(NC_TEXTS)]
            name = template.format(text=f"{base} {nc_text}".strip())[:255]
            asyncio.create_task(safe_api_request(context.bot.send_message, chat_id, f"{base}\n{spam_msg}".strip(), chat_id=chat_id))
            asyncio.create_task(safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id))
            i += 1
            await asyncio.sleep(get_delay(chat_id) + 1.5)
    running_tasks[get_task_key(chat_id, "spnc")] = asyncio.create_task(worker())
    await update.message.reply_text("🔁 SPNC (SPAM+NC) STARTED")

async def stop_spnc(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await stop_task(update.effective_chat.id, "spnc"):
        await update.message.reply_text("🛑 SPNC STOPPED")
    else:
        await update.message.reply_text("⚠️ No active SPNC task.")

async def changepfp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    await stop_task(chat_id, "pfp")
    async def make_pfp_image():
        try:
            from PIL import Image, ImageDraw
            import io as _io
            color = (random.randint(0,255), random.randint(0,255), random.randint(0,255))
            img = Image.new("RGB", (256, 256), color)
            draw = ImageDraw.Draw(img)
            draw.ellipse([30, 30, 226, 226], fill=(
                255 - color[0], 255 - color[1], 255 - color[2]
            ))
            buf = _io.BytesIO()
            img.save(buf, format="JPEG")
            buf.seek(0)
            return buf
        except Exception:
            return None

    async def worker():
        while True:
            buf = await make_pfp_image()
            if buf:
                await safe_api_request(context.bot.set_chat_photo, chat_id, buf, chat_id=chat_id)
            await asyncio.sleep(get_delay(chat_id) + 2.0)
    running_tasks[get_task_key(chat_id, "pfp")] = asyncio.create_task(worker())
    await update.message.reply_text("📸 PFP CHANGER STARTED")

async def stop_pfp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await stop_task(update.effective_chat.id, "pfp"):
        await update.message.reply_text("🛑 PFP CHANGER STOPPED")
    else:
        await update.message.reply_text("⚠️ No active PFP task.")

async def ncpfp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else ""
    await stop_task(chat_id, "ncpfp")
    async def make_pfp_image():
        try:
            from PIL import Image
            import io as _io
            color = (random.randint(0,255), random.randint(0,255), random.randint(0,255))
            img = Image.new("RGB", (256, 256), color)
            buf = _io.BytesIO()
            img.save(buf, format="JPEG")
            buf.seek(0)
            return buf
        except Exception:
            return None

    async def worker():
        i = 0
        while True:
            template = NC_TEMPLATES[i % len(NC_TEMPLATES)]
            nc_text = NC_TEXTS[i % len(NC_TEXTS)]
            name = template.format(text=f"{base} {nc_text}".strip())[:255]
            buf = await make_pfp_image()
            asyncio.create_task(safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id))
            if buf:
                asyncio.create_task(safe_api_request(context.bot.set_chat_photo, chat_id, buf, chat_id=chat_id))
            i += 1
            await asyncio.sleep(get_nc_delay(chat_id))
    running_tasks[get_task_key(chat_id, "ncpfp")] = asyncio.create_task(worker())
    await update.message.reply_text("🔥 NC+PFP COMBO STARTED")

async def stop_ncpfp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await stop_task(update.effective_chat.id, "ncpfp"):
        await update.message.reply_text("🛑 NC+PFP STOPPED")

async def all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start ALL tasks at once: spam + nc + pfp"""
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    base = " ".join(context.args) if context.args else "ALL"

    # Stop everything first
    for action in ["spam", "nc", "pfp", "spnc", "ncpfp", "imgspam", "slide"]:
        await stop_task(chat_id, action)

    # Start spam
    async def spam_worker():
        while True:
            msg = random.choice(SPAM_TEXTS)
            await safe_api_request(context.bot.send_message, chat_id, msg, chat_id=chat_id)
            await asyncio.sleep(get_delay(chat_id))
    running_tasks[get_task_key(chat_id, "spam")] = asyncio.create_task(spam_worker())

    # Start NC
    async def nc_worker():
        i = 0
        while True:
            template = NC_TEMPLATES[i % len(NC_TEMPLATES)]
            nc_text = NC_TEXTS[i % len(NC_TEXTS)]
            name = template.format(text=f"{base} {nc_text}".strip())[:255]
            await safe_api_request(context.bot.set_chat_title, chat_id, name, chat_id=chat_id)
            i += 1
            await asyncio.sleep(get_nc_delay(chat_id))
    running_tasks[get_task_key(chat_id, "nc")] = asyncio.create_task(nc_worker())

    await update.message.reply_text("🔥⚡ ALL IN ONE ACTIVATED — SPAM + NC RUNNING")

# ---- CONTROL COMMANDS ----

async def speed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        return await update.message.reply_text("Usage: /speed <seconds>")
    try:
        val = float(context.args[0])
        if val < MIN_DELAY:
            val = MIN_DELAY
        speed_settings[update.effective_chat.id] = val
        await update.message.reply_text(f"⚡ SPEED SET TO {val}s")
    except ValueError:
        await update.message.reply_text("❌ Invalid value. Use a number like 0.5 or 1.0")

async def delaync(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    if not context.args:
        return await update.message.reply_text("Usage: /delaync <seconds>")
    try:
        val = float(context.args[0])
        if val < MIN_DELAY:
            val = MIN_DELAY
        delay_nc_settings[update.effective_chat.id] = val
        await update.message.reply_text(f"⚡ NC DELAY SET TO {val}s")
    except ValueError:
        await update.message.reply_text("❌ Invalid value.")

async def stop_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id): return
    chat_id = update.effective_chat.id
    to_rem = [k for k in list(running_tasks.keys()) if k.startswith(f"{chat_id}_")]
    count = 0
    for k in to_rem:
        if not running_tasks[k].done():
            running_tasks[k].cancel()
        del running_tasks[k]
        count += 1
    delete_targets.pop(chat_id, None)
    auto_react_targets.pop(chat_id, None)
    await update.message.reply_text(f"🛑 ALL TASKS STOPPED. ({count} tasks halted)")

async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uptime = int(time.time() - bot_start_time)
    h = uptime // 3600
    m = (uptime % 3600) // 60
    s = uptime % 60
    text = f"""
📊 **TEAM AKKU BOT V3 — STATUS**
━━━━━━━━━━━━━━━━━━━━
⏱️ Uptime: `{h}h {m}m {s}s`
🤖 Active Bots: `{len(8641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo/8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560)}`
🏃 Running Tasks: `{len(running_tasks)}`
⚙️ Active Shift: `{active_shift}`
👑 Sudo Users: `{len(SUDO_USERS)}`
━━━━━━━━━━━━━━━━━━━━
    """
    if update.message:
        await update.message.reply_text(text)

# ================= CUSTOM PREFIX DISPATCHER =================
COMMAND_MAP = {
    "start":          help_command,
    "help":           help_command,
    "pre":            set_prefix,
    "sudo":           add_sudo,
    "delsudo":        del_sudo,
    "listsudo":       list_sudo,
    "Listsudo":       list_sudo,
    "status":         status_cmd,
    "test":           test_cmd,
    "dashboard":      dashboard_cmd,
    "del":            del_user_cmd,
    "auto":           auto_react_cmd,
    "stopauto":       stop_auto_react,
    "spam":           spam,
    "raidspam":       raidspam,
    "imagespam":      imagespam,
    "stopspm":        stop_spam,
    "Stopspm":        stop_spam,
    "slidespam":      slidespam,
    "slidestop":      slidestop,
    "nc":             rename,
    "gcnc":           gcnc,
    "ncbaap":         ncbaap,
    "godspeed":       godspeed,
    "exoncgodspeed":  exoncgodspeed,
    "customnc":       customnc,
    "stopnc":         stop_nc,
    "spnc":           spnc,
    "stopspnc":       stop_spnc,
    "changepfp":      changepfp,
    "stoppfp":        stop_pfp,
    "Stoppfp":        stop_pfp,
    "ncpfp":          ncpfp,
    "stopncpfp":      stop_ncpfp,
    "all":            all_cmd,
    "speed":          speed,
    "delaync":        delaync,
    "stop":           stop_all,
}

async def custom_prefix_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Intercepts messages that use a custom prefix (e.g. . or !) and routes them."""
    if not update.message or not update.message.text:
        return
    chat_id = update.effective_chat.id
    pref = prefixes.get(chat_id)
    if not pref or pref == "/":
        return  # default prefix — handled by CommandHandler already

    text = update.message.text.strip()
    if not text.startswith(pref):
        return  # not a prefixed command

    # Strip prefix, split into command + args
    rest = text[len(pref):]
    parts = rest.split()
    if not parts:
        return
    cmd = parts[0].split("@")[0]   # strip bot@username suffix if present
    args = parts[1:]

    handler_func = COMMAND_MAP.get(cmd) or COMMAND_MAP.get(cmd.lower())
    if not handler_func:
        return  # unknown command, ignore

    # Inject args so handlers can read context.args normally
    context.args = args
    await handler_func(update, context)

# ================= BACKGROUND LISTENER =================
async def message_filter_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    user = update.effective_user
    if not user:
        return

    # Skip if this looks like a custom-prefix command (already handled above)
    text = update.message.text or ""
    pref = prefixes.get(chat_id)
    if pref and pref != "/" and text.startswith(pref):
        return

    # Auto Delete
    if chat_id in delete_targets and user.username:
        if user.username.lower() in delete_targets[chat_id]:
            try:
                await update.message.delete()
                return
            except Exception:
                pass

    # Auto React
    if chat_id in auto_react_targets and user.id in auto_react_targets[chat_id]:
        try:
            emoji = random.choice(VALID_REACTIONS)
            await update.message.set_reaction([ReactionTypeEmoji(emoji=emoji)])
        except Exception as e:
            print(f"Reaction error: {e}")

# ================= MAIN RUNNER =================
async def run_bots():
    if not TOKENS:
        print("❌ No bot tokens found! Set BOT_TOKEN_1,8641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo BOT_TOKEN_2, 8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560 etc. as environment variables.")
        print("📌 Or set a single token as BOT_TOKEN_1")
        return

    print(f"👑 TEAM AKKU BOT V3 — BY AKKU BHAGWAN")
    print(f"🛡️ Team A: {len(TEAM_A_TOKENS)}8641515315:AAEITZ-wCfh8XvtH9mCd-3vb-jGmfD3UpCo bots | 🛡️ Team B: {len(TEAM_B_TOKENS)}8229275902:AAHV0UOdOOr75X-YtTDAsEveQXZLF-Fp560 bots")

    apps = []
    for token in TOKENS:
        try:
            app = Application.builder().token(token).build()

            # System & Admin
            app.add_handler(CommandHandler(["start", "help"], help_command))
            app.add_handler(CommandHandler("pre", set_prefix))
            app.add_handler(CommandHandler("sudo", add_sudo))
            app.add_handler(CommandHandler("delsudo", del_sudo))
            app.add_handler(CommandHandler("listsudo", list_sudo))
            app.add_handler(CommandHandler("Listsudo", list_sudo))
            app.add_handler(CommandHandler("status", status_cmd))
            app.add_handler(CommandHandler("test", test_cmd))
            app.add_handler(CommandHandler("dashboard", dashboard_cmd))
            app.add_handler(CommandHandler("del", del_user_cmd))
            app.add_handler(CommandHandler("auto", auto_react_cmd))
            app.add_handler(CommandHandler("stopauto", stop_auto_react))

            # Spam
            app.add_handler(CommandHandler("spam", spam))
            app.add_handler(CommandHandler("raidspam", raidspam))
            app.add_handler(CommandHandler("imagespam", imagespam))
            app.add_handler(CommandHandler("Stopspm", stop_spam))
            app.add_handler(CommandHandler("stopspm", stop_spam))
            app.add_handler(CommandHandler("slidespam", slidespam))
            app.add_handler(CommandHandler("slidestop", slidestop))

            # NC Commands
            app.add_handler(CommandHandler("nc", rename))
            app.add_handler(CommandHandler("gcnc", gcnc))
            app.add_handler(CommandHandler("ncbaap", ncbaap))
            app.add_handler(CommandHandler("godspeed", godspeed))
            app.add_handler(CommandHandler("exoncgodspeed", exoncgodspeed))
            app.add_handler(CommandHandler("customnc", customnc))
            app.add_handler(CommandHandler("stopnc", stop_nc))

            # Combo Commands
            app.add_handler(CommandHandler("spnc", spnc))
            app.add_handler(CommandHandler("stopspnc", stop_spnc))
            app.add_handler(CommandHandler("changepfp", changepfp))
            app.add_handler(CommandHandler("Stoppfp", stop_pfp))
            app.add_handler(CommandHandler("stoppfp", stop_pfp))
            app.add_handler(CommandHandler("ncpfp", ncpfp))
            app.add_handler(CommandHandler("stopncpfp", stop_ncpfp))
            app.add_handler(CommandHandler("all", all_cmd))

            # Control
            app.add_handler(CommandHandler("speed", speed))
            app.add_handler(CommandHandler("delaync", delaync))
            app.add_handler(CommandHandler("stop", stop_all))

            # Custom prefix dispatcher (group 0 — runs before message_filter_handler)
            app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, custom_prefix_handler), group=0)

            # Universal message listener (group 1 — auto-delete / auto-react)
            app.add_handler(MessageHandler(filters.ALL, message_filter_handler), group=1)

            await app.initialize()
            await app.start()
            if app.updater:
                await app.updater.start_polling(drop_pending_updates=True)
            apps.append(app)
            print(f"✅ Bot started: {token[:10]}...")
        except Exception as e:
            print(f"❌ Failed to start bot: {e}")

    if not apps:
        print("❌ No bots started successfully. Check your tokens.")
        return

    print(f"🚀 {len(apps)} bot(s) running! Waiting for commands...")
    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(run_bots())
    except KeyboardInterrupt:
        print("🛑 Bot stopped by user.")
    except Exception as e:
        print(f"❌ Fatal error: {e}")
