"""
Telegram Reminder Bot - Main Application
Handles all bot commands and reminder scheduling
"""

import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from dotenv import load_dotenv

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.error import TelegramError
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ConversationHandler, filters, ContextTypes, JobQueue
)
from telegram.ext import TypeHandler
from datetime import datetime
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

# For webhook
from flask import Flask, request
import threading

from database import Database
from reminder_manager import ReminderManager

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
def _mask_token(t: Optional[str]) -> str:
    if not t:
        return "<NONE>"
    if len(t) <= 8:
        return "********"
    return f"{t[:4]}...{t[-4:]}"


# States for conversation handlers
REMINDER_TEXT, REMINDER_DAY, REMINDER_FREQUENCY, REMINDER_CONFIRM = range(4)
EDIT_CHOICE, EDIT_TEXT, EDIT_TIME, EDIT_FREQUENCY = range(4, 8)

class NoWeakrefJobQueue(JobQueue):
    """A JobQueue that doesn't use weakrefs.
    This is a workaround for Python 3.11+ on some platforms where
    weakref.ref(Application) fails.
    """
    def set_application(self, application) -> None:
        self._application = application

class ReminderBot:
    def __init__(self):
        self.token = os.getenv('TELEGRAM_BOT_TOKEN')
        if self.token:
            self.token = self.token.strip()
        logger.info(f"Bot token loaded: {_mask_token(self.token)}")
        if not self.token or ':' not in self.token:
            raise RuntimeError(
                "TELEGRAM_BOT_TOKEN is missing or looks invalid. Set a valid token from BotFather in your .env file."
            )
        self.db = Database(spreadsheet_name='reminders') # Make sure this matches your Google Sheet name exactly
        self.reminder_manager = ReminderManager(self.db)
        self.vn_tz = pytz.timezone('Asia/Ho_Chi_Minh')
        self.scheduler = AsyncIOScheduler(timezone=self.vn_tz)
        self.app = None
        self.debug_always_on = os.getenv('DEBUG_ALWAYS_ON', '').strip().lower() in (
            '1', 'true', 'yes', 'on'
        )
        if self.debug_always_on:
            logger.info("Debug mode: active window disabled; polling always on")
        self.window_only = os.getenv('WINDOW_ONLY', '').strip().lower() in (
            '1', 'true', 'yes', 'on'
        )
        if self.window_only and self.debug_always_on:
            logger.warning("WINDOW_ONLY=1 overrides DEBUG_ALWAYS_ON=1")
        self.freq_translation = {
            'once': 'Một lần',
            'daily': 'Hàng ngày',
            'weekly': 'Hàng tuần',
            'monthly': 'Hàng tháng'
        }
        # Active window in Vietnam time (UTC+7)
        self.active_start_hm = (7, 30)
        self.active_end_hm = (7,  40)
        # Daily batch reminder time (VN)
        self.notify_hm = (7, 35)

        # Inactivity sliding timeout (only effective outside active hours)
        self.inactivity_minutes = 3
        self._last_activity_vn: Optional[datetime] = None
        self._inactivity_task: Optional[asyncio.Task] = None
        self._peek_offset: Optional[int] = None

        self._polling_lock = asyncio.Lock()
        self._peek_lock = asyncio.Lock()

    def _is_active_hours(self, dt: Optional[datetime] = None) -> bool:
        if getattr(self, 'debug_always_on', False):
            return True
        tz = getattr(self, 'vn_tz', pytz.timezone('Asia/Ho_Chi_Minh'))
        now_vn = dt.astimezone(tz) if dt else datetime.now(tz)
        sh, sm = self.active_start_hm
        eh, em = self.active_end_hm
        start = now_vn.replace(hour=sh, minute=sm, second=0, microsecond=0)
        end = now_vn.replace(hour=eh, minute=em, second=0, microsecond=0)
        return start <= now_vn < end

    def _default_time_str(self) -> str:
        hour, minute = self.notify_hm
        return f"{hour:02d}:{minute:02d}"

    def _pre_sleep_hm(self) -> tuple[int, int]:
        eh, em = self.active_end_hm
        if em == 0:
            return ((eh - 1) % 24, 59)
        return (eh, em - 1)

    def _parse_user_id(self, value) -> Optional[int]:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    def _get_all_user_ids(self, reminders: Optional[List[Dict]] = None) -> List[int]:
        user_ids = set()
        try:
            for user in self.db.get_all_users():
                parsed = self._parse_user_id(user.get('id'))
                if parsed is not None:
                    user_ids.add(parsed)
        except Exception as e:
            logger.warning(f"Failed to load users for daily status: {e}")

        if reminders is None:
            try:
                reminders = self.db.get_all_active_reminders()
            except Exception as e:
                logger.warning(f"Failed to load reminders for daily status: {e}")
                reminders = []
        for reminder in reminders:
            parsed = self._parse_user_id(reminder.get('user_id'))
            if parsed is not None:
                user_ids.add(parsed)

        return sorted(user_ids)

    def _reminder_due_today(self, reminder: Dict) -> bool:
        tz_name = reminder.get('timezone', 'Asia/Ho_Chi_Minh')
        try:
            tz = pytz.timezone(tz_name)
        except pytz.UnknownTimeZoneError:
            tz = self.vn_tz
        now = datetime.now(tz)
        today = now.date()
        frequency = (reminder.get('frequency') or '').lower()

        if frequency == 'daily':
            return True
        if frequency == 'weekly':
            return now.weekday() == 0
        if frequency in ('monthly', 'once'):
            try:
                next_event = self._compute_next_month_event(reminder)
            except Exception:
                return False
            due_dates = {
                next_event.date(),
                (next_event - timedelta(days=1)).date(),
                (next_event - timedelta(days=2)).date(),
            }
            return today in due_dates
        return False

    def _seconds_until_active_end(self, dt: Optional[datetime] = None) -> float:
        tz = getattr(self, 'vn_tz', pytz.timezone('Asia/Ho_Chi_Minh'))
        now_vn = dt.astimezone(tz) if dt else datetime.now(tz)
        eh, em = self.active_end_hm
        end = now_vn.replace(hour=eh, minute=em, second=0, microsecond=0)
        if end <= now_vn:
            end = end + timedelta(days=1)
        return max(0.0, (end - now_vn).total_seconds())

    def _compute_next_month_event(self, reminder: Dict) -> datetime:
        # Returns datetime of next event occurrence in reminder timezone
        user_tz = pytz.timezone(reminder.get('timezone', 'Asia/Ho_Chi_Minh'))
        now = datetime.now(user_tz)
        hour, minute = self.notify_hm
        target_day = reminder.get('day', now.day)
        try:
            run_dt = now.replace(day=target_day, hour=hour, minute=minute, second=0, microsecond=0)
        except ValueError:
            import calendar
            last_day = calendar.monthrange(now.year, now.month)[1]
            run_dt = now.replace(day=last_day, hour=hour, minute=minute, second=0, microsecond=0)
        if run_dt <= now:
            # move to next month
            year = run_dt.year + (1 if run_dt.month == 12 else 0)
            month = 1 if run_dt.month == 12 else run_dt.month + 1
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            day = min(target_day, last_day)
            run_dt = run_dt.replace(year=year, month=month, day=day)
        return run_dt

    def _schedule_offset_job(self, reminder: Dict, run_dt: datetime, offset_days: int):
        job_base = f"reminder_{reminder['id']}_dminus{offset_days}"
        # Remove existing job if present
        try:
            self.scheduler.remove_job(job_base)
        except Exception:
            pass
        tz = pytz.timezone(reminder.get('timezone', 'Asia/Ho_Chi_Minh'))
        trigger = DateTrigger(run_date=run_dt, timezone=tz)
        self.scheduler.add_job(
            self.send_reminder_job,
            trigger=trigger,
            args=(reminder['user_id'], reminder, offset_days),
            id=job_base,
            replace_existing=True,
            misfire_grace_time=300
        )
        logger.info(f"Scheduled offset job {job_base} at {run_dt}")

    async def _start_polling_if_needed(self, source: str):
        async with self._polling_lock:
            if self.app.updater.running:
                return
            await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

            # Cancel inactivity task when entering active hours by schedule
            if self._is_active_hours() and self._inactivity_task and not self._inactivity_task.done():
                self._inactivity_task.cancel()
            now_vn = datetime.now(self.vn_tz).strftime("%Y-%m-%d %H:%M:%S %z")
            if source == 'schedule':
                logger.info(f"wake_by_schedule at {now_vn} (VN)")
            else:
                logger.info(f"wake_by_message at {now_vn} (VN)")

    async def _stop_polling_if_needed(self, reason: str):
        async with self._polling_lock:
            if not self.app.updater.running:
                return
            # Do not sleep if inside active hours
            if self._is_active_hours():
                return
            await self.app.updater.stop()
            now_vn = datetime.now(self.vn_tz).strftime("%Y-%m-%d %H:%M:%S %z")
            if reason == 'schedule':
                logger.info(f"sleep_by_schedule at {now_vn} (VN)")
            else:
                logger.info(f"sleep_by_timeout at {now_vn} (VN)")

    def _register_activity(self):
        now = datetime.now(self.vn_tz)
        self._last_activity_vn = now
        if not self._is_active_hours():
            # Only manage inactivity outside active hours
            self._schedule_inactivity_timer()
            logger.info("activity_extend(T+5m)")

    def _schedule_inactivity_timer(self):
        if self._inactivity_task and not self._inactivity_task.done():
            self._inactivity_task.cancel()
        self._inactivity_task = asyncio.create_task(self._inactivity_countdown())

    async def _inactivity_countdown(self):
        try:
            while True:
                if self._last_activity_vn is None:
                    return
                wake_deadline = self._last_activity_vn + timedelta(minutes=self.inactivity_minutes)
                now = datetime.now(self.vn_tz)
                if now >= wake_deadline:
                    # timeout reached
                    await self._stop_polling_if_needed("timeout")
                    return
                # If we enter active hours during wait, break without sleeping
                if self._is_active_hours():
                    return
                await asyncio.sleep(min(30, max(1, (wake_deadline - now).total_seconds())))
        except asyncio.CancelledError:
            pass

    async def _activity_hook(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        # Called for every update when polling is running
        self._register_activity()

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name

        self.db.add_user(user_id, user_name)

        welcome_text = (
            f"👋 Chào mừng {user_name}!\n\n"
            "Tôi là Bot Nhắc Nhở Thanh Toán Hóa Đơn. Tôi sẽ giúp bạn không bao giờ quên một khoản thanh toán nào.\n\n"
            "Để bắt đầu, hãy sử dụng lệnh /add để tạo lời nhắc thanh toán hóa đơn đầu tiên của[object Object] Tạo lời nhắc hóa đơn mới\n"
            "/list - Xem tất cả các lời nhắc\n"
            "/remove - Xóa một lời nhắc\n"
            "/help - Xem hướng dẫn chi tiết\n"
        )

        await update.message.reply_text(welcome_text)

    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /help command"""
        help_text = (
            "*Trợ giúp Bot Nhắc Nhở Hóa Đơn*\n\n"
            "Bot này giúp bạn quản lý và nhận lời nhắc cho các hóa đơn định kỳ.\n\n"
            "*Các lệnh chính:*\n"
            "• `/add` - Tạo một lời nhắc thanh toán hóa đơn mới.\n"
            "• `/list` - Hiển thị tất cả các lời nhắc của bạn.\n"
            "• `/remove` - Xóa một lời nhắc theo ID của nó.\n"
            "• `/set_timezone` - Đặt múi giờ của bạn để nhận thông báo đúng giờ.\n\n"
            "*Cách tạo lời nhắc hóa đơn:*\n"
            "1. Dùng lệnh `/add`.\n"
            "2. Nhập nội dung hóa đơn (ví dụ: Thanh toán tiền điện).\n"
            "3. Thời gian nhắc mặc định: 07:30 (không cần nhập).\n"
            "4. Chọn tần suất lặp lại.\n\n"
            "*Các tùy chọn tần suất:*\n"
            "• **Một lần:** Chỉ nhắc một lần duy nhất.\n"
            "• **Hàng ngày:** Lặp lại mỗi ngày.\n"
            "• **Hàng tuần:** Lặp lại vào cùng một ngày mỗi tuần.\n"
            "• **Hàng tháng:** Lặp lại vào cùng một ngày mỗi tháng.\n"
        )

        await update.message.reply_text(help_text, parse_mode='Markdown')

    async def menu_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show command menu."""
        keyboard = [
            ["/add", "/list"],
            ["/remove", "/set_timezone"],
            ["/help"],
        ]
        reply_markup = ReplyKeyboardMarkup(
            keyboard=keyboard,
            resize_keyboard=True,
            one_time_keyboard=True
        )
        menu_text = (
            "Menu lenh:\n"
            "/add - Tao loi nhac moi\n"
            "/list - Xem danh sach\n"
            "/remove - Xoa loi nhac\n"
            "/set_timezone - Dat mui gio\n"
            "/help - Huong dan chi tiet"
        )
        await update.message.reply_text(menu_text, reply_markup=reply_markup)

    async def new_reminder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Start new reminder creation and check for timezone setting."""
        user_id = update.effective_user.id
        user_timezone = self.db.get_user_timezone(user_id)

        if not user_timezone or user_timezone == 'UTC':
            await update.message.reply_text(
                "💡 *Mẹo:* Bạn chưa đặt múi giờ của mình. Lời nhắc sẽ mặc định theo giờ UTC.\n\n"
                "Để nhận thông báo chính xác theo giờ địa phương, hãy dùng lệnh (ví dụ):\n"
                "`/set_timezone Asia/Ho_Chi_Minh`",
                parse_mode='Markdown'
            )

        await update.message.reply_text(
            "📝 **Bắt đầu tạo lời nhắc mới...**\n\n"
            "Nội dung lời nhắc là gì?"
        )
        return REMINDER_TEXT

    async def reminder_text_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle reminder text input"""
        context.user_data['reminder_text'] = update.message.text

        keyboard = [[InlineKeyboardButton("Ngày hôm nay", callback_data="day_today")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(
            "📅 Bạn muốn được nhắc vào ngày nào trong tháng (1-31)?\n\n"
            "Đối với lời nhắc hàng tháng, đây sẽ là ngày nhắc của mỗi tháng.",
            reply_markup=reply_markup
        )
        return REMINDER_DAY


    async def reminder_day_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle reminder day input from text or callback."""
        day_str = ""
        query = update.callback_query

        if query:
            await query.answer()
            if query.data == 'day_today':
                day_str = str(datetime.now().day)
            # Remove the button after selection
            await query.edit_message_reply_markup(reply_markup=None)
        else:
            day_str = update.message.text.strip()

        try:
            day = int(day_str)
            if not 1 <= day <= 31:
                raise ValueError("Day out of range")
            context.user_data['reminder_day'] = day
        except (ValueError, TypeError):
            await update.effective_message.reply_text(
                "❌ Ngày không hợp lệ. Vui lòng nhập một số từ 1 đến 31."
            )
            return REMINDER_DAY

        context.user_data['reminder_time'] = self._default_time_str()

        # Show frequency options
        keyboard = [
            [InlineKeyboardButton("Một lần", callback_data="freq_once")],
            [InlineKeyboardButton("Hàng ngày", callback_data="freq_daily")],
            [InlineKeyboardButton("Hàng tuần", callback_data="freq_weekly")],
            [InlineKeyboardButton("Hàng tháng", callback_data="freq_monthly")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.effective_message.reply_text(
            "🔄 Bạn muốn lặp lại lời nhắc này như thế nào?",
            reply_markup=reply_markup
        )
        return REMINDER_FREQUENCY

    async def frequency_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle frequency selection"""
        query = update.callback_query
        await query.answer()

        freq_map = {
            'freq_once': 'once',
            'freq_daily': 'daily',
            'freq_weekly': 'weekly',
            'freq_monthly': 'monthly'
        }

        frequency = freq_map.get(query.data, 'once')
        context.user_data['reminder_frequency'] = frequency

        # Show confirmation
        reminder_text = context.user_data.get('reminder_text', '')
        reminder_day = context.user_data.get('reminder_day', '')
        default_time = self._default_time_str()
        reminder_time = context.user_data.get('reminder_time') or default_time
        context.user_data['reminder_time'] = reminder_time

        translated_freq = self.freq_translation.get(frequency, frequency)
        confirm_text = (
            f"✅ *Tóm tắt lời nhắc*\n\n"
            f"📝 Nội dung: {reminder_text}\n"
            f"📅 Ngày: {reminder_day}\n"
            f"⏰ Thời gian: {reminder_time}\n"
            f"🔄 Tần suất: {translated_freq}\n\n"
            f"Nội dung này đã đúng chưa?"
        )

        keyboard = [
            [InlineKeyboardButton("✅ Có", callback_data="confirm_yes"),
             InlineKeyboardButton("❌ Không", callback_data="confirm_no")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await query.edit_message_text(
            confirm_text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
        return REMINDER_CONFIRM

    async def confirm_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle reminder confirmation"""
        query = update.callback_query
        await query.answer()

        if query.data == "confirm_yes":
            user_id = update.effective_user.id
            reminder_time = context.user_data.get('reminder_time') or self._default_time_str()
            context.user_data['reminder_time'] = reminder_time
            reminder_data = {
                'text': context.user_data.get('reminder_text'),
                'day': context.user_data.get('reminder_day'),
                'time': reminder_time,
                'frequency': context.user_data.get('reminder_frequency'),
                'timezone': self.db.get_user_timezone(user_id) or 'UTC'
            }

            reminder_id = self.reminder_manager.create_reminder(user_id, reminder_data)
            new_reminder = self.db.get_reminder(reminder_id)

            if new_reminder:
                self._schedule_single_reminder(new_reminder)
                job_id = f"reminder_{reminder_id}_dminus0"
                job = self.scheduler.get_job(job_id)

                time_left_str = ""
                if job and job.next_run_time:
                    now = datetime.now(job.next_run_time.tzinfo)
                    time_diff = job.next_run_time - now

                    days = time_diff.days
                    hours, remainder = divmod(time_diff.seconds, 3600)
                    minutes, _ = divmod(remainder, 60)

                    parts = []
                    if days > 0:
                        parts.append(f"{days} ngày")
                    if hours > 0:
                        parts.append(f"{hours} giờ")
                    if minutes > 0:
                        parts.append(f"{minutes} phút")

                    if not parts and time_diff.total_seconds() > 0:
                        parts.append("chưa đầy một phút")

                    if parts:
                        time_left_str = f"\n🔔 Sẽ được gửi sau {' '.join(parts)}."

                await query.edit_message_text(
                    f"✅ Đã tạo lời nhắc thành công!\n"
                    f"ID Lời nhắc: {reminder_id}"
                    f"{time_left_str}"
                )
            else:
                await query.edit_message_text("❌ Đã xảy ra lỗi khi tạo lời nhắc.")
        else:
            await query.edit_message_text("❌ Đã hủy tạo lời nhắc.")

        return ConversationHandler.END

    async def list_reminders(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all reminders for the user"""
        user_id = update.effective_user.id
        reminders = self.db.get_user_reminders(user_id)

        if not reminders:
            await update.message.reply_text("📭 Bạn chưa có lời nhắc nào.")
            return

        message = "*Lời nhắc của bạn*\n\n"
        for reminder in reminders:
            status = "✅" if reminder['active'] else "⏸️"
            translated_freq = self.freq_translation.get(reminder['frequency'], reminder['frequency'])
            day_text = f"📅 Ngày: {reminder['day']}\n" if reminder.get('day') else ""
            reminder_time = self._default_time_str()

            # Calculate time left until next reminder
            time_left_str = ""
            try:
                job_prefix = f"reminder_{reminder['id']}_dminus"
                jobs = [
                    job for job in self.scheduler.get_jobs()
                    if job.id.startswith(job_prefix) and job.next_run_time
                ]
                if jobs:
                    next_job = min(jobs, key=lambda j: j.next_run_time)
                    now = datetime.now(next_job.next_run_time.tzinfo)
                    time_diff = next_job.next_run_time - now

                    if time_diff.total_seconds() > 0:
                        days = time_diff.days
                        hours, remainder = divmod(time_diff.seconds, 3600)
                        minutes, _ = divmod(remainder, 60)

                        parts = []
                        if days > 0:
                            parts.append(f"{days} ngày")
                        if hours > 0:
                            parts.append(f"{hours} giờ")
                        if minutes > 0:
                            parts.append(f"{minutes} phút")

                        if not parts and time_diff.total_seconds() > 0:
                            parts.append("chưa đầy một phút")

                        if parts:
                            time_left_str = f"🔔 Sẽ được gửi sau {' '.join(parts)}"
            except Exception as e:
                logger.error(f"Error calculating time left for reminder {reminder['id']}: {e}")
            message += (
                f"{status} *ID: {reminder['id']}*\n"
                f"📝 {reminder['text']}\n"
                f"{day_text}"
                f"⏰ {reminder_time} ({translated_freq})\n"
                f"{time_left_str}\n"
            )

        await update.message.reply_text(message, parse_mode='Markdown')

    async def delete_reminder(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Delete a reminder"""
        user_id = update.effective_user.id

        if not context.args:
            await update.message.reply_text(
                "Vui lòng cung cấp ID lời nhắc:\n"
                "/remove <id_lời_nhắc>"
            )
            return

        reminder_id = context.args[0]

        if self.reminder_manager.delete_reminder(user_id, reminder_id):
            await update.message.reply_text(
                f"✅ Đã xóa thành công lời nhắc {reminder_id}."
            )
        else:
            await update.message.reply_text(
                f"❌ Không tìm thấy lời nhắc {reminder_id}."
            )

    async def set_timezone(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Set user timezone"""
        if not context.args:
            await update.message.reply_text(
                "Vui lòng cung cấp múi giờ:\n"
                "/set_timezone <múi_giờ>\n\n"
                "Ví dụ: /set_timezone UTC\n"
                "Hoặc: /set_timezone Asia/Ho_Chi_Minh"
            )
            return

        timezone = context.args[0]
        user_id = update.effective_user.id
        user_name = update.effective_user.first_name

        self.db.add_user(user_id, user_name)
        self.db.set_user_timezone(user_id, timezone)
        await update.message.reply_text(
            f"✅ Đã đặt múi giờ thành {timezone}"
        )

    async def send_reminder_job(self, user_id: int, reminder: Dict, offset_days: int = 0):
        """Coroutine to send a reminder, to be run as a task.
        offset_days: 0 for on-time, 1 for 1-day-before, 2 for 2-days-before
        """
        logger.info(f"Executing job for reminder {reminder['id']} for user {user_id} (offset_days={offset_days})")
        try:
            translated_freq = self.freq_translation.get(reminder['frequency'], reminder['frequency'])
            reminder_time = self._default_time_str()
            reminder_text = reminder['text']

            if offset_days == 2:
                title = "⏳ Còn 2 ngày nữa đến hạn thanh toán"
            elif offset_days == 1:
                title = "⏳ Còn 1 ngày nữa đến hạn thanh toán"
            else:
                title = "\U0001F6A8 G\u1ea4P: \u0110\u1ebeN H\u1ea0N THANH TO\u00c1N H\u00d4M NAY"

            text = (
                f"**{title}**\n\n"
                f"🧾 **Nội dung:** {reminder_text}\n"
                f"⏰ **Thời gian:** {reminder_time} - {translated_freq}\n\n"
                f"Vui lòng xử lý đúng hạn!"
            )

            await self.app.bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode='Markdown'
            )
            self.db.log_reminder_sent(reminder['id'], user_id)
            logger.info(f"Successfully sent reminder {reminder['id']} to user {user_id} (offset_days={offset_days})")

            if reminder.get('frequency') == 'once' and offset_days == 0:
                try:
                    self.db.delete_reminder(reminder['id'])
                    job_prefix = f"reminder_{reminder['id']}_"
                    for job in self.scheduler.get_jobs():
                        if job.id.startswith(job_prefix):
                            try:
                                self.scheduler.remove_job(job.id)
                            except Exception:
                                pass
                    logger.info(f"Deleted once reminder {reminder['id']} after due date")
                except Exception as e:
                    logger.error(f"Failed to delete once reminder {reminder['id']}: {e}")

            # For monthly reminders, reschedule the corresponding offset for next month
            if reminder.get('frequency') == 'monthly':
                try:
                    next_event = self._compute_next_month_event(reminder)
                    run_dt = next_event - timedelta(days=offset_days)
                    self._schedule_offset_job(reminder, run_dt, offset_days)
                    logger.info(f"Rescheduled monthly reminder {reminder['id']} (offset {offset_days}) for {run_dt}")
                except Exception as e:
                    logger.error(f"Failed to reschedule monthly reminder {reminder['id']} (offset {offset_days}): {e}")
        except TelegramError as e:
            logger.error(f"Telegram API error sending reminder {reminder['id']} to user {user_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error sending reminder {reminder['id']} to user {user_id}: {e}", exc_info=True)

    def send_reminder(self, user_id: int, reminder: Dict):
        """Create an asyncio task to send the reminder"""
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(self.send_reminder_job(user_id, reminder))
            logger.info(f"Created task to send reminder {reminder['id']} to user {user_id}")
        except RuntimeError:
            # No running loop, try to get the event loop
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self.send_reminder_job(user_id, reminder))
                    logger.info(f"Created task to send reminder {reminder['id']} to user {user_id}")
                else:
                    logger.error(f"Event loop is not running for reminder {reminder['id']}")
            except Exception as e:
                logger.error(f"Failed to create task for reminder {reminder['id']}: {e}")

    def schedule_reminders(self):
        """Schedule all active reminders"""
        reminders = self.db.get_all_active_reminders()
        logger.info(f"Found {len(reminders)} active reminders to schedule.")

        for reminder in reminders:
            self._schedule_single_reminder(reminder)
        self.schedule_daily_housekeeping_jobs()

    def schedule_daily_housekeeping_jobs(self):
        """Schedule daily status and pre-sleep notices."""
        tz = self.vn_tz
        hour, minute = self.notify_hm
        self.scheduler.add_job(
            self._daily_no_reminders_job,
            trigger=CronTrigger(hour=hour, minute=minute, timezone=tz),
            id="daily_no_reminders",
            replace_existing=True,
            misfire_grace_time=300
        )
        if getattr(self, 'debug_always_on', False):
            logger.info("Debug mode: skipping pre-sleep notice scheduling")
            return
        pre_h, pre_m = self._pre_sleep_hm()
        self.scheduler.add_job(
            self._pre_sleep_notice_job,
            trigger=CronTrigger(hour=pre_h, minute=pre_m, timezone=tz),
            id="pre_sleep_notice",
            replace_existing=True,
            misfire_grace_time=300
        )

    def _schedule_single_reminder(self, reminder: Dict):
        """Schedule a single reminder with 3 notifications: 2 days before, 1 day before, on-time"""
        job_id = f"reminder_{reminder['id']}"
        user_timezone = reminder.get('timezone', 'Asia/Ho_Chi_Minh')

        # Remove existing jobs if present
        for offset in [0, 1, 2]:
            try:
                self.scheduler.remove_job(f"{job_id}_dminus{offset}")
            except:
                pass

        hour, minute = self.notify_hm
        try:
            tz = pytz.timezone(user_timezone)
        except pytz.UnknownTimeZoneError:
            logger.warning(f"Unknown timezone {user_timezone}, defaulting to Asia/Ho_Chi_Minh for job {job_id}")
            tz = pytz.timezone('Asia/Ho_Chi_Minh')

        trigger_args = {
            'hour': hour,
            'minute': minute,
            'timezone': tz
        }

        if reminder['frequency'] == 'once':
            try:
                now = datetime.now(tz)
                target_day = reminder.get('day')

                if target_day is None:
                    target_day = now.day

                # Try to schedule for the target day in the current month
                run_date = now.replace(day=target_day, hour=hour, minute=minute, second=0, microsecond=0)

                # If the date/time has already passed, schedule for the next month
                if run_date <= now:
                    logger.info(f"Target time {run_date} has passed, moving to next month")
                    if now.month == 12:
                        run_date = run_date.replace(year=now.year + 1, month=1)
                    else:
                        run_date = run_date.replace(month=now.month + 1)

                    try:
                        run_date = run_date.replace(day=target_day)
                    except ValueError:
                        import calendar
                        last_day = calendar.monthrange(run_date.year, run_date.month)[1]
                        run_date = run_date.replace(day=last_day)

                # Schedule on-time notification
                logger.info(f"Scheduling 'once' reminder {job_id} for {run_date} (now: {now})")
                trigger = DateTrigger(run_date=run_date, timezone=user_timezone)
                self.scheduler.add_job(
                    self.send_reminder_job,
                    trigger=trigger,
                    args=(reminder['user_id'], reminder, 0),
                    id=f"{job_id}_dminus0",
                    replace_existing=True,
                    misfire_grace_time=300
                )

                # Schedule 1-day-before notification
                run_date_1d = run_date - timedelta(days=1)
                trigger_1d = DateTrigger(run_date=run_date_1d, timezone=user_timezone)
                self.scheduler.add_job(
                    self.send_reminder_job,
                    trigger=trigger_1d,
                    args=(reminder['user_id'], reminder, 1),
                    id=f"{job_id}_dminus1",
                    replace_existing=True,
                    misfire_grace_time=300
                )

                # Schedule 2-days-before notification
                run_date_2d = run_date - timedelta(days=2)
                trigger_2d = DateTrigger(run_date=run_date_2d, timezone=user_timezone)
                self.scheduler.add_job(
                    self.send_reminder_job,
                    trigger=trigger_2d,
                    args=(reminder['user_id'], reminder, 2),
                    id=f"{job_id}_dminus2",
                    replace_existing=True,
                    misfire_grace_time=300
                )

            except (pytz.UnknownTimeZoneError, ValueError) as e:
                logger.error(f"Error creating DateTrigger for job {job_id}: {e}")
                return

        elif reminder['frequency'] == 'daily':
            trigger = CronTrigger(**trigger_args)
            self.scheduler.add_job(
                self.send_reminder_job,
                trigger=trigger,
                args=(reminder['user_id'], reminder, 0),
                id=f"{job_id}_dminus0",
                replace_existing=True,
                misfire_grace_time=300
            )

        elif reminder['frequency'] == 'weekly':
            trigger_args['day_of_week'] = '0'  # Default to Monday
            trigger = CronTrigger(**trigger_args)
            self.scheduler.add_job(
                self.send_reminder_job,
                trigger=trigger,
                args=(reminder['user_id'], reminder, 0),
                id=f"{job_id}_dminus0",
                replace_existing=True,
                misfire_grace_time=300
            )

        elif reminder['frequency'] == 'monthly':
            trigger_args['day'] = reminder.get('day', '*')
            trigger = CronTrigger(**trigger_args)

            # On-time monthly cron job
            self.scheduler.add_job(
                self.send_reminder_job,
                trigger=trigger,
                args=(reminder['user_id'], reminder, 0),
                id=f"{job_id}_dminus0",
                replace_existing=True,
                misfire_grace_time=300
            )

            # Additionally schedule the 1-day and 2-days before notifications for the next occurrence
            try:
                next_event = self._compute_next_month_event(reminder)
                # 1 day before
                self._schedule_offset_job(reminder, next_event - timedelta(days=1), 1)
                # 2 days before
                self._schedule_offset_job(reminder, next_event - timedelta(days=2), 2)
                logger.info(f"Scheduled monthly offsets for {job_id} at {next_event - timedelta(days=1)} and {next_event - timedelta(days=2)}")
            except Exception as e:
                logger.error(f"Failed to schedule monthly offset jobs for {job_id}: {e}")

        else:
            logger.warning(f"Unknown frequency: {reminder['frequency']}")
            return

        logger.info(f"Scheduled job {job_id} for user {reminder['user_id']}")

    def schedule_active_window_jobs(self):
        """Create scheduler jobs to start/stop polling daily in VN time."""
        if getattr(self, 'debug_always_on', False):
            logger.info("Debug mode: skipping active window scheduling")
            return
        if getattr(self, 'window_only', False):
            logger.info("Window-only mode: skipping polling window scheduling")
            return
        tz = self.vn_tz
        # Wake at active_start_hm
        self.scheduler.add_job(
            self._wake_job,
            trigger=CronTrigger(hour=self.active_start_hm[0], minute=self.active_start_hm[1], timezone=tz),
            id="wake_polling",
            replace_existing=True
        )
        # Sleep at active_end_hm
        self.scheduler.add_job(
            self._sleep_job,
            trigger=CronTrigger(hour=self.active_end_hm[0], minute=self.active_end_hm[1], timezone=tz),
            id="sleep_polling",
            replace_existing=True
        )
        # Peeker: check updates periodically when outside active hours
        from apscheduler.triggers.interval import IntervalTrigger
        self.scheduler.add_job(
            self._peek_updates_job,
            trigger=IntervalTrigger(seconds=15, timezone=tz),
            id="peek_updates",
            replace_existing=True
        )
        logger.info("Scheduled daily wake/sleep jobs and peeker for polling window")

    async def _wake_job(self):
        try:
            await self._start_polling_if_needed('schedule')
        except Exception as e:
            logger.error(f"Failed to start polling in wake job: {e}")

    async def _sleep_job(self):
        try:
            await self._stop_polling_if_needed('schedule')
        except Exception as e:
            logger.error(f"Failed to stop polling in sleep job: {e}")

    async def _peek_updates_job(self):
        """Lightweight checker that wakes the bot by message outside active hours.
        Runs periodically via scheduler; does not consume updates (no offset).
        """
        try:
            if self._is_active_hours():
                return
            should_wake = False
            async with self._peek_lock:
                if self.app.updater.running:
                    return

                peek_args = {'limit': 1, 'timeout': 0}
                if self._peek_offset:
                    peek_args['offset'] = self._peek_offset + 1

                updates = await self.app.bot.get_updates(**peek_args)

                if updates:
                    last_id = updates[-1].update_id
                    self._peek_offset = last_id
                    now_vn = datetime.now(self.vn_tz).strftime("%Y-%m-%d %H:%M:%S %z")
                    logger.info(f"peek detected update_id={last_id} at {now_vn} (VN)")
                    should_wake = True
            if should_wake:
                await self._start_polling_if_needed('message')
                # Register activity to arm the 5-minute timer
                self._register_activity()
        except TelegramError as e:
            logger.warning(f"peek_updates_job telegram error: {e}")
        except Exception as e:
            logger.error(f"peek_updates_job error: {e}")

    async def _daily_no_reminders_job(self):
        """Send a daily status when there are no reminders due today."""
        try:
            reminders = self.db.get_all_active_reminders()
        except Exception as e:
            logger.error(f"Failed to load reminders for daily status: {e}")
            reminders = []

        reminders_by_user: Dict[int, List[Dict]] = {}
        for reminder in reminders:
            user_id = self._parse_user_id(reminder.get('user_id'))
            if user_id is None:
                continue
            reminders_by_user.setdefault(user_id, []).append(reminder)

        for user_id in self._get_all_user_ids(reminders):
            user_reminders = reminders_by_user.get(user_id, [])
            has_due_today = any(self._reminder_due_today(r) for r in user_reminders)
            if has_due_today:
                continue
            try:
                await self.app.bot.send_message(
                    chat_id=user_id,
                    text="Khỏe quá không cần thanh toán gì cho hôm nay"
                )
            except TelegramError as e:
                logger.warning(f"Failed to send daily status to {user_id}: {e}")
            except Exception as e:
                logger.error(f"Unexpected error sending daily status to {user_id}: {e}")

    async def _pre_sleep_notice_job(self):
        """Warn before the bot goes to sleep."""
        try:
            for user_id in self._get_all_user_ids():
                try:
                    await self.app.bot.send_message(
                        chat_id=user_id,
                        text="Còn 1 phút nữa Bot sẽ ngưng hoạt động"
                    )
                except TelegramError as e:
                    logger.warning(f"Failed to send pre-sleep notice to {user_id}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in pre-sleep notice: {e}")


    async def error_handler(self, update: object, context: ContextTypes.DEFAULT_TYPE):
        """Log errors"""
        logger.error(msg="Exception while handling an update:", exc_info=context.error)

    def setup_handlers(self):
        """Setup all command and message handlers"""
        # Command handlers
        self.app.add_handler(CommandHandler("start", self.start))
        self.app.add_handler(CommandHandler("help", self.help_command))
        self.app.add_handler(CommandHandler("menu", self.menu_command))
        self.app.add_handler(CommandHandler("list", self.list_reminders))
        self.app.add_handler(CommandHandler("remove", self.delete_reminder))
        self.app.add_handler(CommandHandler("set_timezone", self.set_timezone))

        # New reminder conversation
        new_reminder_handler = ConversationHandler(
            entry_points=[CommandHandler("add", self.new_reminder)],
            states={
                REMINDER_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.reminder_text_handler)],
                REMINDER_DAY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, self.reminder_day_handler),
                    CallbackQueryHandler(self.reminder_day_handler)
                ],
                REMINDER_FREQUENCY: [CallbackQueryHandler(self.frequency_handler)],
                REMINDER_CONFIRM: [CallbackQueryHandler(self.confirm_handler)],
            },
            fallbacks=[CommandHandler("cancel", self.cancel)],
        )
        self.app.add_handler(new_reminder_handler)

        # Global activity hook (captures all updates)
        self.app.add_handler(TypeHandler(Update, self._activity_hook), group=-1)

        # Error handler
        self.app.add_error_handler(self.error_handler)

    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Cancel conversation"""
        await update.message.reply_text("❌ Thao tác đã bị hủy.")
        return ConversationHandler.END

    async def run(self):
        """Run the bot and scheduler"""
        # Use custom JobQueue to avoid weakref error on Python 3.11+
        self.app = Application.builder().token(self.token).job_queue(NoWeakrefJobQueue()).build()
        self.setup_handlers()

        # Start scheduler before initializing the app
        self.schedule_reminders()
        self.schedule_active_window_jobs()  # Schedule wake/sleep jobs
        self.scheduler.start()
        logger.info("Scheduler started and reminders scheduled")

        async with self.app:
            await self.app.initialize()
            await self.app.start()
            logger.info("Bot started successfully")

            # Start polling only if within active hours
            if self._is_active_hours():
                await self.app.updater.start_polling(allowed_updates=Update.ALL_TYPES)
                logger.info("Polling started (within active hours)")
            else:
                start_str = f"{self.active_start_hm[0]:02d}:{self.active_start_hm[1]:02d}"
                logger.info(f"Outside active hours, polling will start at {start_str} Vietnam time")
                if getattr(self, 'window_only', False):
                    logger.info("Window-only mode: outside active hours, exiting")
                    return

            if getattr(self, 'window_only', False):
                delay = self._seconds_until_active_end()
                logger.info(f"Window-only mode: exiting after {int(delay)} seconds")
                await asyncio.sleep(delay)
                if self.app.updater.running:
                    await self.app.updater.stop()
                self.scheduler.shutdown(wait=False)
                logger.info("Window-only mode: exited after active window")
                return

            # Keep the bot running until it's stopped
            while True:
                await asyncio.sleep(1)

if __name__ == '__main__':
    bot = ReminderBot()
    asyncio.run(bot.run())

