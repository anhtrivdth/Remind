"""
Database module using Google Sheets as a persistent storage backend.
Replaces the original JSON file-based storage to prevent data loss on ephemeral filesystems.
"""

import gspread
import os
import json
from google.oauth2.service_account import Credentials
from datetime import datetime
from typing import Dict, List, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, credentials_file: str = 'google_credentials.json', spreadsheet_name: str = 'reminders'):
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # Check for Heroku environment variable first
            google_creds_json = os.getenv('GOOGLE_CREDENTIALS_JSON')
            if google_creds_json:
                creds_dict = json.loads(google_creds_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
                logger.info("Loaded Google credentials from environment variable.")
            else:
                creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
                logger.info(f"Loaded Google credentials from file: {credentials_file}")
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open(spreadsheet_name)
            self.reminders_sheet = self.spreadsheet.worksheet('Reminders')
            self.users_sheet = self.spreadsheet.worksheet('Users')
            self.logs_sheet = self.spreadsheet.worksheet('Logs')
            logger.info("Successfully connected to Google Sheets.")
        except gspread.exceptions.SpreadsheetNotFound:
            logger.error(f"Spreadsheet '{spreadsheet_name}' not found. Please create it manually and share it with the service account email.")
            raise
        except gspread.exceptions.WorksheetNotFound as e:
            logger.warning(f"Worksheet not found: {e}. This might be ok if we are creating it.")
            raise
        except Exception as e:
            logger.error(f"An error occurred while connecting to Google Sheets: {e}")
            raise

    def _get_all_reminders(self) -> List[Dict]:
        """Helper to get all reminder records and cache them briefly."""
        # Basic caching can be added here if needed to reduce API calls
        return self.reminders_sheet.get_all_records()

    def _get_all_users(self) -> List[Dict]:
        """Helper to get all user records."""
        return self.users_sheet.get_all_records()

    def _normalize_user_id(self, value) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _parse_reminder_id(self, value) -> Optional[int]:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            if isinstance(value, float) and not value.is_integer():
                return None
            return int(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            if value.isdigit():
                return int(value)
            try:
                as_float = float(value)
                if as_float.is_integer():
                    return int(as_float)
            except ValueError:
                return None
        return None

    def _get_next_reminder_id(self) -> str:
        reminders = self._get_all_reminders()
        max_id = 0
        for reminder in reminders:
            parsed = self._parse_reminder_id(reminder.get('id'))
            if parsed and parsed > max_id:
                max_id = parsed
        return str(max_id + 1)
    
    def add_user(self, user_id: int, first_name: str):
        """Add a new user if they don't exist."""
        users = self._get_all_users()
        user_ids = {self._normalize_user_id(user.get('id')) for user in users}

        if user_id not in user_ids:
            new_user_row = [
                user_id,
                first_name,
                datetime.now().isoformat(),
                'Asia/Ho_Chi_Minh'  # Default timezone
            ]
            self.users_sheet.append_row(new_user_row)
            logger.info(f"Added new user: {user_id}")

    def get_user_timezone(self, user_id: int) -> Optional[str]:
        """Get a user's timezone."""
        try:
            user_cell = self.users_sheet.find(str(user_id), in_column=1)
            if user_cell:
                user_row = self.users_sheet.row_values(user_cell.row)
                # Assuming timezone is in the 4th column (index 3)
                return user_row[3]
        except gspread.exceptions.CellNotFound:
            logger.warning(f"User {user_id} not found when getting timezone.")
        return 'UTC' # Default fallback

    def set_user_timezone(self, user_id: int, timezone: str):
        """Set a user's timezone."""
        try:
            user_cell = self.users_sheet.find(str(user_id), in_column=1)
            if user_cell:
                # Assuming timezone is in the 4th column
                self.users_sheet.update_cell(user_cell.row, 4, timezone)
                logger.info(f"Updated timezone for user {user_id} to {timezone}")
        except gspread.exceptions.CellNotFound:
            logger.error(f"User {user_id} not found when setting timezone.")

    def create_reminder(self, user_id: int, reminder_data: Dict) -> str:
        """Create a new reminder and add it to the sheet."""
        reminder_id = self._get_next_reminder_id()
        new_reminder_row = [
            reminder_id,
            user_id,
            reminder_data['text'],
            reminder_data.get('day'),
            reminder_data['time'],
            reminder_data['frequency'],
            reminder_data.get('timezone', 'UTC'),
            True,  # active
            datetime.now().isoformat(),
            None  # last_sent
        ]
        self.reminders_sheet.append_row(new_reminder_row)
        logger.info(f"Created new reminder {reminder_id} for user {user_id}")
        return reminder_id

    def get_user_reminders(self, user_id: int) -> List[Dict]:
        """Get all reminders for a specific user."""
        all_reminders = self._get_all_reminders()
        return [
            r for r in all_reminders
            if self._normalize_user_id(r.get('user_id')) == user_id
        ]

    def get_all_users(self) -> List[Dict]:
        """Get all users from the sheet."""
        return self._get_all_users()

    def get_all_active_reminders(self) -> List[Dict]:
        """Get all active reminders from the sheet."""
        all_reminders = self._get_all_reminders()
        # gspread returns checkbox values as 'TRUE'/'FALSE' strings
        return [r for r in all_reminders if str(r.get('active')).upper() == 'TRUE']

    def get_reminder(self, reminder_id: str) -> Optional[Dict]:
        """Get a specific reminder by its ID."""
        reminder_id = str(reminder_id).strip()
        all_reminders = self._get_all_reminders()
        for r in all_reminders:
            if str(r.get('id')).strip() == reminder_id:
                return r
        return None

    def update_reminder(self, reminder_id: str, updates: Dict) -> bool:
        """Update a reminder's details in the sheet."""
        try:
            reminder_id = str(reminder_id).strip()
            reminder_cell = self.reminders_sheet.find(reminder_id, in_column=1)
            if not reminder_cell:
                return False

            headers = self.reminders_sheet.row_values(1)
            row_index = reminder_cell.row

            for key, value in updates.items():
                if key in headers:
                    col_index = headers.index(key) + 1
                    self.reminders_sheet.update_cell(row_index, col_index, str(value))

            logger.info(f"Updated reminder {reminder_id} with: {updates}")
            return True
        except gspread.exceptions.CellNotFound:
            logger.warning(f"Reminder {reminder_id} not found for update.")
            return False

    def delete_reminder(self, reminder_id: str) -> bool:
        """Delete a reminder from the sheet."""
        try:
            reminder_id = str(reminder_id).strip()
            reminder_cell = self.reminders_sheet.find(reminder_id, in_column=1)
            if reminder_cell:
                self.reminders_sheet.delete_rows(reminder_cell.row)
                logger.info(f"Deleted reminder {reminder_id}")
                return True
        except gspread.exceptions.CellNotFound:
            logger.warning(f"Reminder {reminder_id} not found for deletion.")
        return False

    def log_reminder_sent(self, reminder_id: str, user_id: int):
        """Log a reminder send event and update its last_sent time."""
        # Log the event
        log_row = [reminder_id, datetime.now().isoformat(), user_id]
        self.logs_sheet.append_row(log_row)

        # Update the last_sent field for the reminder
        self.update_reminder(reminder_id, {'last_sent': datetime.now().isoformat()})
        logger.info(f"Logged sent event for reminder {reminder_id}")

    def get_reminder_history(self, reminder_id: str) -> List[Dict]:
        """Get the send history for a specific reminder."""
        all_logs = self.logs_sheet.get_all_records()
        reminder_id = str(reminder_id).strip()
        return [
            log for log in all_logs
            if str(log.get('reminder_id')).strip() == reminder_id
        ]
