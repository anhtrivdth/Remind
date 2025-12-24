"""
Reminder Manager - Handles reminder creation, editing, and deletion logic
"""

from typing import Dict, Optional
from database import Database

class ReminderManager:
    def __init__(self, db: Database):
        self.db = db
    
    def create_reminder(self, user_id: int, reminder_data: Dict) -> str:
        """
        Create a new reminder
        
        Args:
            user_id: Telegram user ID
            reminder_data: Dict with keys: text, time, frequency, timezone
        
        Returns:
            reminder_id: The created reminder's ID
        """
        return self.db.create_reminder(user_id, reminder_data)
    
    def edit_reminder(self, user_id: int, reminder_id: str, updates: Dict) -> bool:
        """
        Edit an existing reminder
        
        Args:
            user_id: Telegram user ID
            reminder_id: ID of reminder to edit
            updates: Dict with fields to update
        
        Returns:
            bool: Success status
        """
        reminder = self.db.get_reminder(reminder_id)
        
        if not reminder or reminder['user_id'] != user_id:
            return False
        
        # Validate updates
        if 'time' in updates:
            if not self._validate_time(updates['time']):
                return False
        
        if 'frequency' in updates:
            if updates['frequency'] not in ['once', 'daily', 'weekly']:
                return False
        
        self.db.update_reminder(reminder_id, updates)
        return True
    
    def delete_reminder(self, user_id: int, reminder_id: str) -> bool:
        """
        Delete a reminder
        
        Args:
            user_id: Telegram user ID
            reminder_id: ID of reminder to delete
        
        Returns:
            bool: Success status
        """
        reminder = self.db.get_reminder(reminder_id)
        
        if not reminder or reminder['user_id'] != user_id:
            return False
        
        return self.db.delete_reminder(reminder_id)
    
    def toggle_reminder(self, user_id: int, reminder_id: str) -> bool:
        """
        Toggle reminder active status
        
        Args:
            user_id: Telegram user ID
            reminder_id: ID of reminder to toggle
        
        Returns:
            bool: Success status
        """
        reminder = self.db.get_reminder(reminder_id)
        
        if not reminder or reminder['user_id'] != user_id:
            return False
        
        new_status = not reminder['active']
        return self.db.update_reminder(reminder_id, {'active': new_status})
    
    def get_user_reminders(self, user_id: int):
        """Get all reminders for a user"""
        return self.db.get_user_reminders(user_id)
    
    def get_reminder(self, reminder_id: str) -> Optional[Dict]:
        """Get a specific reminder"""
        return self.db.get_reminder(reminder_id)
    
    def _validate_time(self, time_str: str) -> bool:
        """Validate time format HH:MM"""
        try:
            parts = time_str.split(':')
            if len(parts) != 2:
                return False
            
            hour = int(parts[0])
            minute = int(parts[1])
            
            return 0 <= hour <= 23 and 0 <= minute <= 59
        except (ValueError, AttributeError):
            return False
    
    def get_reminder_history(self, reminder_id: str):
        """Get history of reminder sends"""
        return self.db.get_reminder_history(reminder_id)

