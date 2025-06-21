# cron_email_sender.py
# This script runs independently and handles email sending
# Schedule this script with cron job instead of calling Streamlit URL

from datetime import datetime
import smtplib
import ssl
from email.message import EmailMessage
import os
from pyairtable import Table
from dotenv import load_dotenv
import pytz
import sys
import time

load_dotenv()

# -------- TIMEZONE SETUP -------- #
IST = pytz.timezone('Asia/Kolkata')

# -------- CONFIG -------- #
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
AIRTABLE_PERSONAL_ACCESS_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

def get_ist_now():
    """Get current time in IST"""
    return datetime.now(IST)

def convert_to_ist(dt):
    """Convert datetime to IST timezone"""
    if dt.tzinfo is None:
        return IST.localize(dt)
    else:
        return dt.astimezone(IST)

def send_email(subject, body, to):
    """Send email via Gmail SMTP"""
    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = to

        context = ssl._create_unverified_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Error sending email to {to}: {e}")
        return False

def check_and_send_due_reminders():
    """Check for due reminders and send them"""
    
    # Debug: Check what environment variables are available
    print("🔍 DEBUG: Checking environment variables...")
    print(f"EMAIL_ADDRESS: {'✅ Set' if EMAIL_ADDRESS else '❌ Missing'}")
    print(f"EMAIL_PASSWORD: {'✅ Set' if EMAIL_PASSWORD else '❌ Missing'}")
    print(f"AIRTABLE_TOKEN: {'✅ Set' if AIRTABLE_PERSONAL_ACCESS_TOKEN else '❌ Missing'}")
    print(f"AIRTABLE_BASE_ID: {'✅ Set' if AIRTABLE_BASE_ID else '❌ Missing'}")
    print(f"AIRTABLE_TABLE_NAME: {'✅ Set' if AIRTABLE_TABLE_NAME else '❌ Missing'}")
    
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("❌ ERROR: Missing required environment variables")
        return 0, 1
    
    # Rest of your existing code...
    
    """Check for due reminders and send them"""
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("ERROR: Missing required environment variables")
        return 0, 1
    
    try:
        table = Table(AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        current_time_ist = get_ist_now()
        print(f"[{current_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}] Checking for due reminders...")
        
        records = table.all()
        sent_count = 0
        error_count = 0
        
        for record in records:
            fields = record.get('fields', {})
            status = fields.get('Status', '')
            
            # Only process pending reminders
            if status != 'Pending':
                continue
                
            reminder_time_str = fields.get('ReminderTime', '')
            if not reminder_time_str:
                continue
                
            try:
                # Parse the stored time and ensure it's in IST
                reminder_time = datetime.fromisoformat(reminder_time_str.replace('Z', '+00:00'))
                reminder_time_ist = convert_to_ist(reminder_time)
                
                # Check if reminder is due
                if current_time_ist >= reminder_time_ist:
                    email = fields.get('Email', '')
                    subject = fields.get('Subject', '')
                    message = fields.get('Message', '')
                    reminder_id = fields.get('ReminderID', '')
                    
                    print(f"Sending due reminder to {email} (ID: {reminder_id}) - Due: {reminder_time_ist.strftime('%Y-%m-%d %H:%M IST')}")
                    
                    # Send the email
                    if send_email(subject, message, email):
                        # Update status to Sent
                        table.update(record['id'], {"Status": "Sent"})
                        print(f"✅ Successfully sent reminder to {email}")
                        sent_count += 1
                    else:
                        # Update status to Error
                        table.update(record['id'], {"Status": "Error"})
                        print(f"❌ Failed to send reminder to {email}")
                        error_count += 1
                        
            except Exception as e:
                print(f"Error processing reminder {fields.get('ReminderID', 'unknown')}: {e}")
                table.update(record['id'], {"Status": "Error"})
                error_count += 1
                
        print(f"Completed: {sent_count} sent, {error_count} errors")
        return sent_count, error_count
        
    except Exception as e:
        print(f"Error in check_and_send_due_reminders: {e}")
        return 0, 1

if __name__ == "__main__":
    print("Starting email reminder cron job...")
    sent, errors = check_and_send_due_reminders()
    
    # Exit with appropriate code
    if errors > 0:
        sys.exit(1)  # Error exit code
    else:
        sys.exit(0)  # Success exit codes
