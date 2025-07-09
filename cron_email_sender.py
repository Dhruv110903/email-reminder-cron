# import os
# import sys
# import time
# import base64
# import re
# from datetime import datetime
# from dotenv import load_dotenv
# from pyairtable import Api
# import pytz

# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from bs4 import BeautifulSoup

# load_dotenv()

# # ----------- CONFIG ----------- #
# EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
# EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
# AIRTABLE_PERSONAL_ACCESS_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
# AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
# AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
# SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# IST = pytz.timezone('Asia/Kolkata')

# # ----------- UTILS ----------- #
# def get_ist_now():
#     return datetime.now(IST)

# def parse_bill_date(date_str):
#     if not date_str:
#         return None
#     try:
#         return datetime.fromisoformat(date_str)
#     except ValueError:
#         pass
#     try:
#         return datetime.strptime(date_str, '%d-%b-%y')
#     except ValueError:
#         raise ValueError(f"Unknown BILL Date format: {date_str}")

# def send_email(subject, body, to):
#     import smtplib
#     import ssl
#     from email.message import EmailMessage

#     try:
#         msg = EmailMessage()
#         msg.set_content(body)
#         msg['Subject'] = subject
#         msg['From'] = EMAIL_ADDRESS
#         msg['To'] = to

#         context = ssl._create_unverified_context()
#         with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
#             smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
#             smtp.send_message(msg)
#         return True
#     except Exception as e:
#         print(f"❌ Email error to {to}: {e}")
#         return False

# # ----------- REMINDER LOGIC ----------- #
# def check_and_send_due_reminders():
#     print(f"[{get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}] Checking for due reminders...")
#     if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
#         print("❌ ERROR: Missing required environment variables")
#         return 0, 1

#     try:
#         table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
#         current_time = get_ist_now()

#         records = table.all()
#         sent = 0
#         errors = 0

#         for record in records:
#             f = record.get('fields', {})
#             if f.get("Reminder Sent", False):  # ✅ Skip already sent
#                 continue

#             bill_date_str = f.get('BILL Date 1', '')
#             email = f.get('Email ID', '')
#             isin = f.get('ISIN', '')
#             bill_amount = f.get('Bill Amount', '')

#             if not bill_date_str or not email:
#                 continue

#             try:
#                 bill_date = parse_bill_date(bill_date_str)
#                 if bill_date.tzinfo is None:
#                     bill_date = pytz.utc.localize(bill_date)
#                 bill_date_ist = bill_date.astimezone(IST)

#                 if current_time >= bill_date_ist:
#                     subject = f"Reminder: BILL Due for ISIN - {isin}"
#                     message = (
#                         f"Dear user,\n\n"
#                         f"This is a reminder that the BILL dated {bill_date_ist.strftime('%Y-%m-%d')} "
#                         f"for ISIN {isin} with amount {bill_amount} is due.\n\n"
#                         f"Please take the necessary action.\n\n"
#                         f"Regards,\nReminder System"
#                     )
#                     if send_email(subject, message, email):
#                         print(f"✅ Sent reminder to {email} for ISIN {isin}")
#                         table.update(record['id'], {"Reminder Sent": True})
#                         sent += 1
#                     else:
#                         errors += 1
#             except Exception as e:
#                 print(f"❌ Reminder error for ISIN {isin}: {e}")
#                 errors += 1

#         print(f"✅ Completed: {sent} sent, {errors} errors")
#         return sent, errors

#     except Exception as e:
#         print(f"❌ Error in check_and_send_due_reminders: {e}")
#         return 0, 1

# # ----------- GMAIL ISIN EXTRACTION ----------- #
# def authenticate_gmail():
#     creds = None
#     if os.path.exists('token.json'):
#         creds = Credentials.from_authorized_user_file('token.json', SCOPES)
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
#         creds = flow.run_local_server(port=8080)
#         with open('token.json', 'w') as token:
#             token.write(creds.to_json())
#     return build('gmail', 'v1', credentials=creds)

# def get_email_body(payload):
#     plain, html = None, None
#     if 'parts' in payload:
#         for part in payload['parts']:
#             p_txt, h_txt = get_email_body(part)
#             plain = plain or p_txt
#             html = html or h_txt
#     else:
#         mime_type = payload.get('mimeType')
#         data = payload.get('body', {}).get('data')
#         if data:
#             decoded = base64.urlsafe_b64decode(data).decode(errors='replace')
#             if mime_type == 'text/plain':
#                 plain = decoded
#             elif mime_type == 'text/html':
#                 html = decoded
#     return plain, html

# def extract_isin_details_from_text(text):
#     results = []
#     for line in text.splitlines():
#         line = line.strip()
#         match = re.search(r'(INE[A-Z0-9]{9})', line)
#         if match:
#             isin = match.group(1)
#             clean_line = re.sub(r'[^\w\s\-]', '', line)  # Remove special chars
#             parts = clean_line.split()
#             isin_index = next((i for i, part in enumerate(parts) if isin in part), -1)
#             if isin_index != -1:
#                 company = " ".join(parts[:isin_index])
#                 instrument = " ".join(parts[isin_index + 1:])
#                 results.append({
#                     "Company": company.strip(),
#                     "ISIN": isin.strip(),
#                     "Instrument": instrument.strip()
#                 })
#             else:
#                 print(f"⚠️ ISIN {isin} found but not in parts: {parts}")
#     return results

# def isin_record_exists(table, isin):
#     records = table.all()
#     for r in records:
#         f = r.get("fields", {})
#         if f.get("ISIN", "").strip().lower() == isin.strip().lower():
#             return True
#     return False

# def fetch_and_append_new_isin_records():
#     try:
#         service = authenticate_gmail()
#         table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

#         from_time = int(time.time()) - 7 * 24 * 60 * 60
#         query = f'subject:"ISIN Activated" after:{from_time}'
#         results = service.users().messages().list(userId='me', q=query, maxResults=20).execute()
#         messages = results.get('messages', [])
#         new_records = 0

#         for msg in messages:
#             msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
#             payload = msg_data['payload']
#             plain, html = get_email_body(payload)
#             text = plain or BeautifulSoup(html or "", "html.parser").get_text(separator="\n")

#             for entry in extract_isin_details_from_text(text):
#                 isin = entry["ISIN"]
#                 company = entry["Company"]
#                 instrument = entry["Instrument"]

#                 if not isin_record_exists(table, isin):
#                     record = {
#                         "ISIN": isin,
#                         "Security Type": instrument,
#                         "Company Name": company,
#                         "ISIN Allotment Date": datetime.now().strftime("%Y-%m-%d")
#                     }
#                     table.create(record)
#                     print(f"➕ Added ISIN: {isin} | {company} | {instrument}")
#                     new_records += 1
#         print(f"📬 Total new ISINs added: {new_records}")
#         return new_records

#     except Exception as e:
#         print(f"❌ Error in fetch_and_append_new_isin_records: {e}")
#         return 0

# # ----------- MAIN ----------- #
# if __name__ == "__main__":
#     print("🚀 Starting cron job...")
#     sent, errors = check_and_send_due_reminders()
#     new_isins = fetch_and_append_new_isin_records()
#     print(f"📬 Summary: Sent reminders: {sent}, Errors: {errors}, New ISINs added: {new_isins}")
#     sys.exit(1 if errors else 0)


import os
import sys
import time
import base64
import re
from datetime import datetime
from dotenv import load_dotenv
from pyairtable import Api
import pytz

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from bs4 import BeautifulSoup

load_dotenv()

# ----------- CONFIG ----------- #
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
AIRTABLE_PERSONAL_ACCESS_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

IST = pytz.timezone('Asia/Kolkata')

# ----------- UTILS ----------- #
def get_ist_now():
    return datetime.now(IST)

def parse_bill_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass
    try:
        return datetime.strptime(date_str, '%d-%b-%y')
    except ValueError:
        raise ValueError(f"Unknown BILL Date format: {date_str}")

def send_email(subject, body, to):
    import smtplib
    import ssl
    from email.message import EmailMessage

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
        print(f"❌ Email error to {to}: {e}")
        return False

# ----------- REMINDER LOGIC ----------- #
def check_and_send_due_reminders():
    print(f"[{get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}] Checking for due reminders...")
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("❌ ERROR: Missing required environment variables")
        return 0, 1

    try:
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        current_time = get_ist_now()

        records = table.all()
        sent = 0
        errors = 0

        for record in records:
            f = record.get('fields', {})
            if f.get("Reminder Sent", False):  # ✅ Skip already sent
                continue

            bill_date_str = f.get('BILL Date 1', '')
            email = f.get('Email ID', '')
            isin = f.get('ISIN', '')
            bill_amount = f.get('Bill Amount', '')

            if not bill_date_str or not email:
                continue

            try:
                bill_date = parse_bill_date(bill_date_str)
                if bill_date.tzinfo is None:
                    bill_date = pytz.utc.localize(bill_date)
                bill_date_ist = bill_date.astimezone(IST)

                if current_time >= bill_date_ist:
                    subject = f"Reminder: BILL Due for ISIN - {isin}"
                    message = (
                        f"Dear user,\n\n"
                        f"This is a reminder that the BILL dated {bill_date_ist.strftime('%Y-%m-%d')} "
                        f"for ISIN {isin} with amount {bill_amount} is due.\n\n"
                        f"Please take the necessary action.\n\n"
                        f"Regards,\nReminder System"
                    )
                    if send_email(subject, message, email):
                        print(f"✅ Sent reminder to {email} for ISIN {isin}")
                        table.update(record['id'], {"Reminder Sent": True})
                        sent += 1
                    else:
                        errors += 1
            except Exception as e:
                print(f"❌ Reminder error for ISIN {isin}: {e}")
                errors += 1

        print(f"✅ Completed: {sent} sent, {errors} errors")
        return sent, errors

    except Exception as e:
        print(f"❌ Error in check_and_send_due_reminders: {e}")
        return 0, 1

# ----------- GMAIL ISIN EXTRACTION ----------- #
def authenticate_gmail():
    """Authenticate Gmail API - handles both local and headless environments"""
    creds = None
    
    # Try to load existing credentials
    if os.path.exists('token.json'):
        print("📱 Loading existing Gmail credentials from token.json...")
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            
            # Check if credentials are valid
            if creds and creds.valid:
                print("✅ Gmail credentials are valid")
                return build('gmail', 'v1', credentials=creds)
            elif creds and creds.expired and creds.refresh_token:
                print("🔄 Refreshing expired Gmail credentials...")
                creds.refresh(Request())
                # Save refreshed credentials
                with open('token.json', 'w') as token:
                    token.write(creds.to_json())
                print("✅ Gmail credentials refreshed successfully")
                return build('gmail', 'v1', credentials=creds)
            else:
                print("⚠️ Gmail credentials are invalid or expired without refresh token")
                
        except Exception as e:
            print(f"❌ Error loading credentials from token.json: {e}")
    
    # If we reach here, we need to get new credentials
    print("🔐 Attempting to get new Gmail credentials...")
    
    # Check if we're in a headless environment (like GitHub Actions)
    if os.getenv('GITHUB_ACTIONS') or os.getenv('CI'):
        print("❌ ERROR: Running in headless environment (GitHub Actions/CI)")
        print("❌ Gmail OAuth requires manual authentication which cannot be done in headless environments")
        print("❌ Please run this script locally first to generate token.json, then commit it to your repository")
        print("❌ Make sure token.json is not in .gitignore and is committed to your repo")
        raise Exception("Cannot authenticate Gmail in headless environment")
    
    # Try to authenticate locally
    if not os.path.exists('credentials.json'):
        print("❌ ERROR: credentials.json file not found")
        print("❌ Please download credentials.json from Google Cloud Console")
        raise FileNotFoundError("credentials.json file is missing")
    
    try:
        print("🌐 Starting OAuth flow (will open browser)...")
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=8080)
        
        # Save credentials for future use
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
        print("✅ Gmail authentication successful, token.json saved")
        
        return build('gmail', 'v1', credentials=creds)
        
    except Exception as e:
        print(f"❌ OAuth authentication failed: {e}")
        raise

def get_email_body(payload):
    plain, html = None, None
    if 'parts' in payload:
        for part in payload['parts']:
            p_txt, h_txt = get_email_body(part)
            plain = plain or p_txt
            html = html or h_txt
    else:
        mime_type = payload.get('mimeType')
        data = payload.get('body', {}).get('data')
        if data:
            decoded = base64.urlsafe_b64decode(data).decode(errors='replace')
            if mime_type == 'text/plain':
                plain = decoded
            elif mime_type == 'text/html':
                html = decoded
    return plain, html

def extract_isin_details_from_text(text):
    results = []
    for line in text.splitlines():
        line = line.strip()
        match = re.search(r'(INE[A-Z0-9]{9})', line)
        if match:
            isin = match.group(1)
            clean_line = re.sub(r'[^\w\s\-]', '', line)  # Remove special chars
            parts = clean_line.split()
            isin_index = next((i for i, part in enumerate(parts) if isin in part), -1)
            if isin_index != -1:
                company = " ".join(parts[:isin_index])
                instrument = " ".join(parts[isin_index + 1:])
                results.append({
                    "Company": company.strip(),
                    "ISIN": isin.strip(),
                    "Instrument": instrument.strip()
                })
            else:
                print(f"⚠️ ISIN {isin} found but not in parts: {parts}")
    return results

def isin_record_exists(table, isin):
    try:
        records = table.all()
        for r in records:
            f = r.get("fields", {})
            if f.get("ISIN", "").strip().lower() == isin.strip().lower():
                return True
        return False
    except Exception as e:
        print(f"❌ Error checking ISIN existence: {e}")
        return False

def fetch_and_append_new_isin_records():
    """Fetch new ISIN records from Gmail and add to Airtable"""
    print("📧 Fetching new ISIN records from Gmail...")
    
    try:
        service = authenticate_gmail()
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

        from_time = int(time.time()) - 7 * 24 * 60 * 60
        query = f'subject:"ISIN Activated" after:{from_time}'
        
        print(f"🔍 Searching Gmail with query: {query}")
        results = service.users().messages().list(userId='me', q=query, maxResults=20).execute()
        messages = results.get('messages', [])
        print(f"📬 Found {len(messages)} messages")
        
        new_records = 0

        for msg in messages:
            try:
                msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
                payload = msg_data['payload']
                plain, html = get_email_body(payload)
                text = plain or BeautifulSoup(html or "", "html.parser").get_text(separator="\n")

                for entry in extract_isin_details_from_text(text):
                    isin = entry["ISIN"]
                    company = entry["Company"]
                    instrument = entry["Instrument"]

                    if not isin_record_exists(table, isin):
                        record = {
                            "ISIN": isin,
                            "Security Type": instrument,
                            "Company Name": company,
                            "ISIN Allotment Date": datetime.now().strftime("%Y-%m-%d")
                        }
                        table.create(record)
                        print(f"➕ Added ISIN: {isin} | {company} | {instrument}")
                        new_records += 1
                    else:
                        print(f"⚠️ ISIN {isin} already exists, skipping")
                        
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                continue
                
        print(f"📬 Total new ISINs added: {new_records}")
        return new_records

    except Exception as e:
        print(f"❌ Error in fetch_and_append_new_isin_records: {e}")
        print("ℹ️ Gmail functionality will be skipped, but reminder system will still work")
        return 0

# ----------- MAIN ----------- #
if __name__ == "__main__":
    print("🚀 Starting cron job...")
    print(f"📅 Current time: {get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    
    # Check environment variables
    required_vars = ['EMAIL_ADDRESS', 'EMAIL_PASSWORD', 'AIRTABLE_PERSONAL_ACCESS_TOKEN', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ ERROR: Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    try:
        # Always run reminder check (this should work in GitHub Actions)
        sent, errors = check_and_send_due_reminders()
        
        # Try to fetch new ISINs (this might fail in GitHub Actions if token.json is missing)
        new_isins = fetch_and_append_new_isin_records()
        
        print(f"📬 Summary: Sent reminders: {sent}, Errors: {errors}, New ISINs added: {new_isins}")
        
        # Exit with error code if there were errors in the reminder system
        sys.exit(1 if errors else 0)
        
    except Exception as e:
        print(f"❌ Fatal error in main: {e}")
        sys.exit(1)
