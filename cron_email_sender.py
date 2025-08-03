import os
import sys
import time
import base64
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyairtable import Api
import pytz

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

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

def send_email(subject, html_body, to):
    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = EMAIL_ADDRESS
        msg['To'] = to
        part = MIMEText(html_body, 'html')
        msg.attach(part)
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
            smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"❌ Email error to {to}: {e}")
        return False

def create_html_email_body(record_data):
    due_date = record_data.get('due_date', 'N/A')
    isin = record_data.get('isin', 'N/A')
    issuer = record_data.get('issuer', 'N/A')
    amount = record_data.get('amount', 'N/A')
    status = record_data.get('status', 'N/A')
    depository = record_data.get('depository', 'N/A')
    arn = record_data.get('arn', 'N/A')
    gstin = record_data.get('gstin', 'N/A')
    referred_by = record_data.get('company referred by', 'N/A')
    
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f9f9f9; }}
        .container {{ max-width: 650px; margin: 0; background: #ffffff; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.1); overflow: hidden; }}
        .header {{ background-color: #495057; color: white; padding: 25px; text-align: center; }}
        .header h1 {{ margin: 0; font-size: 24px; }}
        .content {{ padding: 30px; }}
        .content p {{ color: #555555; line-height: 1.6; }}
        .details-table {{ width: 100%; border-collapse: collapse; margin-top: 20px; margin-bottom: 20px; text-align: left; }}
        .details-table td {{ padding: 12px 15px; border: 1px solid #dddddd; }}
        .details-table .label-cell {{ background-color: #f2f2f2; font-weight: bold; width: 35%; color: #333333; }}
        .footer {{ background-color: #f2f2f2; text-align: center; padding: 15px; font-size: 12px; color: #888888; }}
    </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>Bill Due Reminder</h1>
            </div>
            <div class="content">
                <p>Dear User,</p>
                <p>This is an automated reminder that the following bill is now due. Please see the details below and take the necessary action.</p>
                <table class="details-table">
                    <tr><td class="label-cell">Due Date</td><td><b>{due_date}</b></td></tr>
                    <tr><td class="label-cell">ISIN</td><td>{isin}</td></tr>
                    <tr><td class="label-cell">Issuer / Company</td><td>{issuer}</td></tr>
                    <tr><td class="label-cell">Amount</td><td>{amount}</td></tr>
                    <tr><td class="label-cell">Status</td><td>{status}</td></tr>
                    <tr><td class="label-cell">Depository</td><td>{depository}</td></tr>
                    <tr><td class="label-cell">ARN</td><td>{arn}</td></tr>
                    <tr><td class="label-cell">GSTIN</td><td>{gstin}</td></tr>
                    <tr><td class="label-cell">Referred By</td><td>{referred_by}</td></tr>
                </table>
            </div>
            <div class="footer">
                <p>This is an automated message from the Nivis backend system.</p>
            </div>
        </div>
    </body>
    </html>
    """
    return html

def check_and_send_due_reminders():
    print(f"[{get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}] Checking for due reminders...")
    if not all([AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("❌ ERROR: Missing Airtable environment variables.")
        return 0, 1
    try:
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        today = get_ist_now().date()
        all_records = table.all()
        sent_count = 0
        error_count = 0
        for record in all_records:
            fields = record.get('fields', {})
            f_lower = {k.lower(): v for k, v in fields.items()}
            email = f_lower.get('email id', '')
            if not email:
                continue
            last_processed_index = int(f_lower.get('reminders sent till bill #', 0))
            next_bill_to_check_date = None
            next_bill_to_check_index = -1 # FIX: Initialized to -1 for clarity
            start_index = last_processed_index + 1
            for i in range(start_index, 73):
                date_str = f_lower.get(f'bill date {i}')
                if not date_str:
                    continue
                try:
                    bill_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    next_bill_to_check_date = bill_date
                    next_bill_to_check_index = i
                    break
                except (ValueError, TypeError):
                    continue
            
            # FIX: Removed duplicated 'if' statement
            if next_bill_to_check_date and next_bill_to_check_date <= today:
                try:
                    record_details = {
                        'due_date': next_bill_to_check_date.strftime('%Y-%m-%d'),
                        'isin': f_lower.get('isin', 'N/A'),
                        'issuer': f_lower.get('issuer', 'N/A'),
                        'amount': f_lower.get('amount', 'N/A'),
                        'status': f_lower.get('status', 'N/A'),
                        'depository': f_lower.get('depository', 'N/A'),
                        'arn': f_lower.get('arn if isin na (nsdl)', 'N/A'),
                        'gstin': f_lower.get('gstin', 'N/A'),
                        'company referred by': f_lower.get('company referred by', 'N/A'),
                    }
                    subject = f"Reminder: Bill Due for {record_details['issuer']} (ISIN: {record_details['isin']})"
                    html_message = create_html_email_body(record_details)
                    if send_email(subject, html_message, email):
                        print(f"✅ Sent reminder to {email} for Bill Date #{next_bill_to_check_index} (Due: {next_bill_to_check_date})")
                        table.update(record['id'], {"Reminders Sent Till Bill #": next_bill_to_check_index})
                        sent_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    print(f"❌ Reminder sending error for ISIN {f_lower.get('isin')}: {e}")
                    error_count += 1
        print(f"✅ Completed: {sent_count} sent, {error_count} errors")
        return sent_count, error_count
    except Exception as e:
        print(f"❌ Fatal error in check_and_send_due_reminders: {e}")
        return 0, 1

# ----------- GMAIL ISIN EXTRACTION ----------- #
def authenticate_gmail():
    creds = None
    if os.getenv('GITHUB_ACTIONS') or os.getenv('CI'):
        gmail_token = os.getenv('GMAIL_TOKEN_JSON')
        if gmail_token:
            try:
                with open('token.json', 'w') as f:
                    f.write(base64.b64decode(gmail_token).decode('utf-8'))
            except Exception as e:
                print(f"❌ Error processing GMAIL_TOKEN_JSON: {e}")
    if os.path.exists('token.json'):
        try:
            if os.path.getsize('token.json') > 0:
                creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        except Exception as e:
            print(f"❌ Error loading credentials from token.json: {e}")
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if os.getenv('GITHUB_ACTIONS') or os.getenv('CI'):
                raise Exception("Cannot authenticate Gmail in headless environment without a valid token.")
            if not os.path.exists('credentials.json'):
                raise FileNotFoundError("credentials.json file is missing.")
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

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
            clean_line = re.sub(r'[^\w\s\-]', '', line)
            parts = clean_line.split()
            isin_index = next((i for i, part in enumerate(parts) if isin in part), -1)
            if isin_index != -1:
                company = " ".join(parts[:isin_index])
                instrument = " ".join(parts[isin_index + 1:])
                results.append({"Company": company.strip(), "ISIN": isin.strip(), "Instrument": instrument.strip()})
    return results

def isin_record_exists(table, isin):
    try:
        records = table.all()
        for r in records:
            f = r.get("fields", {})
            f_lower = {k.lower(): v for k, v in f.items()}
            if f_lower.get("isin", "").strip().lower() == isin.strip().lower():
                return True
        return False
    except Exception as e:
        print(f"❌ Error checking ISIN existence: {e}")
        return False

def fetch_and_append_new_isin_records():
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
                plain, html = get_email_body(msg_data['payload'])
                text = plain or BeautifulSoup(html or "", "html.parser").get_text(separator="\n")
                for entry in extract_isin_details_from_text(text):
                    if not isin_record_exists(table, entry["ISIN"]):
                        record = {
                            "ISIN": entry["ISIN"],
                            "Security Type": entry["Instrument"],
                            "Issuer": entry["Company"],
                            "ISIN allotment date": datetime.now().strftime("%Y-%m-%d")
                        }
                        table.create(record)
                        print(f"➕ Added ISIN: {entry['ISIN']} | {entry['Company']} | {entry['Instrument']}")
                        new_records += 1
                    else:
                        print(f"⚠️ ISIN {entry['ISIN']} already exists, skipping")
            except Exception as e:
                print(f"❌ Error processing message: {e}")
        print(f"📬 Total new ISINs added: {new_records}")
        return new_records
    except Exception as e:
        print(f"❌ Error in fetch_and_append_new_isin_records: {e}")
        return 0

# ----------- MAIN ----------- #
if __name__ == "__main__":
    print("🚀 Starting cron job...")
    print(f"📅 Current time: {get_ist_now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    required_vars = ['EMAIL_ADDRESS', 'EMAIL_PASSWORD', 'AIRTABLE_PERSONAL_ACCESS_TOKEN', 'AIRTABLE_BASE_ID', 'AIRTABLE_TABLE_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print(f"❌ ERROR: Missing required environment variables: {missing_vars}")
        sys.exit(1)
    
    try:
        sent, errors = check_and_send_due_reminders()
        new_isins = fetch_and_append_new_isin_records()
        print(f"📬 Summary: Sent reminders: {sent}, Errors: {errors}, New ISINs added: {new_isins}")
        sys.exit(1 if errors else 0)
    except Exception as e:
        print(f"❌ Fatal error in main: {e}")
        sys.exit(1)
