import os
import sys
import time
from datetime import datetime
import pytz
import base64
import re
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from pyairtable import Api
from dotenv import load_dotenv

load_dotenv()

# -------- TIMEZONE SETUP -------- #
IST = pytz.timezone('Asia/Kolkata')

# -------- CONFIG -------- #
EMAIL_ADDRESS = os.getenv("EMAIL_ADDRESS")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
AIRTABLE_PERSONAL_ACCESS_TOKEN = os.getenv("AIRTABLE_PERSONAL_ACCESS_TOKEN")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# -------- Helper functions -------- #
def get_ist_now():
    return datetime.now(IST)

def convert_to_ist(dt):
    if dt.tzinfo is None:
        return IST.localize(dt)
    else:
        return dt.astimezone(IST)

# -------- Reminder Email Sender -------- #
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
        print(f"Error sending email to {to}: {e}")
        return False

def parse_bill_date(date_str):
    if not date_str:
        return None

    # Try ISO format first
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass

    # Try dd-Mmm-yy format like '28-Sep-24'
    try:
        return datetime.strptime(date_str, '%d-%b-%y')
    except ValueError:
        pass

    # Unknown format
    raise ValueError(f"Unknown date format: {date_str}")

def check_and_send_due_reminders():
    """Send reminders for all records whose BILL Date 1 is today or earlier"""
    if not all([EMAIL_ADDRESS, EMAIL_PASSWORD, AIRTABLE_PERSONAL_ACCESS_TOKEN, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME]):
        print("ERROR: Missing required environment variables")
        return 0, 1

    try:
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        ist = pytz.timezone('Asia/Kolkata')
        current_time_ist = datetime.now(ist)
        print(f"[{current_time_ist.strftime('%Y-%m-%d %H:%M:%S IST')}] Checking for due reminders based on BILL Date 1...")

        records = table.all()
        sent_count = 0
        error_count = 0

        for record in records:
            fields = record.get('fields', {})
            bill_date_str = fields.get('BILL Date 1', '')
            isin = fields.get('ISIN', '')
            email = fields.get('Email ID', '')
            bill_amount = fields.get('Bill Amount', '')

            if not bill_date_str:
                print(f"Skipping record with ISIN {isin} due to empty BILL Date 1")
                continue
            if not email:
                print(f"Skipping record with ISIN {isin} due to missing email")
                continue

            try:
                dt = parse_bill_date(bill_date_str)

                # If naive datetime, assume UTC
                if dt.tzinfo is None:
                    dt = pytz.utc.localize(dt)

                dt_ist = dt.astimezone(ist)

                if current_time_ist >= dt_ist:
                    # subject = f"TEST Reminder: BILL Due for ISIN -{isin}"
                    subject = f"TEST Reminder: BILL Due for ISIN -ISIN12345"
                    message = (
                        f"Dear user,\n\n"
                        f"This is a test reminder that the BILL dated {dt_ist.strftime('%Y-%m-%d')} "
                        f"for ISIN {isin} with amount {bill_amount} is due.\n\n"
                        f"Please take the necessary action.\n\n"
                        f"Best regards,\nYour Reminder System"
                    )

                    print(f"Sending reminder to {email} for ISIN {isin} with BILL Date {dt_ist.strftime('%Y-%m-%d')}")
                    test_email = "dhruv.vatsa1111@gmail.com"
                    if send_email(subject, message, test_email):
                        print(f"✅ Successfully sent reminder to {email}")
                        sent_count += 1
                    else:
                        print(f"❌ Failed to send reminder to {email}")
                        error_count += 1
                else:
                    print(f"Reminder NOT due yet for ISIN {isin} on {dt_ist.strftime('%Y-%m-%d')}")

            except Exception as e:
                print(f"Failed to parse BILL Date 1 '{bill_date_str}' for ISIN {isin}: {e}")
                error_count += 1

        print(f"Completed: {sent_count} reminders sent, {error_count} errors")
        return sent_count, error_count

    except Exception as e:
        print(f"Error in check_and_send_due_reminders: {e}")
        return 0, 1


# -------- Gmail API Helpers for ISIN fetch -------- #
def authenticate_gmail():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    else:
        flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
        creds = flow.run_local_server(port=8080)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('gmail', 'v1', credentials=creds)

def get_email_body(payload):
    plain_text = None
    html_content = None
    if 'parts' in payload:
        for part in payload['parts']:
            pt, ht = get_email_body(part)
            if pt and not plain_text:
                plain_text = pt
            if ht and not html_content:
                html_content = ht
    else:
        mime_type = payload.get('mimeType')
        data = payload.get('body', {}).get('data')
        if data:
            decoded = base64.urlsafe_b64decode(data).decode(errors='replace')
            if mime_type == 'text/plain':
                plain_text = decoded
            elif mime_type == 'text/html':
                html_content = decoded
    return plain_text, html_content

def extract_isin_details_from_text(text):
    results = []
    for line in text.splitlines():
        line = line.strip()
        if not line or 'Company' in line or 'ISIN' in line or 'Instrument' in line:
            continue
        match = re.search(r'(INE[A-Z0-9]{9})', line)
        if match:
            isin = match.group(1)
            parts = line.split()
            isin_index = parts.index(isin)
            company = " ".join(parts[:isin_index])
            instrument = " ".join(parts[isin_index+1:])
            results.append({
                "Company": company,
                "ISIN": isin,
                "Instrument": instrument
            })
    return results

def isin_record_exists(table, company_name, isin):
    records = table.all()
    for r in records:
        f = r.get("fields", {})
        if f.get("Company Name", "").strip().lower() == company_name.strip().lower() and \
           f.get("ISIN", "").strip().lower() == isin.strip().lower():
            return True
    return False

def fetch_and_append_new_isin_records():
    try:
        service = authenticate_gmail()
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)
        
        # Query emails with subject "ISIN Activated" from last 7 days (or adjust as needed)
        from_time = int(time.time()) - 7*24*60*60
        query = f'subject:"ISIN Activated" after:{from_time}'
        results = service.users().messages().list(userId='me', q=query, maxResults=20).execute()
        messages = results.get('messages', [])
        
        new_records_added = 0
        for msg in messages:
            msg_data = service.users().messages().get(userId='me', id=msg['id'], format='full').execute()
            headers = msg_data['payload']['headers']

            subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), '')
            # You can print or log subject if needed
            
            payload = msg_data['payload']
            plain_text, html_content = get_email_body(payload)

            text_to_parse = plain_text or (BeautifulSoup(html_content, "html.parser").get_text(separator="\n") if html_content else None)

            if text_to_parse:
                extracted = extract_isin_details_from_text(text_to_parse)
                for entry in extracted:
                    company = entry["Company"]
                    isin = entry["ISIN"]
                    instrument = entry["Instrument"]

                    if not isin_record_exists(table, company, isin):
                        record = {
                            "ISIN": isin,
                            "Security Type": instrument,
                            "Company Name": company,
                            "ISIN Allotment Date": datetime.now().strftime("%Y-%m-%d")
                        }
                        try:
                            table.create(record)
                            print(f"Added new ISIN record: {company} - {isin}")
                            new_records_added += 1
                        except Exception as e:
                            print(f"Error adding record for {company}: {e}")
        
        print(f"Total new ISIN records added: {new_records_added}")
        return new_records_added
    
    except Exception as e:
        print(f"Error in fetch_and_append_new_isin_records: {e}")
        return 0

# -------- Main -------- #
if __name__ == "__main__":
    print("Starting cron job...")
    sent, errors = check_and_send_due_reminders()
    new_isins = fetch_and_append_new_isin_records()
    print(f"Summary: Sent reminders: {sent}, Errors: {errors}, New ISINs added: {new_isins}")
    
    if errors > 0:
        sys.exit(1)
    else:
        sys.exit(0)
