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

def debug_env_and_files():
    print("🔍 Debug Environment Variables and Files:")
    vars_to_check = [
        "EMAIL_ADDRESS", "EMAIL_PASSWORD",
        "AIRTABLE_PERSONAL_ACCESS_TOKEN", "AIRTABLE_BASE_ID",
        "AIRTABLE_TABLE_NAME"
    ]
    for var in vars_to_check:
        val = os.getenv(var)
        print(f" - {var}: {'Set' if val else 'NOT SET or empty'}")

    # Check existence and size of credentials.json and token.json
    for filename in ['credentials.json', 'token.json']:
        if os.path.exists(filename):
            size = os.path.getsize(filename)
            print(f" - {filename}: Exists, size = {size} bytes")
            if size > 0:
                with open(filename, 'r', encoding='utf-8') as f:
                    preview = f.read(200)
                    print(f"   Content preview (first 200 chars):\n{preview}\n---")
            else:
                print(f"   {filename} is empty!")
        else:
            print(f" - {filename}: NOT FOUND")

# Call this at the start of your main or key functions
debug_env_and_files()

# ... rest of your functions here unchanged ...

# Modify authenticate_gmail() to add debug prints
def authenticate_gmail():
    creds = None
    print("🔐 Authenticating Gmail API credentials...")
    if os.path.exists('token.json'):
        print(" - Found token.json, attempting to load credentials from it...")
        try:
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
            print(" - Loaded credentials from token.json successfully.")
        except Exception as e:
            print(f"❌ Failed to load credentials from token.json: {e}")
            raise
    else:
        print(" - token.json NOT found, attempting to generate new credentials from credentials.json...")
        if not os.path.exists('credentials.json'):
            raise FileNotFoundError("credentials.json file is missing.")
        try:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=8080)
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
            print(" - Generated new credentials and saved token.json successfully.")
        except Exception as e:
            print(f"❌ Failed during OAuth flow: {e}")
            raise
    return build('gmail', 'v1', credentials=creds)

# Add debug at the start of fetch_and_append_new_isin_records()
def fetch_and_append_new_isin_records():
    try:
        debug_env_and_files()  # Debug here too

        service = authenticate_gmail()
        table = Api(AIRTABLE_PERSONAL_ACCESS_TOKEN).table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

        from_time = int(time.time()) - 7 * 24 * 60 * 60
        query = f'subject:"ISIN Activated" after:{from_time}'
        results = service.users().messages().list(userId='me', q=query, maxResults=20).execute()
        messages = results.get('messages', [])
        print(f" - Fetched {len(messages)} messages with query: {query}")

        new_records = 0

        for msg in messages:
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
        print(f"📬 Total new ISINs added: {new_records}")
        return new_records

    except Exception as e:
        print(f"❌ Error in fetch_and_append_new_isin_records: {e}")
        return 0

# ----------- MAIN ----------- #
if __name__ == "__main__":
    print("🚀 Starting cron job...")
    debug_env_and_files()
    sent, errors = check_and_send_due_reminders()
    new_isins = fetch_and_append_new_isin_records()
    print(f"📬 Summary: Sent reminders: {sent}, Errors: {errors}, New ISINs added: {new_isins}")
    sys.exit(1 if errors else 0)
