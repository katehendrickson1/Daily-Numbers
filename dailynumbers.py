
"""
Daily Google Sheets → Slack (previous day's summary)
---------------------------------------------------
What this does
- Connects to Google Sheets with a Service Account (no Zapier needed)
- Pulls one row per sheet for the PREVIOUS day (America/Denver timezone)
- Builds a Slack message (Block Kit) and sends it to a channel via webhook or bot token
Setup (one-time)
1) Create a Google Cloud project → enable "Google Sheets API" + "Google Drive API".
2) Create a Service Account. Generate a JSON key and save its content in an env var: GOOGLE_SERVICE_ACCOUNT_JSON.
3) Share each target Google Sheet with the service account's email (Editor or at least Viewer).
4) Choose Slack auth:
   a) Incoming Webhook (simplest): set SLACK_WEBHOOK_URL
   b) Or a Slack App with chat:write: set SLACK_BOT_TOKEN and SLACK_CHANNEL_ID
5) Set optional envs: SHEETS_CONFIG_JSON to override the config below.
Run locally
- `pip install -r requirements.txt`
- `export GOOGLE_SERVICE_ACCOUNT_JSON='...json blob...'`
- `export SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'` (or SLACK_BOT_TOKEN & SLACK_CHANNEL_ID)
- `python daily_sheets_to_slack.py`
Scheduling
- Use cron, systemd timer, or GitHub Actions (workflow included in repo).
Notes
- We compute "yesterday" in America/Denver regardless of where this runs.
- Adjust the CONFIG to match your sheet columns (e.g., Date, Revenue, Unlimited, Washes).
"""
import json
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env

# ========== CONFIG ==========
# Replace these with your spreadsheets. You can also set SHEETS_CONFIG_JSON to a JSON array of the same shape.
# Each object:
# - title: label for Slack output
# - spreadsheet_id: the Google Sheets ID (the long hash in the URL)
# - worksheet: tab name or gid. Prefer the tab name for clarity
# - date_column: header name for the date column (must match the first row in the sheet)
# - fields: list of columns to include in the Slack summary (must match headers in the sheet)
# Slack auth (choose one method)
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")  # incoming webhook (simplest)
#SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")      # xoxb-...*
#SLACK_CHANNEL_ID = os.getenv("SLACK_CHANNEL_ID")    # e.g. C0123456789
# Optional: override CONFIG via env var (handy for GitHub Actions secrets)
if os.getenv("SHEETS_CONFIG_JSON"):
    CONFIG = json.loads(os.getenv("SHEETS_CONFIG_JSON"))
else:
    import pathlib
    cfg_path = pathlib.Path(__file__).with_name("config.json")
    if cfg_path.exists():
        CONFIG = json.loads(cfg_path.read_text(encoding="utf-8"))
        print(f"Loaded {len(CONFIG)} locations from config.json")

# Timezone for "previous day"
MT_TZ = ZoneInfo("America/Denver")

def mountain_yesterday() -> datetime:
    now_mt = datetime.now(MT_TZ)
    # "Previous day" at local 00:00
    y = (now_mt - timedelta(days=1)).date()
    return datetime(y.year, y.month, y.day, tzinfo=MT_TZ)
def make_creds():
    # Expect full JSON in GOOGLE_SERVICE_ACCOUNT_JSON
    key_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    if not key_json:
        raise RuntimeError("Missing env GOOGLE_SERVICE_ACCOUNT_JSON (the entire JSON key).")
    key_dict = json.loads(key_json)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(key_dict, scopes=scopes)
    return creds, key_dict.get("client_email")
def fetch_sheet_df(client: gspread.Client, spreadsheet_id: str, worksheet: str) -> pd.DataFrame:
    # Open by ID, then by worksheet name (or gid string like 'gid:123456')
    sh = client.open_by_key(spreadsheet_id)
    if worksheet.startswith("gid:"):
        ws = None
        gid = worksheet.split(":", 1)[1]
        # gspread doesn't open by gid directly; iterate to find a match
        for w in sh.worksheets():
            if str(w.id) == gid:
                ws = w
                break
        if ws is None:
            raise RuntimeError(f"Worksheet with {worksheet} not found.")
    else:
        ws = sh.worksheet(worksheet)
    data = ws.get_all_records(empty2zero=False, head=1)
    df = pd.DataFrame(data)
    return df
def row_for_date(df: pd.DataFrame, date_column: str, target_date: datetime) -> dict | None:
    if date_column not in df.columns:
        raise RuntimeError(f"Date column '{date_column}' not found. Found: {list(df.columns)}")
    # Normalize the date column
    dc = pd.to_datetime(df[date_column], errors="coerce").dt.date
    mask = dc == target_date.date()
    if not mask.any():
        return None
    row = df.loc[mask].iloc[0].to_dict()
    return row
def slack_post_blocks(blocks: list):
    # Prefer webhook if provided
    if SLACK_WEBHOOK_URL:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks})
        if resp.status_code >= 300:
            raise RuntimeError(f"Slack webhook failed: {resp.status_code} {resp.text}")
        return

    raise RuntimeError("No Slack credentials set. Provide SLACK_WEBHOOK_URL or SLACK_BOT_TOKEN+SLACK_CHANNEL_ID.")
def build_blocks(results: list[dict], date_label: str) -> list:
    import math
    def _fmt_value(label: str, v):
        # Missing/NaN
        if v is None:
            return "—"
        try:
            if isinstance(v, float) and math.isnan(v):
                return "—"
        except Exception:
            pass
        # Currency-style formatting if label hints at money
        if isinstance(v, (int, float)) and ("$" in label or "revenue" in label.lower()):
            return f"${float(v):,.2f}"
        # Strip decimals if it's a whole number
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"Daily Car Wash Summary – {date_label}"}},
        {"type": "divider"}
    ]
    for r in results:
        title = r["title"]
        status = r["status"]
        fields = r.get("fields", {})
        emoji = r.get("emoji", "📍")
        section_text = f"{emoji} *{title}*"
        fields_md = []
        for k, v in fields.items():
            fields_md.append(f"*{k}:* {_fmt_value(k, v)}")
        if fields_md:
            section_text += "\n" + "\n".join(fields_md)
        if status:
            section_text += f"\n{status}"
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": section_text}})
        blocks.append({"type": "divider"})
    return blocks
def main():
    y_mt = mountain_yesterday()
    # Cross-platform date (no %-d on Windows)
    date_label = y_mt.strftime("%a, %b %d, %Y").replace(" 0", " ")
    creds, sa_email = make_creds()
    client = gspread.authorize(creds)
    results = []
    for cfg in CONFIG:
        try:
            df = fetch_sheet_df(client, cfg["spreadsheet_id"], cfg["worksheet"])
            row = row_for_date(df, cfg["date_column"], y_mt)
            if row is None:
                results.append({
                    "title": cfg.get("title", cfg.get("worksheet", "Sheet")),
                    "status": ":warning: No row for previous day",
                })
                continue
            # Case-insensitive view of the row
            row_l = {str(k).lower(): v for k, v in row.items()}
            fields_cfg = cfg.get("fields", [])
            fields_out = {}
            for f in fields_cfg:
                # Case 1: simple string field
                if isinstance(f, str):
                    key = f.strip().lower()
                    label = f
                    val = row_l.get(key, None)
                # Case 2: object with "key" and optional "label"
                elif isinstance(f, dict) and "key" in f:
                    key = str(f["key"]).strip().lower()
                    label = f.get("label", key)
                    val = row_l.get(key, None)
                # Case 3: computed sum of multiple columns
                elif isinstance(f, dict) and "sum" in f:
                    label = f.get("label", "Sum")
                    keys = [str(k).strip().lower() for k in f["sum"]]
                    vals = []
                    for k in keys:
                        v = row_l.get(k, 0)
                        try:
                            # treat blanks/None/NaN as 0
                            if v is None or (isinstance(v, float) and pd.isna(v)) or v == "":
                                v = 0
                        except Exception:
                            pass
                        try:
                            vals.append(float(v))
                        except Exception:
                            vals.append(0.0)
                    val = sum(vals)
                else:
                    # Unrecognized field spec; skip it
                    continue
                fields_out[label] = val
            # Add one result per sheet
            results.append({
                "title": cfg.get("title", cfg.get("worksheet", "Sheet")),
                "emoji": cfg.get("emoji", "📍"), 
                "status": "",
                "fields": fields_out,
            })
        except Exception as e:
            results.append({
                "title": cfg.get("title", cfg.get("worksheet", "Sheet")),
                "status": f":x: Error – {e}",
            })
    blocks = build_blocks(results, date_label)
    slack_post_blocks(blocks)

if __name__ == "__main__":
    main()