"""
Google Sheets sync service.

Tabs:
  "Form Responses 1"  — deal enrollments
  "Active Agents"     — short name -> full name mapping

Share the spreadsheet with the service account email from your credentials.json.
"""

import json
import logging
import os
from datetime import datetime

from google.oauth2 import service_account
from googleapiclient.discovery import build

from app.config import (
    GOOGLE_SHEETS_CREDENTIALS_JSON,
    GOOGLE_SHEETS_CREDENTIALS_FILE,
    GOOGLE_SHEETS_ID,
    GOOGLE_SHEETS_TAB,
    GOOGLE_SHEETS_AGENTS_TAB,
    GOOGLE_SHEETS_COL_TIMESTAMP,
    GOOGLE_SHEETS_COL_AGENT,
    GOOGLE_SHEETS_COL_PHONE,
    GOOGLE_SHEETS_COL_CLOSE_DATE,
    GOOGLE_SHEETS_COL_DEAL_VALUE,
    GOOGLE_SHEETS_COL_FIRST_PAYMENT,
    GOOGLE_SHEETS_COL_STATUS,
    GOOGLE_SHEETS_COL_CLIENT_NAME,
    GOOGLE_SHEETS_HEADER_ROW,
)
from app.models.database import get_db

logger = logging.getLogger(__name__)

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _get_sheets_service():
    if GOOGLE_SHEETS_CREDENTIALS_JSON:
        info = json.loads(GOOGLE_SHEETS_CREDENTIALS_JSON)
        creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    else:
        creds_path = GOOGLE_SHEETS_CREDENTIALS_FILE
        if not os.path.exists(creds_path):
            raise FileNotFoundError(
                f"Google Sheets credentials not found at '{creds_path}'. "
                "Set GOOGLE_SHEETS_CREDENTIALS_JSON or GOOGLE_SHEETS_CREDENTIALS_FILE."
            )
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=SCOPES
        )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def _col(index: int, row: list) -> str | None:
    if index < 0 or index >= len(row):
        return None
    val = str(row[index]).strip()
    return val if val else None


def _digits_only(value: str | None) -> str | None:
    if not value:
        return None
    digits = "".join(c for c in value if c.isdigit())
    return digits if digits else None


async def sync_sheets():
    """Pull deal rows and agent names from Google Sheets, upsert into the DB."""
    if not GOOGLE_SHEETS_ID:
        logger.warning("GOOGLE_SHEETS_ID not set — skipping Sheets sync")
        return

    logger.info("Google Sheets sync starting…")
    error_msg = None
    records_written = 0

    try:
        service = _get_sheets_service()
        sheet = service.spreadsheets()

        # ── 1. Sync Active Agents tab ──────────────────────────────────────
        try:
            agents_result = sheet.values().get(
                spreadsheetId=GOOGLE_SHEETS_ID,
                range=f"'{GOOGLE_SHEETS_AGENTS_TAB}'!A2:C200",
            ).execute()
            agent_rows = agents_result.get("values", [])

            async with get_db() as db:
                for row in agent_rows:
                    short_name = _col(0, row)
                    full_name  = _col(1, row)
                    email      = _col(2, row)
                    if short_name:
                        await db.execute(
                            """INSERT INTO agents (short_name, full_name, email)
                               VALUES ($1, $2, $3)
                               ON CONFLICT(short_name) DO UPDATE SET
                                 full_name = EXCLUDED.full_name,
                                 email     = EXCLUDED.email""",
                            short_name, full_name, email,
                        )
                # Remove agents no longer in the sheet (but keep those with BR usernames)
                sheet_names = [_col(0, r) for r in agent_rows if _col(0, r)]
                if sheet_names:
                    await db.execute(
                        """DELETE FROM agents
                           WHERE NOT (short_name = ANY($1))
                             AND (bluerock_username IS NULL OR bluerock_username = '')""",
                        sheet_names,
                    )
            logger.info("Agents synced: %d rows", len(agent_rows))
        except Exception as exc:
            logger.warning("Could not sync Active Agents tab: %s", exc)

        # ── 2. Sync Form Responses 1 -> deals ─────────────────────────────
        deals_result = sheet.values().get(
            spreadsheetId=GOOGLE_SHEETS_ID,
            range=f"'{GOOGLE_SHEETS_TAB}'!A1:Z",
        ).execute()
        all_rows = deals_result.get("values", [])

        if not all_rows:
            logger.warning("No rows returned from '%s'", GOOGLE_SHEETS_TAB)
            return

        data_rows = all_rows[GOOGLE_SHEETS_HEADER_ROW:]
        logger.info("Deals sheet: %d data rows", len(data_rows))

        def _parse_timestamp(val: str | None):
            if not val:
                return None
            for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y", "%-m/%-d/%Y %H:%M:%S"):
                try:
                    return datetime.strptime(val.strip(), fmt)
                except ValueError:
                    pass
            return None

        def _parse_amount(raw):
            if not raw:
                return None
            try:
                return float(raw.replace("$", "").replace(",", "").strip())
            except ValueError:
                return None

        async with get_db() as db:
            # Upsert every row — no date filter, no DELETE
            # sheet_row (spreadsheet row number) is the natural unique key
            active_sheet_rows: list[int] = []
            for idx, row in enumerate(data_rows, start=GOOGLE_SHEETS_HEADER_ROW + 1):
                submitted_raw = _col(GOOGLE_SHEETS_COL_TIMESTAMP, row)
                submitted_dt  = _parse_timestamp(submitted_raw)
                if submitted_dt is None:
                    continue  # skip rows with no parseable timestamp
                active_sheet_rows.append(idx)

                agent_name    = _col(GOOGLE_SHEETS_COL_AGENT, row)
                client_id_raw = _col(GOOGLE_SHEETS_COL_PHONE, row)
                closed_at     = _col(GOOGLE_SHEETS_COL_CLOSE_DATE, row)
                deal_val_raw  = _col(GOOGLE_SHEETS_COL_DEAL_VALUE, row)
                first_pay_raw = _col(GOOGLE_SHEETS_COL_FIRST_PAYMENT, row)
                status        = _col(GOOGLE_SHEETS_COL_STATUS, row)
                client_name   = _col(GOOGLE_SHEETS_COL_CLIENT_NAME, row)

                phone            = str(client_id_raw) if client_id_raw else None
                deal_value       = _parse_amount(deal_val_raw)
                first_payment_amt = _parse_amount(first_pay_raw)
                submitted_at_str = submitted_dt.strftime("%Y-%m-%d %H:%M:%S")

                await db.execute(
                    """INSERT INTO deals
                       (agent_name, client_name, phone_number, enrollment_status,
                        closed_at, deal_value, first_payment_amount, submitted_at,
                        raw_row, sheet_row)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                       ON CONFLICT(sheet_row) DO UPDATE SET
                         agent_name           = EXCLUDED.agent_name,
                         client_name          = EXCLUDED.client_name,
                         phone_number         = EXCLUDED.phone_number,
                         enrollment_status    = EXCLUDED.enrollment_status,
                         closed_at            = EXCLUDED.closed_at,
                         deal_value           = EXCLUDED.deal_value,
                         first_payment_amount = EXCLUDED.first_payment_amount,
                         submitted_at         = EXCLUDED.submitted_at,
                         raw_row              = EXCLUDED.raw_row,
                         synced_at            = NOW()::TEXT""",
                    agent_name, client_name, phone, status,
                    closed_at, deal_value, first_payment_amt, submitted_at_str,
                    json.dumps(row), idx,
                )
                records_written += 1

            # Remove deals whose sheet row was deleted from the spreadsheet
            if active_sheet_rows:
                deleted = await db.fetchval(
                    "SELECT COUNT(*) FROM deals WHERE sheet_row IS NOT NULL AND NOT (sheet_row = ANY($1))",
                    active_sheet_rows,
                )
                if deleted:
                    await db.execute(
                        "DELETE FROM deals WHERE sheet_row IS NOT NULL AND NOT (sheet_row = ANY($1))",
                        active_sheet_rows,
                    )
                    logger.info("Deals reconciliation: removed %d rows no longer in sheet", deleted)

        logger.info("Sheets sync done — %d deals loaded", records_written)

    except Exception as exc:
        error_msg = str(exc)
        logger.exception("Google Sheets sync failed: %s", exc)

    # ── Update sync log ────────────────────────────────────────────────
    async with get_db() as db:
        await db.execute(
            """UPDATE sync_log
               SET last_synced_at  = NOW()::TEXT,
                   last_error      = $1,
                   records_written = $2
               WHERE source = 'google_sheets'""",
            error_msg, records_written,
        )
