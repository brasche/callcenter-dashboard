"""
BlueRock call recording service.

Recordings are NOT available through the BlueRock REST API. Instead, they are accessed
by authenticating against the BlueRock web portal (https://web3.1bluerock.com/communicator/)
using a persistent session, then searching and downloading through that session.

Environment variables required:
  BLUEROCK_ACCOUNT   — account name (separate from BLUEROCK_API_KEY)
  BLUEROCK_USERNAME  — portal username
  BLUEROCK_PASSWORD  — portal password
"""

import asyncio
import logging
import os

import requests

from app.config import BLUEROCK_ACCOUNT, BLUEROCK_USERNAME, BLUEROCK_PASSWORD

logger = logging.getLogger(__name__)

_PORTAL_URL = "https://web3.1bluerock.com/communicator/index.php"

# Single persistent session — cookies from login carry over to all subsequent requests.
_session = requests.Session()
_logged_in = False


def login_to_bluerock() -> None:
    global _logged_in
    payload = {
        "mod": "login",
        "action": "login",
        "ajax": 1,
        "wid": "web-desktop-A559C1E8-9782-430D-A2B9-04EB0A9BF3B2",
        "account": BLUEROCK_ACCOUNT,
        "username": BLUEROCK_USERNAME,
        "password": BLUEROCK_PASSWORD,
        "remember": "on",
        "lang": "en_US",
    }
    response = _session.post(_PORTAL_URL, data=payload)
    if response.status_code == 200:
        _logged_in = True
        logger.info("Login to BlueRock recording portal successful")
    else:
        _logged_in = False
        raise Exception(
            f"BlueRock portal login failed ({response.status_code}): {response.text[:200]}"
        )


def find_recording_id(phone_number: str, call_time: str) -> str | None:
    """
    Search the portal for a recording matching phone_number and call_time.

    call_time should be "YYYY-MM-DD HH:MM:SS" (milliseconds are stripped before comparison).
    Returns the BlueRock recording ID string, or None if not found.
    """
    payload = {
        "mod": "recordings",
        "action": "get-recordings",
        "start": 0,
        "limit": 50,
        "filter": phone_number,
        "sort": "calldate",
        "dir": "DESC",
        "ajax": 1,
    }

    response = _session.post(_PORTAL_URL, data=payload)

    # Re-login on auth failure then retry once
    if response.status_code == 401:
        logger.info("BlueRock session expired (401); re-logging in")
        login_to_bluerock()
        response = _session.post(_PORTAL_URL, data=payload)

    if response.status_code != 200:
        raise Exception(
            f"Failed to fetch recordings: {response.status_code} {response.text[:200]}"
        )

    results = response.json().get("recordings", [])

    # Empty results may indicate a stale session; re-login once and retry
    if not results:
        logger.info("BlueRock returned empty recordings; re-logging in and retrying")
        login_to_bluerock()
        response = _session.post(_PORTAL_URL, data=payload)
        if response.status_code == 200:
            results = response.json().get("recordings", [])

    call_time_stripped = str(call_time).split(".")[0].strip()
    for recording in results:
        recording_time = str(recording.get("calldate", "")).split(".")[0].strip()
        if recording_time == call_time_stripped:
            return str(recording.get("id"))

    return None


def download_recording(recording_id: str) -> str:
    """
    Download a recording by its BlueRock recording ID as an MP3.
    Saves to the recordings/ directory; returns the local file path.
    Subsequent calls for the same ID return the cached file immediately.
    """
    save_dir = "recordings"
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"recording_{recording_id}.mp3")

    if os.path.exists(save_path):
        logger.debug("Recording %s already cached at %s", recording_id, save_path)
        return save_path

    payload = {
        "mod": "recordings",
        "action": "download-recording",
        "id": recording_id,
        "format": "mp3",
        "ajax": 1,
    }
    response = _session.post(_PORTAL_URL, data=payload)
    if response.status_code != 200:
        raise Exception(
            f"Failed to download recording {recording_id}: {response.status_code}"
        )

    with open(save_path, "wb") as f:
        f.write(response.content)

    logger.info(
        "Recording %s saved to %s (%d bytes)", recording_id, save_path, len(response.content)
    )
    return save_path


# ── Async wrappers (run synchronous requests in a thread to avoid blocking the event loop) ──

async def async_login() -> None:
    await asyncio.to_thread(login_to_bluerock)


async def async_find_recording_id(phone_number: str, call_time: str) -> str | None:
    return await asyncio.to_thread(find_recording_id, phone_number, call_time)


async def async_download_recording(recording_id: str) -> str:
    return await asyncio.to_thread(download_recording, recording_id)
