#!/usr/bin/env python3
import os
import sqlite3
import threading
import time

# ─── Configuration ───────────────────────────────────────────────────────────────
WINDOW_SECONDS = 300    # 5 minutes
MAX_CALLS      = 1150    # maximum allowed calls per window
DB_PATH        = "rate_limiter.db"

# Internal lock to serialize SQLite access within a process
_lock = threading.Lock()

def _get_connection():
    """
    Opens (or creates) the SQLite DB and ensures the 'calls' table exists.
    Returns a sqlite3.Connection with autocommit enabled.
    """
    parent = os.path.dirname(DB_PATH)
    if parent:
        os.makedirs(parent, exist_ok=True)

    # timeout=10 allows up to 10 seconds if the DB is locked by another process
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS calls (
            ts INTEGER PRIMARY KEY
        )
    """)
    return conn

def _prune_old(conn, cutoff_ts):
    """
    Delete any rows older than cutoff_ts.
    """
    conn.execute("DELETE FROM calls WHERE ts < ?", (cutoff_ts,))

def get_remaining_calls() -> int:
    """
    Returns how many calls remain in the current WINDOW_SECONDS window.
    """
    now = int(time.time())
    cutoff = now - WINDOW_SECONDS

    conn = _get_connection()
    with conn:
        # Prune any entries older than the window
        conn.execute("DELETE FROM calls WHERE ts < ?", (cutoff,))
        # Count how many remain
        cursor = conn.execute("SELECT COUNT(*) FROM calls")
        count = cursor.fetchone()[0]
    conn.close()

    return MAX_CALLS - count

def enforce_rate_limit():
    """
    Blocks (sleeps) until we are below MAX_CALLS in the last WINDOW_SECONDS.
    Then records the current timestamp as one new call.
    """
    while True:
        now = int(time.time())
        cutoff = now - WINDOW_SECONDS

        with _lock:
            conn = _get_connection()
            try:
                # 1) Prune old entries
                conn.execute("DELETE FROM calls WHERE ts < ?", (cutoff,))

                # 2) Count remaining
                cursor = conn.execute("SELECT COUNT(*) FROM calls")
                count = cursor.fetchone()[0]

                if count < MAX_CALLS:
                    # We have room → insert new timestamp, then return
                    conn.execute("INSERT INTO calls (ts) VALUES (?)", (now,))
                    conn.close()
                    return
                else:
                    # We are at limit → find oldest timestamp to know how long to wait
                    cursor = conn.execute("SELECT MIN(ts) FROM calls")
                    oldest = cursor.fetchone()[0] or cutoff
                    wait = (oldest + WINDOW_SECONDS) - now
                    conn.close()
                    if wait <= 0:
                        # If somehow already past, loop again immediately
                        continue
            except Exception:
                conn.close()
                raise

        # Sleep outside the lock so other threads/processes can proceed
        time.sleep(wait)

def record_call():
    """
    Alternative helper: simply record a new call timestamp without blocking.
    (Useful if you want to manually insert after a request.)
    """
    now = int(time.time())
    cutoff = now - WINDOW_SECONDS

    with _lock:
        conn = _get_connection()
        try:
            conn.execute("DELETE FROM calls WHERE ts < ?", (cutoff,))
            conn.execute("INSERT OR IGNORE INTO calls (ts) VALUES (?)", (now,))
        finally:
            conn.close()
