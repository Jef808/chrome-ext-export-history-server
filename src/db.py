import sqlite3
from datetime import datetime
from contextlib import contextmanager

class BrowsingEventDB:
    def __init__(self, db_path: str = '/Users/jfa/.histdb-chrome/browsing_events.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database tables."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS browsing_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    url_id INTEGER NOT NULL,
                    tab_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    user_id INTEGER,
                    FOREIGN KEY (url_id) REFERENCES urls (id),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON browsing_events (timestamp)")

            conn.commit()

    @contextmanager
    def get_connection(self):
        """Context manager for database connection."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def _get_or_create_user(self, cursor: sqlite3.Cursor, username: str) -> int | None:
        """Get user ID or create a new user."""
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO users (username) VALUES (?)", (username,))
        return cursor.lastrowid

    def _get_or_create_url(self, cursor: sqlite3.Cursor, url: str) -> int | None:
        """Get URL ID or create a new URL."""
        cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO urls (url) VALUES (?)", (url,))
        return cursor.lastrowid

    def store_event(self, event_data: dict) -> int | None:
        """Store a browsing event in the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            url_id = self._get_or_create_url(cursor, event_data['url'])
            user_id = None
            if event_data.get('user'):
                user_id = self._get_or_create_user(cursor, event_data['user'])

            timestamp = event_data['timestamp']
            if isinstance(timestamp, datetime):
                timestamp_epoch = int(timestamp.timestamp())
            else:
                timestamp_epoch = int(datetime.fromisoformat(str(timestamp)).timestamp())

            cursor.execute("""
                INSERT INTO browsing_events
                (type, url_id, tab_id, timestamp, user_id)
                VALUES (?, ?, ?, ?, ?)
            """, (
                event_data['type'],
                url_id,
                event_data['tabId'],
                timestamp_epoch,
                user_id
            ))

            event_id = cursor.lastrowid
            conn.commit()
            return event_id
