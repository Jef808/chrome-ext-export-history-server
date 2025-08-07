import sqlite3
from datetime import datetime
from contextlib import contextmanager

class EventDB:
    def __init__(self, db_path: str = '/Users/jfa/.histdb-events/events.db'):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize database tables."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            # Browsing events tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE NOT NULL,
                    title TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS browsing_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT NOT NULL,
                    url_id INTEGER NOT NULL,
                    timestamp INTEGER NOT NULL,
                    user_id INTEGER,
                    FOREIGN KEY (url_id) REFERENCES urls (id),
                    FOREIGN KEY (user_id) REFERENCES users (id)
                )
            """)

            # Emacs events tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS buffers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS places (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host TEXT,
                    dir TEXT,
                    UNIQUE(host, dir)
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS emacs_commands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS emacs_major_modes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS emacs_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER NOT NULL,
                    session_id TEXT NOT NULL,
                    command_id INTEGER NOT NULL,
                    buffer_id INTEGER NOT NULL,
                    place_id INTEGER NOT NULL,
                    major_mode_id INTEGER NOT NULL,
                    project_id INTEGER,
                    FOREIGN KEY (command_id) REFERENCES emacs_commands (id),
                    FOREIGN KEY (buffer_id) REFERENCES buffers (id),
                    FOREIGN KEY (major_mode_id) REFERENCES emacs_major_modes (id),
                    FOREIGN KEY (project_id) REFERENCES projects (id)
                )
            """)

            # Indexes
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_browsing_events_timestamp ON browsing_events (timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emacs_events_timestamp ON emacs_events (timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_emacs_events_project ON emacs_events (project_id)")

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

    def _get_or_create_url(self, cursor: sqlite3.Cursor, url: str, title: str | None = None) -> int | None:
        """Get URL ID or create a new URL."""
        cursor.execute("SELECT id FROM urls WHERE url = ?", (url,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO urls (url, title) VALUES (?, ?)", (url, title))
        return cursor.lastrowid

    def _get_or_create_project(self, cursor: sqlite3.Cursor, project_name: str) -> int | None:
        """Get project ID or create a new project."""
        cursor.execute("SELECT id FROM projects WHERE name = ?", (project_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO projects (name) VALUES (?)", (project_name,))
        return cursor.lastrowid

    def _get_or_create_buffer(self, cursor: sqlite3.Cursor, buffer_name: str) -> int | None:
        """Get buffer ID or create a new buffer."""
        cursor.execute("SELECT id FROM buffers WHERE name = ?", (buffer_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO buffers (name) VALUES (?)", (buffer_name,))
        return cursor.lastrowid

    def _get_or_create_place(self, cursor: sqlite3.Cursor, host: str, directory: str | None = None) -> int | None:
        """Get place ID or create a new place."""
        cursor.execute("SELECT id FROM places WHERE host = ? AND dir = ?", (host, directory))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO places (host, dir) VALUES (?, ?)", (host, directory))
        return cursor.lastrowid

    def _get_or_create_command(self, cursor: sqlite3.Cursor, command_name: str) -> int | None:
        """Get command ID or create a new command."""
        cursor.execute("SELECT id FROM emacs_commands WHERE name = ?", (command_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO emacs_commands (name) VALUES (?)", (command_name,))
        return cursor.lastrowid

    def _get_or_create_major_mode(self, cursor: sqlite3.Cursor, mode_name: str) -> int | None:
        """Get major mode ID or create a new major mode."""
        cursor.execute("SELECT id FROM emacs_major_modes WHERE name = ?", (mode_name,))
        result = cursor.fetchone()

        if result:
            return result[0]

        cursor.execute("INSERT INTO emacs_major_modes (name) VALUES (?)", (mode_name,))
        return cursor.lastrowid

    def store_browsing_event(self, event_data: dict) -> int | None:
        """Store a browsing event in the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            url_id = self._get_or_create_url(cursor, event_data['url'], event_data['title'])
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
                (type, url_id, timestamp, user_id)
                VALUES (?, ?, ?, ?)
            """, (
                event_data['type'],
                url_id,
                timestamp_epoch,
                user_id
            ))

            event_id = cursor.lastrowid
            conn.commit()
            return event_id

    def store_emacs_event(self, event_data: dict) -> int | None:
        """Store an Emacs event in the database."""
        with self.get_connection() as conn:
            cursor = conn.cursor()

            context = event_data['context']

            project_id = None
            if context.get('project'):
                project_id = self._get_or_create_project(cursor, context['project'])

            buffer_id = self._get_or_create_buffer(cursor, context['buffer'])
            place_id = self._get_or_create_place(cursor, event_data['host'], context.get('file_name'))
            command_id = self._get_or_create_command(cursor, event_data['command'])
            major_mode_id = self._get_or_create_major_mode(cursor, context['major_mode'])

            timestamp = event_data['timestamp']
            if isinstance(timestamp, datetime):
                timestamp_epoch = int(timestamp.timestamp())
            else:
                timestamp_epoch = int(datetime.fromisoformat(str(timestamp)).timestamp())

            cursor.execute("""
                INSERT INTO emacs_events
                (timestamp, session_id, command_id, buffer_id, place_id, major_mode_id, project_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp_epoch,
                event_data['session_id'],
                command_id,
                buffer_id,
                place_id,
                major_mode_id,
                project_id
            ))

            event_id = cursor.lastrowid
            conn.commit()
            return event_id
