from pathlib import Path
import sqlite3
from typing import Optional, Tuple


class SQLiteManager:
    """SQLite database manager."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._init_database()

    def _init_database(self) -> None:
        """Initialize SQLite database schema."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            self._create_tables(cursor)
            conn.commit()

    def _create_tables(self, cursor: sqlite3.Cursor) -> None:
        """Create necessary database tables."""
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS sent_checks (
                id INTEGER PRIMARY KEY,
                check_id INT,
                sent_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS terminal_parking_associations (
                terminal_description TEXT,
                parking_number VARCHAR(255),
                payment_terminal_id INT PRIMARY KEY
            );

            CREATE TABLE IF NOT EXISTS last_processed_operation (
                id INT PRIMARY KEY,
                mysql_id INT,
                operation_id INT,
                computed_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                computed_count INT DEFAULT 0
            );
        ''')
        
        # Initialize last_processed_operation if empty
        cursor.execute('''
            INSERT OR IGNORE INTO last_processed_operation 
            (id, mysql_id, operation_id, computed_timestamp, computed_count)
            VALUES (1, 0, 0, CURRENT_TIMESTAMP, 0)
        ''')

    def save_terminal_association(self, description: str, parking_number: str, terminal_id: int) -> None:
        """Save terminal-parking association."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT OR IGNORE INTO terminal_parking_associations VALUES (?, ?, ?)',
                (description, parking_number, terminal_id)
            )

    def get_terminal_info(self, terminal_id: int) -> Tuple[Optional[str], Optional[str]]:
        """Get parking information for terminal."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''SELECT parking_number, terminal_description 
                FROM terminal_parking_associations 
                WHERE payment_terminal_id = ?''',
                (terminal_id,)
            )
            result = cursor.fetchone()
            return (result[0], result[1]) if result else (None, None)

