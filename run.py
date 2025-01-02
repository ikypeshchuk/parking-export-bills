from threading import Thread
from typing import Dict, List, Set
from datetime import datetime
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path

import pymysql
from as_types import BillPaymentTypes, PaymentTypes, SendToTypes, JsonDict
from clients.bills_client import BillsAPIClient
from configs import AppConfig, DatabaseConfig
from db_managers.sqllite_manager import SQLiteManager
import schedule
from dotenv import load_dotenv
from terminal_associations import TERMINAL_ASSOCIATIONS


class WeekdayFormatter:
    """Utility class for weekday formatting."""
    _WEEKDAY_NAMES = {
        1: 'Понеділок',
        2: 'Вівторок',
        3: 'Середа',
        4: 'Четвер',
        5: 'Пʼятниця',
        6: 'Субота',
        7: 'Неділя'
    }

    @classmethod
    def format_weekday(cls, weekday_number: int) -> str:
        """Format weekday number to localized string."""
        if weekday_number not in cls._WEEKDAY_NAMES:
            return ''
        return f"{weekday_number}. {cls._WEEKDAY_NAMES[weekday_number]}"


class ParkingDataProcessor:
    """Main parking data processing logic."""
    def __init__(self, config: AppConfig):
        self.config = config
        self.sqlite = SQLiteManager(config.sqlite_path)
        self.client = BillsAPIClient()
        self.sent_checks: Set[int] = set()
        self._update_sent_checks()

    def _update_sent_checks(self) -> None:
        """Update set of processed checks."""
        with sqlite3.connect(self.config.sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM sent_checks')
            self.sent_checks = {row[0] for row in cursor.fetchall()}

    def _format_datetime(self, timestamp: int, remove_minutes: bool = False) -> str:
        """Format Unix timestamp to datetime string."""
        dt = datetime.fromtimestamp(timestamp)
        if remove_minutes:
            dt = dt.replace(minute=0, second=0)

        return dt.strftime('%Y-%m-%d %H:%M:%S')

    def process_bills_data(self, row: Dict) -> JsonDict:
        """Transform raw payment record into bills format."""
        parking_number, point_of_sale = self.sqlite.get_terminal_info(
            row['PAYMENT_TERMINAL_ID']
        )

        entry_time = datetime.fromtimestamp(int(row['ENTRY_TIME']))
        payment_time = datetime.fromtimestamp(int(row['PAYMENT_TIME']))
        duration = payment_time - entry_time
        duration_minutes = duration.total_seconds() // 60
        duration_hours = duration_minutes // 60
        remaining_minutes = duration_minutes % 60

        if duration_hours > 0 and remaining_minutes > 0:
            duration_str = f"{int(duration_hours)}г {int(remaining_minutes)}хв"
        elif duration_hours > 0:
            duration_str = f"{int(duration_hours)}г"
        else:
            duration_str = f"{int(remaining_minutes)}хв"
    
        return {
            'POINT_OF_SALE': parking_number,
            'ID': row['ID'],
            'OPERATION_ID': row['OPERATION_ID'],
            'date_payment': self._format_datetime(int(row['PAYMENT_TIME'])),
            'description': (f"бокс: {point_of_sale}, "
                            f"час перебування: {duration_str}, "
                            f"час вїзду: {self._format_datetime(int(row['ENTRY_TIME']))}, "
                            f"час оплати: {self._format_datetime(int(row['PAYMENT_TIME']))} "),
            'document_id': f"{row['ID']}_{row['OPERATION_ID']}",
            'discount': row['DISCOUNT'] / 100,
            'items': [
                {
                    'name': f'Оплата парковки',
                    'price': (row['PAYMENT_MONEY'] - row['DISCOUNT']) / 100,
                    'quantity': 1
                }
            ],
            'number': f"{row['ID']}_{row['OPERATION_ID']}",
            'payments': [
                {
                    'type': BillPaymentTypes.get_type(row['TYPE_PAY']),
                    'value': (row['PAYMENT_MONEY'] - row['DISCOUNT']) / 100
                }
            ]
        }

    def process_batch(self) -> None:
        """Process and send batch of payment records."""
        try:
            with pymysql.connect(
                host=self.config.db_config.host,
                user=self.config.db_config.user,
                password=self.config.db_config.password,
                db=self.config.db_config.name,
                port=self.config.db_config.port,
                cursorclass=pymysql.cursors.DictCursor
            ) as conn:
                with conn.cursor() as cursor:
                    last_id = self._get_last_processed_id()
                    cursor.execute(
                        """SELECT * FROM payments_invoices 
                        WHERE ID > %s 
                        ORDER BY ID ASC 
                        LIMIT %s""",
                        (last_id, self.config.batch_limit)
                    )
                    records = cursor.fetchall()

            if not records:
                logging.info("No new records to process")
                return

            batch_data = []
            for record in records:
                if record['ID'] in self.sent_checks:
                    continue
                
                processed_record = self.process_bills_data(record)

                batch_data.append(processed_record)

                if len(batch_data) >= self.config.batch_limit:
                    from pprint import pprint
                    #print('in', datetime.now(), batch_data)
                    respotse_data = self.client.send(batch_data)
                    self._mark_batch_as_sent(respotse_data)

                    batch_data = []

            if batch_data:  # Send remaining records
                from pprint import pprint
                #print('out', datetime.now(), batch_data)
                respotse_data = self.client.send(batch_data)
                self._mark_batch_as_sent(respotse_data)

                batch_data = []

        except Exception as e:
            logging.error(f"Error processing batch: {e}")
            raise

    def _get_last_processed_id(self) -> int:
        """Get ID of last processed record."""
        with sqlite3.connect(self.config.sqlite_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT mysql_id FROM last_processed_operation WHERE id = 1")
            result = cursor.fetchone()

            return result[0] if result else 0

    def _mark_batch_as_sent(self, batch: List[JsonDict]) -> None:
        """Mark batch records as processed."""
        with sqlite3.connect(self.config.sqlite_path) as conn:
            cursor = conn.cursor()
            for record in batch:
                cursor.execute(
                    """INSERT INTO sent_checks (id, check_id, sent_timestamp)
                    VALUES (?, ?, CURRENT_TIMESTAMP)""",
                    (record['ID'], record['OPERATION_ID'])
                )
                cursor.execute(
                    """UPDATE last_processed_operation 
                    SET mysql_id = ?, operation_id = ?, 
                    computed_timestamp = CURRENT_TIMESTAMP,
                    computed_count = computed_count + 1 
                    WHERE id = 1""",
                    (record['ID'], record['OPERATION_ID'])
                )
            conn.commit()
    
    def run_scheduler(self):
        schedule.every(self.config.task_interval).minutes.do(self.process_batch)
        logging.info(f'Starting parking data processor at {datetime.now()}')
        
        while True:
            schedule.run_pending()
            time.sleep(1)

    def start(self):
        scheduler_thread = Thread(target=self.run_scheduler, daemon=True)
        scheduler_thread.start()
        logging.info('Scheduler thread started')


def setup_logging():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )


def load_config():
    load_dotenv()
    return AppConfig(
        batch_limit=int(os.getenv('BATCH_DATA_LIMIT', '5000')),
        task_interval=int(os.getenv('PERFORM_TASKS_EVERY_MINUTES', '5')),
        db_config=DatabaseConfig(
            name=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=int(os.getenv('DB_PORT', '3306'))
        ),
        sqlite_path=Path('./db-data') / f"{os.getenv('DB_NAME')}.db"
    )

def main():
    setup_logging()
    config = load_config()

    processor = ParkingDataProcessor(config)

    for terminal_id, description, parking in TERMINAL_ASSOCIATIONS:
        processor.sqlite.save_terminal_association(description, parking, terminal_id)

    processor.start()

    try:
        while True:
            time.sleep(10)  # Keep the main thread alive
    except KeyboardInterrupt:
        logging.info('Shutting down...')
    except Exception as e:
        logging.error(f'Unexpected error: {e}')

if __name__ == '__main__':
    main()
