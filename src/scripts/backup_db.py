# src/scripts/backup_db.py

import os
import subprocess
from datetime import datetime, timezone
import gzip
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseBackup:
    def __init__(self, backup_path: Optional[str] = None):
        """
        Initialize backup configuration.

        Args:
            backup_path: Optional path to store backups. If None, uses default location.
        """
        self.db_name = os.getenv('DB_NAME', 'coinwatch')
        self.db_user = os.getenv('DB_USER', 'user')
        self.db_password = os.getenv('DB_PASSWORD', 'password')
        self.db_host = os.getenv('DB_HOST', 'db')

        # Setup backup location
        if backup_path:
            self.backup_dir = Path(backup_path)
        else:
            self.backup_dir = Path('backups')
        self.backup_dir.mkdir(exist_ok=True)

    def create_backup(self, compress: bool = True) -> str:
        """Create a compressed backup of the database."""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        backup_name = f"{self.db_name}_{timestamp}"
        backup_file = self.backup_dir / f"{backup_name}.sql"

        try:
            # Create mysqldump command
            cmd = [
                'mysqldump',
                f'--host={self.db_host}',
                f'--user={self.db_user}',
                f'--password={self.db_password}',
                '--single-transaction',
                '--quick',
                self.db_name
            ]

            # Execute backup
            if compress:
                compressed_file = backup_file.with_suffix('.sql.gz')
                with gzip.open(compressed_file, 'wb') as gz:
                    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    stdout, stderr = process.communicate()

                    if process.returncode != 0:
                        raise Exception(f"Backup failed: {stderr.decode()}")

                    gz.write(stdout)
                logger.info(f"Created compressed backup: {compressed_file}")
                return str(compressed_file)
            else:
                with open(backup_file, 'w') as f:
                    subprocess.run(cmd, stdout=f, check=True)
                logger.info(f"Created backup: {backup_file}")
                return str(backup_file)

        except Exception as e:
            logger.error(f"Backup failed: {str(e)}")
            raise

    def backup_partition(self, partition_name: str) -> str:
        """Create a compressed backup of a specific partition."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = self.backup_dir / f"{partition_name}_{timestamp}.sql.gz"

        try:
            cmd = [
                'mysqldump',
                f'--host={self.db_host}',
                f'--user={self.db_user}',
                f'--password={self.db_password}',
                '--single-transaction',
                '--quick',
                f'--where="start_time IN (SELECT start_time FROM kline_data PARTITION({partition_name}))"',
                self.db_name,
                'kline_data'
            ]

            with gzip.open(backup_file, 'wb') as gz:
                process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                stdout, stderr = process.communicate()

                if process.returncode != 0:
                    raise Exception(f"Partition backup failed: {stderr.decode()}")

                gz.write(stdout)

            logger.info(f"Created partition backup: {backup_file}")
            return str(backup_file)

        except Exception as e:
            logger.error(f"Partition backup failed: {str(e)}")
            raise

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Database backup utility')
    parser.add_argument('--path', help='Backup storage path')
    parser.add_argument('--partition', help='Specific partition to backup')
    parser.add_argument('--no-compress', action='store_true', help='Disable compression')

    args = parser.parse_args()

    backup = DatabaseBackup(args.path)

    try:
        if args.partition:
            backup.backup_partition(args.partition)
        else:
            backup.create_backup(compress=not args.no_compress)
    except Exception as e:
        logger.error(f"Backup failed: {str(e)}")
        exit(1)