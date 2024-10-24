# src/scripts/restore_db.py

import os
import subprocess
import gzip
import logging
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseRestore:
    def __init__(self, backup_path: Optional[str] = None):
        self.db_name = os.getenv('DB_NAME', 'coinwatch')
        self.db_user = os.getenv('DB_USER', 'user')
        self.db_password = os.getenv('DB_PASSWORD', 'password')
        self.db_host = os.getenv('DB_HOST', 'db')

        self.backup_dir = Path(backup_path) if backup_path else Path('backups')

    def restore_backup(self, backup_file: str) -> None:
        """Restore database from a backup file."""
        backup_path = Path(backup_file)
        if not backup_path.exists():
            raise FileNotFoundError(f"Backup file not found: {backup_file}")

        try:
            cmd = [
                'mysql',
                f'--host={self.db_host}',
                f'--user={self.db_user}',
                f'--password={self.db_password}',
                self.db_name
            ]

            # Handle compressed files
            if backup_file.endswith('.gz'):
                with gzip.open(backup_path, 'rb') as gz:
                    process = subprocess.Popen(
                        cmd,
                        stdin=subprocess.PIPE,
                        stderr=subprocess.PIPE
                    )
                    stdout, stderr = process.communicate(gz.read())

                    if process.returncode != 0:
                        raise Exception(f"Restore failed: {stderr.decode()}")
            else:
                with open(backup_path, 'r') as f:
                    subprocess.run(cmd, stdin=f, check=True)

            logger.info(f"Database restored successfully from: {backup_file}")

        except Exception as e:
            logger.error(f"Restore failed: {str(e)}")
            raise

    def verify_backup(self, backup_file: str) -> bool:
        """Verify if backup file is valid and readable."""
        try:
            backup_path = Path(backup_file)
            if not backup_path.exists():
                return False

            # Try to read the file
            if backup_file.endswith('.gz'):
                with gzip.open(backup_path, 'rb') as gz:
                    # Read first few bytes to verify it's a valid gzip file
                    gz.read(1024)
            else:
                with open(backup_path, 'r') as f:
                    # Read first few lines to verify it's a SQL dump
                    first_lines = [next(f) for _ in range(5)]
                    if not any('CREATE TABLE' in line or 'INSERT INTO' in line for line in first_lines):
                        return False

            return True

        except Exception as e:
            logger.error(f"Backup verification failed: {str(e)}")
            return False

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Database restore utility')
    parser.add_argument('file', help='Backup file to restore')
    parser.add_argument('--verify-only', action='store_true', help='Only verify backup without restoring')

    args = parser.parse_args()

    restore = DatabaseRestore()

    try:
        if args.verify_only:
            if restore.verify_backup(args.file):
                logger.info("Backup file verified successfully")
                exit(0)
            else:
                logger.error("Backup file verification failed")
                exit(1)
        else:
            restore.restore_backup(args.file)
    except Exception as e:
        logger.error(f"Restore failed: {str(e)}")
        exit(1)