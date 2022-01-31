import os
import logging
import asyncpg
from databases import Database
log = logging.getLogger(__name__)

def create_database(default_db_path: str) -> Database:
    if os.environ.get("CHIA_DB_CONNECTION", None) is not None:
        validate_environment_variables()
        #still need to extract db name from default_db_string and use that
        return Database(os.environ.get("CHIA_DB_CONNECTION"))
    else:
        return Database("sqlite:///{}".format(default_db_path))

async def create_and_connect_to_wallet_database(default_db_path: str) -> Database:
    if os.environ.get("CHIA_WALLET_DB_ROOT", None) is not None:
        validate_environment_variables()
        db_name = extract_db_name(default_db_path)
        wallet_db_root  = os.environ.get('CHIA_WALLET_DB_ROOT')
        connection_string = f"{wallet_db_root}{db_name}"
        try:
            database = Database(connection_string)
            await database.connect()
            return database
        except asyncpg.InvalidCatalogNameError:
            sys_conn = Database(f"{wallet_db_root}postgres")
            await sys_conn.connect()
            await sys_conn.execute(f'CREATE DATABASE "{db_name}"')
            await sys_conn.disconnect()
            database = Database(connection_string)
            await database.connect()
            return database
    else:
        database = Database("sqlite:///{}".format(default_db_path))
        await database.connect()
        return database


def extract_db_name(db_path: str):
    return db_path.split('/')[-1].replace('.sqlite', '')

def validate_environment_variables():
    chia_db_connection = os.environ.get("CHIA_DB_CONNECTION", None)
    wallet_db_root = os.environ.get("CHIA_WALLET_DB_ROOT", None)

    if chia_db_connection is None and wallet_db_root is None:
        return
    
    if chia_db_connection is None:
        log.error('Missing environment variable: CHIA_DB_CONNECTION')
    
    if wallet_db_root is None:
        log.error('Missing environment variable: CHIA_WALLET_DB_ROOT')
