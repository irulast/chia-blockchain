import os
import logging
import traceback
import asyncpg
from databases import Database
log = logging.getLogger(__name__)

async def create_database(default_db_path: str) -> Database:
    if os.environ.get("CHIA_DB_ROOT", None) is not None:
        return await _create_database_from_env_var(default_db_path)
    else:
        database = Database("sqlite:///{}".format(default_db_path))
        await database.connect()
        return database


async def _create_database_from_env_var(default_db_path):
    db_name = default_db_path.split('/')[-1].replace('.sqlite', '')
    db_root  = os.environ.get("CHIA_DB_ROOT")
    connection_string = f"{db_root}{db_name}"
    try:
        database = Database(connection_string)
        await database.connect()
        await database.disconnect()
        return database
    except asyncpg.InvalidCatalogNameError:
        sys_conn = Database(f"{db_root}postgres")
        await sys_conn.connect()
        await sys_conn.execute(f'CREATE DATABASE "{db_name}"')
        await sys_conn.disconnect()
        database = Database(connection_string)
        return database
    except Exception as e:
        log.error('CREATE_DATABASE_ERRORAAAAAAAA')
        log.error(traceback.format_exc())
        sys_conn = Database(db_root)
        await sys_conn.connect()
        await sys_conn.execute(f'CREATE DATABASE {db_name}')
        await sys_conn.disconnect()
        database = Database(connection_string)
        return database
