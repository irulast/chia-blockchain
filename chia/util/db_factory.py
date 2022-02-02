import asyncio
import os
import logging
import traceback
import asyncpg
from databases import Database
log = logging.getLogger(__name__)

async def get_database_connection(default_db_path: str) -> Database:
    if os.environ.get("CHIA_DB_ROOT", None) is not None:
        return await _create_database_from_env_var(default_db_path)
    else:
        database = DatabaseWrapper("sqlite:///{}".format(default_db_path))
        await database.connect()
        return database


async def _create_database_from_env_var(default_db_path):
    db_name = default_db_path.split('/')[-1].replace('.sqlite', '')
    db_root  = os.environ.get("CHIA_DB_ROOT")
    connection_string = f"{db_root}{db_name}"
    try:
        database = DatabaseWrapper(connection_string)
        await database.connect()
        return database
    except asyncpg.InvalidCatalogNameError:
        sys_conn = Database(f"{db_root}postgres")
        await sys_conn.connect()
        await sys_conn.execute(f'CREATE DATABASE "{db_name}"')
        await sys_conn.disconnect()
        database = DatabaseWrapper(connection_string)
        await database.connect()
        return database
    except Exception as e:
        log.error('CREATE_DATABASE_ERRORAAAAAAAA')
        log.error(traceback.format_exc())
        sys_conn = Database(db_root)
        await sys_conn.connect()
        await sys_conn.execute(f'CREATE DATABASE {db_name}')
        await sys_conn.disconnect()
        database = DatabaseWrapper(connection_string)
        await database.connect()
        return database

class DatabaseWrapper(Database):
    async def execute(self, *args, **kwargs):
        return (await asyncio.gather(super().execute(*args, **kwargs)))[0]

    async def execute_many(self, *args, **kwargs):
        return (await asyncio.gather(super().execute_many(*args, **kwargs)))[0]

    async def fetch_all(self, *args, **kwargs):
        return (await asyncio.gather(super().fetch_all(*args, **kwargs)))[0]

    async def fetch_one(self, *args, **kwargs):
        return (await asyncio.gather(super().fetch_one(*args, **kwargs)))[0]
    

