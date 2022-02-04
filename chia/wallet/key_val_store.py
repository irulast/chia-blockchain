from typing import Any

from databases import Database

from chia.util.db_wrapper import DBWrapper
from chia.util import dialect_utils
from chia.util.streamable import Streamable


class KeyValStore:
    """
    Multipurpose persistent key-value store
    """

    db_connection: Database
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.db_wrapper = db_wrapper
        self.db_connection = db_wrapper.db
        await self.db_connection.execute(
            f"CREATE TABLE IF NOT EXISTS key_val_store({dialect_utils.reserved_word('key', self.db_connection.url.dialect)} {dialect_utils.data_type('blob-as-index', self.db_connection.url.dialect)} PRIMARY KEY, value text)"
        )

        await dialect_utils.create_index_if_not_exists(self.db_connection, 'name', 'key_val_store', [dialect_utils.reserved_word('key', self.db_connection.url.dialect)])
        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM key_val_store")

    async def get_object(self, key: str, object_type: Any) -> Any:
        """
        Return bytes representation of stored object
        """

        row = await self.db_connection.fetch_one(f"SELECT * from key_val_store WHERE {dialect_utils.reserved_word('key', self.db_connection.url.dialect)}=:key", {"key": key})

        if row is None:
            return None

        return object_type.from_bytes(row[1])

    async def set_object(self, key: str, obj: Streamable):
        """
        Adds object to key val store
        """
        async with self.db_wrapper.lock:
            row_to_insert = {"key": key, "value": bytes(obj)}
            await self.db_connection.execute(
                dialect_utils.upsert_query('key_val_store', ['key'], row_to_insert.keys(), self.db_connection.url.dialect),
                row_to_insert
            )


    async def remove_object(self, key: str):
        await self.db_connection.execute("DELETE FROM key_val_store where key=:key", {"key": key})
