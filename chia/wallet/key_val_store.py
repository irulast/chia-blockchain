from typing import Any

from databases import Database

from chia.util.byte_types import hexstr_to_bytes
from chia.util.db_wrapper import DBWrapper
from chia.util import dialect_utils
from chia.util.streamable import Streamable


class KeyValStore:
    """
    Multipurpose persistent key-value store
    """

    #Changed
    db_connection: Database
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.db_wrapper = db_wrapper
        self.db_connection = db_wrapper.db
        await self.db_connection.execute(
            f"CREATE TABLE IF NOT EXISTS key_val_store(key {dialect_utils.data_type('text-as-index', self.db_connection.url.dialect)} PRIMARY KEY, value text)"
        )

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS name on key_val_store(key)")

        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM key_val_store")

    # Would this get a @classmethod tag as well?
    async def get_object(self, key: str, type: Any) -> Any:
        """
        Return bytes representation of stored object
        """

        row = await self.db_connection.fetch_one("SELECT * from key_val_store WHERE key=:key", {"key": key})

        if row is None:
            return None

        return type.from_bytes(hexstr_to_bytes(row[1]))

    async def set_object(self, key: str, obj: Streamable):
        """
        Adds object to key val store
        """
        async with self.db_wrapper.lock:
            row_to_insert = {"key": key, "value": bytes(obj).hex()}
            await self.db_connection.execute(
                dialect_utils.upsert_query('key_val_store', ['key'], row_to_insert.keys(), self.db_connection.url.dialect),
                row_to_insert
            )

