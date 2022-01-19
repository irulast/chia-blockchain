from typing import Any

from chia.util.byte_types import hexstr_to_bytes
from chia.util.db_wrapper import DBWrapper
from databases import Database
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
            ("CREATE TABLE IF NOT EXISTS key_val_store(" " key text PRIMARY KEY," " value text)")
        )

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS name on key_val_store(key)")

        await self.db_connection.commit()
        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM key_val_store")

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
            await self.db_connection.execute(
                "INSERT OR REPLACE INTO key_val_store VALUES(:key, :value)",
                {"key": key, "value": bytes(obj).hex()},
            )

