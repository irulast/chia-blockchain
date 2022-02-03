from typing import List, Tuple
from databases import Database
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.db_wrapper import DBWrapper
from chia.util import dialect_utils
import logging

log = logging.getLogger(__name__)


class HintStore:
    coin_record_db: Database
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.db_wrapper = db_wrapper

        if self.db_wrapper.db_version == 2:
            # TODO: handle blob key (mysql)
            await self.db_wrapper.db.execute(
                "CREATE TABLE IF NOT EXISTS hints(coin_id blob, hint blob, UNIQUE (coin_id, hint))"
            )
        else:
            # TODO: handle autoincrement use
            await self.db_wrapper.db.execute(
                "CREATE TABLE IF NOT EXISTS hints(id INTEGER PRIMARY KEY AUTOINCREMENT, coin_id blob, hint blob)"
            )
        await dialect_utils.create_index_if_not_exists(self.coin_record_db, 'hint_index', 'hints', ['hint'])
        return self

    async def get_coin_ids(self, hint: bytes) -> List[bytes32]:
        rows = await self.db_wrapper.db.fetch_all("SELECT * from hints WHERE hint=:hint", {"hint": hint})
        coin_ids = []
        for row in rows:
            coin_ids.append(row[0])
        return coin_ids

    async def add_hints(self, coin_hint_list: List[Tuple[bytes32, bytes]]) -> None:
        if self.db_wrapper.db_version == 2:
            # TODO: on conflict clause
            await self.coin_record_db.execute_many(
                "INSERT INTO hints(coin_id, hint) VALUES(:coin_id, :hint) ON CONFLICT DO NOTHING",
                map(lambda coin_hint: {"coin_id": coin_hint[0], "hint": coin_hint[1]}, coin_hint_list),
            )
        else:
            # TODO
            cursor = await self.db_wrapper.db.executemany(
                "INSERT INTO hints VALUES(?, ?, ?)",
                [(None,) + record for record in coin_hint_list],
            )

    async def count_hints(self) -> int:
        # TODO
        async with self.db_wrapper.db.execute("select count(*) from hints") as cursor:
            row = await cursor.fetchone()

        assert row is not None

        [count] = row
        return int(count)
