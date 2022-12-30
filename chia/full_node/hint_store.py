from typing import List, Tuple, Any, Dict
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.db_wrapper import DBWrapper2
import logging

log = logging.getLogger(__name__)


class HintStore:
    db_wrapper: DBWrapper2

    @classmethod
    async def create(cls, db_wrapper: DBWrapper2):
        self = cls()
        self.db_wrapper = db_wrapper

        async with self.db_wrapper.write_db() as conn:
            if self.db_wrapper.db_version == 2:
                await conn.execute("CREATE TABLE IF NOT EXISTS hints(coin_id blob, hint blob, UNIQUE (coin_id, hint))")
            else:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS hints(id INTEGER PRIMARY KEY AUTOINCREMENT, coin_id blob, hint blob)"
                )
            await conn.execute("CREATE INDEX IF NOT EXISTS hint_index on hints(hint)")
        return self

    async def get_coin_ids(self, hint: bytes) -> List[bytes32]:
        async with self.db_wrapper.read_db() as conn:
            cursor = await conn.execute("SELECT coin_id from hints WHERE hint=?", (hint,))
            rows = await cursor.fetchall()
            await cursor.close()
        coin_ids = []
        for row in rows:
            coin_ids.append(row[0])
        return coin_ids

    async def get_coin_ids_by_hints(self, hints: List[bytes]) -> List[bytes32]:
        hints = list(hints)

        if len(hints) == 0:
            return []

        hints_db: Tuple[Any, ...]
        if self.db_wrapper.db_version == 2:
            hints_db = tuple(hints)
        else:
            hints_db = tuple([hint.hex() for hint in hints])

        async with self.db_wrapper.read_db() as conn:
            cursor = await conn.execute(f'SELECT coin_id FROM hints WHERE hint in ({"?," * (len(hints) - 1)}?)', hints_db)
            rows = await cursor.fetchall()
            await cursor.close()
        coin_ids = []
        for row in rows:
            coin_ids.append(row[0])
        return coin_ids
    
    async def get_hints_for_coin_ids(self, coin_ids: List[bytes32]) -> Dict[bytes32, bytes]:
        coin_ids = list(coin_ids)

        if len(coin_ids) == 0:
            return []

        coin_ids_db: Tuple[Any, ...]
        if self.db_wrapper.db_version == 2:
            coin_ids_db = tuple(coin_ids)
        else:
            coin_ids_db = tuple([coin_id.hex() for coin_id in coin_ids])

        async with self.db_wrapper.read_db() as conn:
            cursor = await conn.execute(f'SELECT coin_id, hint FROM hints WHERE coin_id in ({"?," * (len(coin_ids) - 1)}?)', coin_ids_db)
            rows = await cursor.fetchall()
            await cursor.close()
        coin_id_hint_dict = dict()
        for row in rows:
            coin_id_hint_dict[row[0]] = row[1]
        return coin_id_hint_dict

    async def add_hints(self, coin_hint_list: List[Tuple[bytes32, bytes]]) -> None:
        async with self.db_wrapper.write_db() as conn:
            if self.db_wrapper.db_version == 2:
                cursor = await conn.executemany(
                    "INSERT OR IGNORE INTO hints VALUES(?, ?)",
                    coin_hint_list,
                )
            else:
                cursor = await conn.executemany(
                    "INSERT INTO hints VALUES(?, ?, ?)",
                    [(None,) + record for record in coin_hint_list],
                )
            await cursor.close()

    async def count_hints(self) -> int:
        async with self.db_wrapper.read_db() as conn:
            async with conn.execute("select count(*) from hints") as cursor:
                row = await cursor.fetchone()

        assert row is not None

        [count] = row
        return int(count)
