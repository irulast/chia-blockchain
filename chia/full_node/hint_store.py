from __future__ import annotations

import dataclasses
import logging
from typing import List, Tuple, Any, Dict, Optional

import typing_extensions

from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.db_wrapper import DBWrapper2

log = logging.getLogger(__name__)


@typing_extensions.final
@dataclasses.dataclass
class HintStore:
    db_wrapper: DBWrapper2

    @classmethod
    async def create(cls, db_wrapper: DBWrapper2) -> HintStore:
        self = HintStore(db_wrapper)

        async with self.db_wrapper.writer_maybe_transaction() as conn:
            log.info("DB: Creating hint store tables and indexes.")
            if self.db_wrapper.db_version == 2:
                await conn.execute("CREATE TABLE IF NOT EXISTS hints(coin_id blob, hint blob, UNIQUE (coin_id, hint))")
            else:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS hints(id INTEGER PRIMARY KEY AUTOINCREMENT, coin_id blob, hint blob)"
                )
            log.info("DB: Creating index hint_index")
            await conn.execute("CREATE INDEX IF NOT EXISTS hint_index on hints(hint)")
        return self

    async def get_coin_ids(self, hint: bytes, *, max_items: int = 50000) -> List[bytes32]:
        async with self.db_wrapper.reader_no_transaction() as conn:
            cursor = await conn.execute("SELECT coin_id from hints WHERE hint=? LIMIT ?", (hint, max_items))
            rows = await cursor.fetchall()
            await cursor.close()
        return [bytes32(row[0]) for row in rows]

    async def get_coin_ids_by_hints(self, hints: List[bytes]) -> List[bytes32]:
        hints = list(hints)

        if len(hints) == 0:
            return []

        hints_db = tuple(hints)

        async with self.db_wrapper.read_db() as conn:
            cursor = await conn.execute(f'SELECT coin_id FROM hints WHERE hint in ({"?," * (len(hints) - 1)}?)', hints_db)
            rows = await cursor.fetchall()
            await cursor.close()
        coin_ids = []
        for row in rows:
            coin_ids.append(row[0])
        return coin_ids

    async def get_coin_ids_by_hints_paginated(
        self,
        hints: List[bytes],
        page_size: int,
        last_id: Optional[bytes32] = None,
    ) -> Tuple[List[bytes32], Optional[bytes32], Optional[int]]:
        hints = list(hints)

        if len(hints) == 0:
            return []

        hints_db = tuple(hints)

        log = logging.getLogger(__name__)

        count_query = (
            "SELECT COUNT(*) as coin_count "
            "FROM hints "
            f'WHERE hint in ({"?," * (len(hints) - 1)}?) '
        )
        count_query_params = hints_db

        query = (
            f"SELECT coin_id FROM hints "
            f'WHERE hint in ({"?," * (len(hints) - 1)}?) '
            f"{'AND coin_id > ?' if last_id is not None else ''} "
            f"ORDER BY coin_id "
            f"LIMIT {page_size}"
        )
        params = hints_db
        if last_id is not None:
            params += (last_id,)

        async with self.db_wrapper.read_db() as conn:
            total_coin_count = None

            if last_id is None:
                async with conn.execute(
                    count_query,
                    count_query_params,
                ) as cursor:
                    count_row =  await cursor.fetchone()
                    total_coin_count = count_row[0]

            coin_ids = []
            next_last_id = last_id

            async with conn.execute(
                query,
                params,
            ) as cursor:
                for row in await cursor.fetchall():
                    coin_ids.append(row[0])

                if len(coin_ids) > 0:
                    next_last_id = coin_ids[len(coin_ids) - 1]

            return coin_ids, next_last_id, total_coin_count

    async def get_hints_for_coin_ids(self, coin_ids: List[bytes32]) -> Dict[bytes32, bytes]:
        coin_ids = list(coin_ids)

        if len(coin_ids) == 0:
            return []

        coin_ids_db = tuple(coin_ids)

        async with self.db_wrapper.read_db() as conn:
            cursor = await conn.execute(
                f'SELECT coin_id, hint FROM hints INDEXED BY sqlite_autoindex_hints_1 '
                f'WHERE coin_id in ({"?," * (len(coin_ids) - 1)}?)',
                coin_ids_db
            )
            rows = await cursor.fetchall()
            await cursor.close()
        coin_id_hint_dict = dict()
        for row in rows:
            coin_id_hint_dict[row[0]] = row[1]
        return coin_id_hint_dict

    async def add_hints(self, coin_hint_list: List[Tuple[bytes32, bytes]]) -> None:
        if len(coin_hint_list) == 0:
            return None

        async with self.db_wrapper.writer_maybe_transaction() as conn:
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
        async with self.db_wrapper.reader_no_transaction() as conn:
            async with conn.execute("select count(*) from hints") as cursor:
                row = await cursor.fetchone()

        assert row is not None

        [count] = row
        return int(count)
