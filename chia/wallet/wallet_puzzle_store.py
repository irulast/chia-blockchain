import asyncio
import logging
from typing import List, Optional, Set, Tuple

from databases import Database
from blspy import G1Element

from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.db_wrapper import DBWrapper
from chia.util.ints import uint32
from chia.util import dialect_utils
from chia.wallet.derivation_record import DerivationRecord
from chia.wallet.util.wallet_types import WalletType

log = logging.getLogger(__name__)


class WalletPuzzleStore:
    """
    WalletPuzzleStore keeps track of all generated puzzle_hashes and their derivation path / wallet.
    This is only used for HD wallets where each address is derived from a public key. Otherwise, use the
    WalletInterestedStore to keep track of puzzle hashes which we are interested in.
    """

    # Changed
    db_connection: Database
    lock: asyncio.Lock
    cache_size: uint32
    all_puzzle_hashes: Set[bytes32]
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper, cache_size: uint32 = uint32(600000)):
        self = cls()

        self.cache_size = cache_size

        self.db_wrapper = db_wrapper
        self.db_connection = self.db_wrapper.db
        await self.db_connection.execute(
            (
                "CREATE TABLE IF NOT EXISTS derivation_paths("
                "derivation_index int,"
                " pubkey text,"
                " puzzle_hash text PRIMARY KEY,"
                " wallet_type int,"
                " wallet_id int,"
                f" used {dialect_utils.data_type('tinyint', self.db_wrapper.db.url.dialect)})"
            )
        )
        await self.db_connection.execute(
            "CREATE INDEX IF NOT EXISTS derivation_index_index on derivation_paths(derivation_index)"
        )

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS ph on derivation_paths(puzzle_hash)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS pubkey on derivation_paths(pubkey)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS wallet_type on derivation_paths(wallet_type)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS wallet_id on derivation_paths(wallet_id)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS used on derivation_paths(wallet_type)")

        # Lock
        self.lock = asyncio.Lock()  # external
        await self._init_cache()
        return self

    async def close(self):
        await self.db_connection.disconnect()

    async def _init_cache(self):
        self.all_puzzle_hashes = await self.get_all_puzzle_hashes()

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM derivation_paths")

    async def add_derivation_paths(self, records: List[DerivationRecord], in_transaction=False) -> None:
        """
        Insert many derivation paths into the database.
        """

        if not in_transaction:
            await self.db_wrapper.lock.acquire()
        try:
            sql_records = []
            for record in records:
                self.all_puzzle_hashes.add(record.puzzle_hash)
                sql_records.append(
                    {
                        "derivation_index": record.index,
                        "pubkey": bytes(record.pubkey).hex(),
                        "puzzle_hash": record.puzzle_hash.hex(),
                        "wallet_type": record.wallet_type,
                        "wallet_id": record.wallet_id,
                        "used": 0,
                    }
                )
            if len(sql_records) > 0:
                await self.db_connection.execute_many(
                    dialect_utils.upsert_query('derivation_paths', ['puzzle_hash'], sql_records[0].keys(), self.db_connection.url.dialect),
                    sql_records,
                )

        finally:
            if not in_transaction:
                self.db_wrapper.lock.release()

    async def get_derivation_record(self, index: uint32, wallet_id: uint32) -> Optional[DerivationRecord]:
        """
        Returns the derivation record by index and wallet id.
        """
        row = await self.db_connection.fetch_one(
            "SELECT * FROM derivation_paths WHERE derivation_index=:derivation_index and wallet_id=:wallet_id;",
            {
                "derivation_index": index,
                "wallet_id": wallet_id,
            }
        )

        if row is not None and row[0] is not None:
            return DerivationRecord(
                uint32(row[0]),
                bytes32.fromhex(row[2]),
                G1Element.from_bytes(bytes.fromhex(row[1])),
                WalletType(row[3]),
                uint32(row[4]),
            )

        return None

    async def get_derivation_record_for_puzzle_hash(self, puzzle_hash: bytes32) -> Optional[DerivationRecord]:
        """
        Returns the derivation record by index and wallet id.
        """
        row = await self.db_connection.fetch_one(
            "SELECT * FROM derivation_paths WHERE puzzle_hash=:puzzle_hash;",
            {"puzzle_hash": puzzle_hash.hex()},
        )

        if row is not None and row[0] is not None:
            return DerivationRecord(
                uint32(row[0]),
                bytes32.fromhex(row[2]),
                G1Element.from_bytes(bytes.fromhex(row[1])),
                WalletType(row[3]),
                uint32(row[4]),
            )

        return None

    async def set_used_up_to(self, index: uint32, in_transaction=False) -> None:
        """
        Sets a derivation path to used so we don't use it again.
        """

        if not in_transaction:
            await self.db_wrapper.lock.acquire()
        try:
            await self.db_connection.execute(
                "UPDATE derivation_paths SET used=1 WHERE derivation_index<=:derivation_index",
                {"derivation_index": index},
            )
        finally:
            if not in_transaction:
                self.db_wrapper.lock.release()

    async def puzzle_hash_exists(self, puzzle_hash: bytes32) -> bool:
        """
        Checks if passed puzzle_hash is present in the db.
        """

        row = await self.db_connection.fetch_one(
            "SELECT * from derivation_paths WHERE puzzle_hash=:puzzle_hash", {"puzzle_hash": puzzle_hash.hex()}
        )

        return row is not None

    async def one_of_puzzle_hashes_exists(self, puzzle_hashes: List[bytes32]) -> bool:
        """
        Checks if one of the passed puzzle_hashes is present in the db.
        """
        if len(puzzle_hashes) < 1:
            return False

        for ph in puzzle_hashes:
            if ph in self.all_puzzle_hashes:
                return True

        return False

    async def index_for_pubkey(self, pubkey: G1Element) -> Optional[uint32]:
        """
        Returns derivation paths for the given pubkey.
        Returns None if not present.
        """

        row = await self.db_connection.fetch_one(
            "SELECT * from derivation_paths WHERE pubkey=:pubkey", {"pubkey": bytes(pubkey).hex()}
        )

        if row is not None:
            return uint32(row[0])

        return None

    async def index_for_puzzle_hash(self, puzzle_hash: bytes32) -> Optional[uint32]:
        """
        Returns the derivation path for the puzzle_hash.
        Returns None if not present.
        """
        row = await self.db_connection.fetch_one(
            "SELECT * from derivation_paths WHERE puzzle_hash=:puzzle_hash", {"puzzle_hash": puzzle_hash.hex()}
        )

        if row is not None:
            return uint32(row[0])

        return None

    async def index_for_puzzle_hash_and_wallet(self, puzzle_hash: bytes32, wallet_id: uint32) -> Optional[uint32]:
        """
        Returns the derivation path for the puzzle_hash.
        Returns None if not present.
        """
        row = await self.db_connection.fetch_one(
            "SELECT * from derivation_paths WHERE puzzle_hash=:puzzle_hash and wallet_id=:wallet_id;",
            {
                "puzzle_hash": puzzle_hash.hex(),
                "wallet_id": wallet_id,
            }
        )

        if row is not None:
            return uint32(row[0])

        return None

    async def wallet_info_for_puzzle_hash(self, puzzle_hash: bytes32) -> Optional[Tuple[uint32, WalletType]]:
        """
        Returns the derivation path for the puzzle_hash.
        Returns None if not present.
        """

        row = await self.db_connection.fetch_one(
            "SELECT * from derivation_paths WHERE puzzle_hash=:puzzle_hash", {"puzzle_hash": puzzle_hash.hex()}
        )

        if row is not None:
            return row[4], WalletType(row[3])

        return None

    async def get_all_puzzle_hashes(self) -> Set[bytes32]:
        """
        Return a set containing all puzzle_hashes we generated.
        """

        rows = await self.db_connection.fetch_all("SELECT * from derivation_paths")

        result: Set[bytes32] = set()

        for row in rows:
            result.add(bytes32(bytes.fromhex(row[2])))

        return result

    async def get_last_derivation_path(self) -> Optional[uint32]:
        """
        Returns the last derivation path by derivation_index.
        """

        row = await self.db_connection.fetch_one("SELECT MAX(derivation_index) FROM derivation_paths;")

        if row is not None and row[0] is not None:
            return uint32(row[0])

        return None

    async def get_last_derivation_path_for_wallet(self, wallet_id: int) -> Optional[uint32]:
        """
        Returns the last derivation path by derivation_index.
        """

        row = await self.db_connection.fetch_one(
            f"SELECT MAX(derivation_index) FROM derivation_paths WHERE wallet_id={wallet_id};"
        )

        if row is not None and row[0] is not None:
            return uint32(row[0])

        return None

    async def get_current_derivation_record_for_wallet(self, wallet_id: uint32) -> Optional[DerivationRecord]:
        """
        Returns the current derivation record by derivation_index.
        """

        row = await self.db_connection.fetch_one(
            f"SELECT MAX(derivation_index) FROM derivation_paths WHERE wallet_id={wallet_id} and used=1;"
        )

        if row is not None and row[0] is not None:
            index = uint32(row[0])
            return await self.get_derivation_record(index, wallet_id)

        return None

    async def get_unused_derivation_path(self) -> Optional[uint32]:
        """
        Returns the first unused derivation path by derivation_index.
        """
        row = await self.db_connection.fetch_one("SELECT MIN(derivation_index) FROM derivation_paths WHERE used=0;")

        if row is not None and row[0] is not None:
            return uint32(row[0])

        return None
