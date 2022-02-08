from typing import Dict, List, Optional, Set


from chia.types.blockchain_format.coin import Coin
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.db_wrapper import DBWrapper
from chia.util.ints import uint32, uint64
from chia.util import dialect_utils
from chia.wallet.util.wallet_types import WalletType
from chia.wallet.wallet_coin_record import WalletCoinRecord
from databases import Database
from sqlalchemy import bindparam
from sqlalchemy.sql import text
import typing


class WalletCoinStore:
    """
    This object handles CoinRecords in DB used by wallet.
    """

    #Changed
    db_connection: Database
    # coin_record_cache keeps ALL coin records in memory. [record_name: record]
    coin_record_cache: Dict[bytes32, WalletCoinRecord]
    # unspent_coin_wallet_cache keeps ALL unspent coin records for wallet in memory [wallet_id: [record_name: record]]
    unspent_coin_wallet_cache: Dict[int, Dict[bytes32, WalletCoinRecord]]
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, wrapper: DBWrapper):
        self = cls()

        self.db_connection = wrapper.db
        self.db_wrapper = wrapper
        async with self.db_connection.connection() as connection:
            async with connection.transaction():
                await self.db_connection.execute(
                    (
                        "CREATE TABLE IF NOT EXISTS coin_record("
                        f"coin_name {dialect_utils.data_type('text-as-index', self.db_connection.url.dialect)} PRIMARY KEY,"
                        " confirmed_height bigint,"
                        " spent_height bigint,"
                        " spent int,"
                        " coinbase int,"
                        f" puzzle_hash {dialect_utils.data_type('text-as-index', self.db_connection.url.dialect)},"
                        " coin_parent text,"
                        f" amount {dialect_utils.data_type('blob', self.db_connection.url.dialect)},"
                        " wallet_type int,"
                        " wallet_id int)"
                    )
                )

                # Useful for reorg lookups
                await dialect_utils.create_index_if_not_exists(self.db_connection, 'coin_confirmed_height', 'coin_record', ['confirmed_height'])
                await dialect_utils.create_index_if_not_exists(self.db_connection, 'coin_spent_height', 'coin_record', ['spent_height'])
                await dialect_utils.create_index_if_not_exists(self.db_connection, 'coin_spent', 'coin_record', ['spent'])

                await dialect_utils.create_index_if_not_exists(self.db_connection, 'coin_puzzle_hash', 'coin_record', ['puzzle_hash'])

                await dialect_utils.create_index_if_not_exists(self.db_connection, 'wallet_type', 'coin_record', ['wallet_type'])

                await dialect_utils.create_index_if_not_exists(self.db_connection, 'wallet_id', 'coin_record', ['wallet_id'])

        self.coin_record_cache = {}
        self.unspent_coin_wallet_cache = {}
        await self.rebuild_wallet_cache()
        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM coin_record")

    async def rebuild_wallet_cache(self):
        # First update all coins that were reorged, then re-add coin_records
        all_coins = await self.get_all_coins()
        self.unspent_coin_wallet_cache = {}
        self.coin_record_cache = {}
        for coin_record in all_coins:
            name = coin_record.name()
            self.coin_record_cache[name] = coin_record
            if coin_record.spent is False:
                if coin_record.wallet_id not in self.unspent_coin_wallet_cache:
                    self.unspent_coin_wallet_cache[coin_record.wallet_id] = {}
                self.unspent_coin_wallet_cache[coin_record.wallet_id][name] = coin_record

    async def get_multiple_coin_records(self, coin_names: List[bytes32]) -> List[WalletCoinRecord]:
        """Return WalletCoinRecord(s) that have a coin name in the specified list"""
        if set(coin_names).issubset(set(self.coin_record_cache.keys())):
            return list(filter(lambda cr: cr.coin.name() in coin_names, self.coin_record_cache.values()))
        else:
            as_hexes = [cn.hex() for cn in coin_names]
            query = text('SELECT * from coin_record WHERE coin_name in :coin_names')
            query = query.bindparams(bindparam("coin_names", as_hexes, expanding=True))
            rows = await self.db_connection.fetch_all(query)

            return [self.coin_record_from_row(row) for row in rows]

    # Store CoinRecord in DB and ram cache
    async def add_coin_record(self, record: WalletCoinRecord) -> None:
        # update wallet cache
        name = record.name()
        self.coin_record_cache[name] = record
        if record.wallet_id in self.unspent_coin_wallet_cache:
            if record.spent and name in self.unspent_coin_wallet_cache[record.wallet_id]:
                self.unspent_coin_wallet_cache[record.wallet_id].pop(name)
            if not record.spent:
                self.unspent_coin_wallet_cache[record.wallet_id][name] = record
        else:
            if not record.spent:
                self.unspent_coin_wallet_cache[record.wallet_id] = {}
                self.unspent_coin_wallet_cache[record.wallet_id][name] = record
        row_to_insert = {
            "coin_name": name.hex(),
            "confirmed_height": int(record.confirmed_block_height),
            "spent_height": int(record.spent_block_height),
            "spent": int(record.spent),
            "coinbase": int(record.coinbase),
            "puzzle_hash": str(record.coin.puzzle_hash.hex()),
            "coin_parent": str(record.coin.parent_coin_info.hex()),
            "amount": bytes(record.coin.amount),
            "wallet_type": int(record.wallet_type),
            "wallet_id": int(record.wallet_id),
        }
        await self.db_connection.execute(
            dialect_utils.upsert_query('coin_record', ['coin_name'], row_to_insert.keys(), self.db_connection.url.dialect),
            row_to_insert
        )

    # Sometimes we realize that a coin is actually not interesting to us so we need to delete it
    async def delete_coin_record(self, coin_name: bytes32) -> None:
        if coin_name in self.coin_record_cache:
            coin_record = self.coin_record_cache.pop(coin_name)
            if coin_record.wallet_id in self.unspent_coin_wallet_cache:
                coin_cache = self.unspent_coin_wallet_cache[coin_record.wallet_id]
                if coin_name in coin_cache:
                    coin_cache.pop(coin_record.coin.name())

        await self.db_connection.execute("DELETE FROM coin_record WHERE coin_name=:coin_name", {"coin_name": coin_name.hex()})

    # Update coin_record to be spent in DB
    async def set_spent(self, coin_name: bytes32, height: uint32) -> WalletCoinRecord:
        current: Optional[WalletCoinRecord] = await self.get_coin_record(coin_name)
        assert current is not None
        # assert current.spent is False

        spent: WalletCoinRecord = WalletCoinRecord(
            current.coin,
            current.confirmed_block_height,
            height,
            True,
            current.coinbase,
            current.wallet_type,
            current.wallet_id,
        )

        await self.add_coin_record(spent)
        return spent

    def coin_record_from_row(self, row: typing.Mapping) -> WalletCoinRecord:
        coin = Coin(bytes32(bytes.fromhex(row[6])), bytes32(bytes.fromhex(row[5])), uint64.from_bytes(row[7]))
        return WalletCoinRecord(
            coin, uint32(row[1]), uint32(row[2]), bool(row[3]), bool(row[4]), WalletType(row[8]), row[9]
        )

    async def get_coin_record(self, coin_name: bytes32) -> Optional[WalletCoinRecord]:
        """Returns CoinRecord with specified coin id."""
        if coin_name in self.coin_record_cache:
            return self.coin_record_cache[coin_name]
        row = await self.db_connection.fetch_one("SELECT * from coin_record WHERE coin_name=:coin_name", {"coin_name": coin_name.hex()})

        if row is None:
            return None
        return self.coin_record_from_row(row)

    async def get_first_coin_height(self) -> Optional[uint32]:
        """Returns height of first confirmed coin"""
        row = await self.db_connection.fetch_one("SELECT MIN(confirmed_height) FROM coin_record;")

        if row is not None and row[0] is not None:
            return uint32(row[0])

        return None

    async def get_unspent_coins_at_height(self, height: Optional[uint32] = None) -> Set[WalletCoinRecord]:
        """
        Returns set of CoinRecords that have not been spent yet. If a height is specified,
        We can also return coins that were unspent at this height (but maybe spent later).
        Finally, the coins must be confirmed at the height or less.
        """
        if height is None:
            all_unspent = set()
            for name, coin_record in self.coin_record_cache.items():
                if coin_record.spent is False:
                    all_unspent.add(coin_record)
            return all_unspent
        else:
            all_unspent = set()
            for name, coin_record in self.coin_record_cache.items():
                if (
                    coin_record.spent is False
                    or coin_record.spent_block_height > height >= coin_record.confirmed_block_height
                ):
                    all_unspent.add(coin_record)
            return all_unspent

    async def get_unspent_coins_for_wallet(self, wallet_id: int) -> Set[WalletCoinRecord]:
        """Returns set of CoinRecords that have not been spent yet for a wallet."""
        if wallet_id in self.unspent_coin_wallet_cache:
            wallet_coins: Dict[bytes32, WalletCoinRecord] = self.unspent_coin_wallet_cache[wallet_id]
            return set(wallet_coins.values())
        else:
            return set()

    async def get_all_coins(self) -> Set[WalletCoinRecord]:
        """Returns set of all CoinRecords."""
        rows = await self.db_connection.fetch_all("SELECT * from coin_record")

        return set(self.coin_record_from_row(row) for row in rows)

    async def get_coins_to_check(self, check_height) -> Set[WalletCoinRecord]:
        """Returns set of all CoinRecords."""
        rows = await self.db_connection.fetch_all(
            "SELECT * from coin_record where spent_height=0 or spent_height>:min_spent_height or confirmed_height>:min_confirmed_height",
            {
                "min_spent_height": int(check_height),
                "min_confirmed_height": int(check_height),
            }
        )

        return set(self.coin_record_from_row(row) for row in rows)

    # Checks DB and DiffStores for CoinRecords with puzzle_hash and returns them
    async def get_coin_records_by_puzzle_hash(self, puzzle_hash: bytes32) -> List[WalletCoinRecord]:
        """Returns a list of all coin records with the given puzzle hash"""
        rows = await self.db_connection.fetch_all("SELECT * from coin_record WHERE puzzle_hash=:puzzle_hash", {"puzzle_hash": puzzle_hash.hex()})

        return [self.coin_record_from_row(row) for row in rows]

    # Checks DB and DiffStores for CoinRecords with parent_coin_info and returns them
    async def get_coin_records_by_parent_id(self, parent_coin_info: bytes32) -> List[WalletCoinRecord]:
        """Returns a list of all coin records with the given parent id"""
        rows = await self.db_connection.fetch_all(
            "SELECT * from coin_record WHERE coin_parent=:coin_parent", {"coin_parent": parent_coin_info.hex()}
        )

        return [self.coin_record_from_row(row) for row in rows]

    async def rollback_to_block(self, height: int):
        """
        Rolls back the blockchain to block_index. All blocks confirmed after this point
        are removed from the LCA. All coins confirmed after this point are removed.
        All coins spent after this point are set to unspent. Can be -1 (rollback all)
        """
        # Delete from storage
        delete_queue: List[WalletCoinRecord] = []
        for coin_name, coin_record in self.coin_record_cache.items():
            if coin_record.spent_block_height > height:
                new_record = WalletCoinRecord(
                    coin_record.coin,
                    coin_record.confirmed_block_height,
                    uint32(0),
                    False,
                    coin_record.coinbase,
                    coin_record.wallet_type,
                    coin_record.wallet_id,
                )
                self.coin_record_cache[coin_record.coin.name()] = new_record
                if coin_record.wallet_id in self.unspent_coin_wallet_cache:
                    self.unspent_coin_wallet_cache[coin_record.wallet_id][coin_record.coin.name()] = new_record
            if coin_record.confirmed_block_height > height:
                delete_queue.append(coin_record)

        for coin_record in delete_queue:
            self.coin_record_cache.pop(coin_record.coin.name())
            if coin_record.wallet_id in self.unspent_coin_wallet_cache:
                coin_cache = self.unspent_coin_wallet_cache[coin_record.wallet_id]
                if coin_record.coin.name() in coin_cache:
                    coin_cache.pop(coin_record.coin.name())

        await self.db_connection.execute("DELETE FROM coin_record WHERE confirmed_height>:min_confirmed_height", {"min_confirmed_height": int(height)})
        await self.db_connection.execute(
            "UPDATE coin_record SET spent_height = 0, spent = 0 WHERE spent_height>:min_spent_height",
            {"min_spent_height": int(height)},
        )
