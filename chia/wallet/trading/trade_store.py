from typing import List, Optional

from databases import Database

from chia.types.blockchain_format.sized_bytes import bytes32
from chia.types.mempool_inclusion_status import MempoolInclusionStatus
from chia.util.db_wrapper import DBWrapper
from chia.util.errors import Err
from chia.util.ints import uint8, uint32
from chia.util import dialect_utils
from chia.wallet.trade_record import TradeRecord
from chia.wallet.trading.trade_status import TradeStatus


class TradeStore:
    """
    TradeStore stores trading history.
    """

    db_connection: Database
    cache_size: uint32
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper, cache_size: uint32 = uint32(600000)):
        self = cls()

        self.cache_size = cache_size
        self.db_wrapper = db_wrapper
        self.db_connection = db_wrapper.db
        await self.db_connection.execute(
            (
                "CREATE TABLE IF NOT EXISTS trade_records("
                f" trade_record {'blob' if self.db_connection.url.dialect == 'sqlite' else 'bytea'},"
                " trade_id text PRIMARY KEY,"
                " status int,"
                " confirmed_at_index int,"
                " created_at_time bigint,"
                " sent int)"
            )
        )

        await self.db_connection.execute(
            "CREATE INDEX IF NOT EXISTS trade_confirmed_index on trade_records(confirmed_at_index)"
        )
        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS trade_status on trade_records(status)")
        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS trade_id on trade_records(trade_id)")

        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM trade_records")

    async def add_trade_record(self, record: TradeRecord, in_transaction) -> None:
        """
        Store TradeRecord into DB
        """
        if not in_transaction:
            await self.db_wrapper.lock.acquire()
        try:
            row_to_insert = {
                "trade_record": bytes(record),
                "trade_id": record.trade_id.hex(),
                "status": record.status,
                "confirmed_at_index": record.confirmed_at_index,
                "created_at_time": record.created_at_time,
                "sent": record.sent,
            }
            await self.db_connection.execute(
                dialect_utils.upsert_query('trade_records', ['trade_id'], row_to_insert.keys(), self.db_connection.url.dialect),
                row_to_insert
            )
        finally:
            if not in_transaction:
                self.db_wrapper.lock.release()

    async def set_status(self, trade_id: bytes32, status: TradeStatus, in_transaction: bool, index: uint32 = uint32(0)):
        """
        Updates the status of the trade
        """
        current: Optional[TradeRecord] = await self.get_trade_record(trade_id)
        if current is None:
            return None
        confirmed_at_index = current.confirmed_at_index
        if index != 0:
            confirmed_at_index = index
        tx: TradeRecord = TradeRecord(
            confirmed_at_index=confirmed_at_index,
            accepted_at_time=current.accepted_at_time,
            created_at_time=current.created_at_time,
            my_offer=current.my_offer,
            sent=current.sent,
            spend_bundle=current.spend_bundle,
            tx_spend_bundle=current.tx_spend_bundle,
            additions=current.additions,
            removals=current.removals,
            trade_id=current.trade_id,
            status=uint32(status.value),
            sent_to=current.sent_to,
        )
        await self.add_trade_record(tx, in_transaction)

    async def increment_sent(
        self,
        id: bytes32,
        name: str,
        send_status: MempoolInclusionStatus,
        err: Optional[Err],
    ) -> bool:
        """
        Updates trade sent count (Full Node has received spend_bundle and sent ack).
        """

        current: Optional[TradeRecord] = await self.get_trade_record(id)
        if current is None:
            return False

        sent_to = current.sent_to.copy()

        err_str = err.name if err is not None else None
        append_data = (name, uint8(send_status.value), err_str)

        # Don't increment count if it's already sent to this peer
        if append_data in sent_to:
            return False

        sent_to.append(append_data)

        tx: TradeRecord = TradeRecord(
            confirmed_at_index=current.confirmed_at_index,
            accepted_at_time=current.accepted_at_time,
            created_at_time=current.created_at_time,
            my_offer=current.my_offer,
            sent=uint32(current.sent + 1),
            spend_bundle=current.spend_bundle,
            tx_spend_bundle=current.tx_spend_bundle,
            additions=current.additions,
            removals=current.removals,
            trade_id=current.trade_id,
            status=current.status,
            sent_to=sent_to,
        )

        await self.add_trade_record(tx, False)
        return True

    async def set_not_sent(self, id: bytes32):
        """
        Updates trade sent count to 0.
        """

        current: Optional[TradeRecord] = await self.get_trade_record(id)
        if current is None:
            return None

        tx: TradeRecord = TradeRecord(
            confirmed_at_index=current.confirmed_at_index,
            accepted_at_time=current.accepted_at_time,
            created_at_time=current.created_at_time,
            my_offer=current.my_offer,
            sent=uint32(0),
            spend_bundle=current.spend_bundle,
            tx_spend_bundle=current.tx_spend_bundle,
            additions=current.additions,
            removals=current.removals,
            trade_id=current.trade_id,
            status=uint32(TradeStatus.PENDING_CONFIRM.value),
            sent_to=[],
        )

        await self.add_trade_record(tx, False)

    async def get_trade_record(self, trade_id: bytes32) -> Optional[TradeRecord]:
        """
        Checks DB for TradeRecord with id: id and returns it.
        """
        row = await self.db_connection.fetch_one("SELECT * from trade_records WHERE trade_id=:trade_id", {"trade_id": trade_id.hex()})
        if row is not None:
            record = TradeRecord.from_bytes(row[0])
            return record
        return None

    async def get_trade_record_with_status(self, status: TradeStatus) -> List[TradeRecord]:
        """
        Checks DB for TradeRecord with id: id and returns it.
        """
        rows = await self.db_connection.fetch_all("SELECT * from trade_records WHERE status=:status", {"status": status.value})
        records = []
        for row in rows:
            record = TradeRecord.from_bytes(row[0])
            records.append(record)

        return records

    async def get_not_sent(self) -> List[TradeRecord]:
        """
        Returns the list of trades that have not been received by full node yet.
        """

        rows = await self.db_connection.fetch_all(
            "SELECT * from trade_records WHERE sent<:sent and confirmed=:confirmed",
            {
                "sent": 4,
                "confirmed": 0,
            }
        )
        records = []
        for row in rows:
            record = TradeRecord.from_bytes(row[0])
            records.append(record)

        return records

    async def get_all_unconfirmed(self) -> List[TradeRecord]:
        """
        Returns the list of all trades that have not yet been confirmed.
        """

        rows = await self.db_connection.fetch_all("SELECT * from trade_records WHERE confirmed=confirmed", {"confirmed": 0})
        records = []

        for row in rows:
            record = TradeRecord.from_bytes(row[0])
            records.append(record)

        return records

    async def get_all_trades(self) -> List[TradeRecord]:
        """
        Returns all stored trades.
        """

        rows = await self.db_connection.fetch_all("SELECT * from trade_records")
        records = []

        for row in rows:
            record = TradeRecord.from_bytes(row[0])
            records.append(record)

        return records

    async def get_trades_above(self, height: uint32) -> List[TradeRecord]:
        rows = await self.db_connection.fetch_all("SELECT * from trade_records WHERE confirmed_at_index>:min_confirmed_at_index", {"min_confirmed_at_index": height})
        records = []

        for row in rows:
            record = TradeRecord.from_bytes(row[0])
            records.append(record)

        return records

    async def rollback_to_block(self, block_index):

        # Delete from storage
        await self.db_connection.execute(
            "DELETE FROM trade_records WHERE confirmed_at_index>:min_confirmed_at_index", {"min_confirmed_at_index": block_index}
        )
