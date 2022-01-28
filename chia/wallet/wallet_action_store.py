from typing import List, Optional

from chia.util.db_wrapper import DBWrapper
from databases import Database
from chia.util.ints import uint32
from chia.wallet.util.wallet_types import WalletType
from chia.wallet.wallet_action import WalletAction
from databases import Database
from chia.util import dialect_utils


class WalletActionStore:
    """
    WalletActionStore keeps track of all wallet actions that require persistence.
    Used by Colored coins, Atomic swaps, Rate Limited, and Authorized payee wallets
    """

    # Changed
    db_connection: Database
    cache_size: uint32
    db_wrapper: DBWrapper

    @classmethod
    async def create(cls, db_wrapper: DBWrapper):
        self = cls()
        self.db_wrapper = db_wrapper
        self.db_connection = db_wrapper.db

        await self.db_connection.execute(
            (
                "CREATE TABLE IF NOT EXISTS action_queue("
                f"id INTEGER {dialect_utils.clause('AUTOINCREMENT', self.db_connection.url.dialect)}  PRIMARY KEY,"
                " name text,"
                " wallet_id int,"
                " wallet_type int,"
                " wallet_callback text,"
                " done int,"
                " data text)"
            )
        )

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS name on action_queue(name)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS wallet_id on action_queue(wallet_id)")

        await self.db_connection.execute("CREATE INDEX IF NOT EXISTS wallet_type on action_queue(wallet_type)")

        # await self.db_connection.commit()
        return self

    async def _clear_database(self):
        await self.db_connection.execute("DELETE FROM action_queue")


    async def get_wallet_action(self, id: int) -> Optional[WalletAction]:
        """
        Return a wallet action by id
        """

        row = await self.db_connection.fetch_one("SELECT * from action_queue WHERE id=:id", {"id": id})

        if row is None:
            return None

        return WalletAction(row[0], row[1], row[2], WalletType(row[3]), row[4], bool(row[5]), row[6])

    async def create_action(
        self, name: str, wallet_id: int, type: int, callback: str, done: bool, data: str, in_transaction: bool
    ):
        """
        Creates Wallet Action
        """
        if not in_transaction:
            await self.db_wrapper.lock.acquire()
        try:
            await self.db_connection.execute(
                "INSERT INTO action_queue VALUES(:id, :name, :wallet_id, :wallet_type, :wallet_callback, :done, :data)",
                {"id": None, "name":  name, "wallet_id":  wallet_id, "wallet_type":  type, "wallet_callback":  callback, "done":  done, "data":  data},
            )
        finally:
            if not in_transaction:
                self.db_wrapper.lock.release()

    async def action_done(self, action_id: int):
        """
        Marks action as done
        """
        action: Optional[WalletAction] = await self.get_wallet_action(action_id)
        assert action is not None
        async with self.db_wrapper.lock:
            await self.db_connection.execute(
                "Replace INTO action_queue VALUES(:id, :name, :wallet_id, :wallet_type, :wallet_callback, :done, :data)",
                {
                    "id": action.id,
                    "name": action.name,
                    "wallet_id": action.wallet_id,
                    "wallet_type": action.type.value,
                    "wallet_callback": action.wallet_callback,
                    "done": True,
                    "data": action.data,
                }
            )

    async def get_all_pending_actions(self) -> List[WalletAction]:
        """
        Returns list of all pending action
        """
        result: List[WalletAction] = []
        rows = await self.db_connection.fetch_all("SELECT * from action_queue WHERE done=:done", {"done": 0})

        if rows is None:
            return result

        for row in rows:
            action = WalletAction(row[0], row[1], row[2], WalletType(row[3]), row[4], bool(row[5]), row[6])
            result.append(action)

        return result

    async def get_action_by_id(self, id) -> Optional[WalletAction]:
        """
        Return a wallet action by id
        """

        row = await self.db_connection.fetch_one("SELECT * from action_queue WHERE id=:id", {"id": id})

        if row is None:
            return None

        return WalletAction(row[0], row[1], row[2], WalletType(row[3]), row[4], bool(row[5]), row[6])
