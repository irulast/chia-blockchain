import asyncio

from databases import Database


class DBWrapper:
    """
    This object handles HeaderBlocks and Blocks stored in DB used by wallet.
    """

    db: Database
    lock: asyncio.Lock
    allow_upgrades: bool
    db_version: int

    def __init__(self, connection: Database, allow_upgrades: bool = False, db_version: int = 1):
        self.db = connection
        self.allow_upgrades = allow_upgrades
        self.lock = asyncio.Lock()
        self.db_version = db_version
