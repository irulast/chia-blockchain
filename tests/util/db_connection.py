from pathlib import Path
from chia.util.db_factory import create_database
from chia.util.db_wrapper import DBWrapper
import tempfile


class DBConnection:
    def __init__(self, db_version):
        self.db_version = db_version

    async def __aenter__(self) -> DBWrapper:
        self.db_path = Path(tempfile.NamedTemporaryFile().name)
        if self.db_path.exists():
            self.db_path.unlink()
        self.connection = create_database(str(self.db_path))
        await self.connection.connect()
        return DBWrapper(self.connection, False, self.db_version)

    async def __aexit__(self, exc_t, exc_v, exc_tb):
        await self.connection.disconnect()
        self.db_path.unlink()
