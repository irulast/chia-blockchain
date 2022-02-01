from pathlib import Path

from databases import Database
import tempfile
import logging
log = logging.getLogger(__name__)

class TempFileDatabase:
    def __init__(self):
        self.db_path = Path(tempfile.NamedTemporaryFile().name)
        if self.db_path.exists():
            self.db_path.unlink()
        self.connection = Database("sqlite:///{}".format(str(self.db_path)))
    
    async def disconnect(self):
        await self.connection.disconnect()
        self.db_path.unlink()