import os

from databases import Database


def create_database(default_db_path: str) -> Database:
    if os.environ.get("CHIA_DB_CONNECTION", None) is not None:
        return Database(os.environ.get("CHIA_DB_CONNECTION"))
    else:
        return Database("sqlite:///{}".format(default_db_path))