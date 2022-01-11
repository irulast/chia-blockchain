from databases import Database


async def lookup_db_version(db: Database) -> int:
    try:
        cursor = await db.execute("SELECT * from database_version")
        row = await cursor.fetchone()
        if row is not None and row[0] == 2:
            return 2
        else:
            return 1
    except Exception:
        # expects OperationalError('no such table: database_version')
        return 1
