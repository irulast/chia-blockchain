from enum import Enum
from typing import List

class SqlDialect(Enum):
    SQLITE = 'sqlite'
    POSTGRES = 'postgresql'

data_type_map = {
    'blob' : {
        SqlDialect.SQLITE: 'blob',
        SqlDialect.POSTGRES: 'bytea'
    },
    'tinyint': {
        SqlDialect.SQLITE: 'tinyint',
        SqlDialect.POSTGRES: 'smallint'
    }
}
def data_type(data_type: str, dialect: str):
    return data_type_map[data_type][SqlDialect(dialect)]


def upsert_query(table_name: str, primary_key_columns: List[str], columns: List[str], dialect: str):
    query_param_columns = map(lambda v: ':' + v, columns)
    
    if SqlDialect(dialect) == SqlDialect.SQLITE:
        return f"INSERT OR REPLACE INTO {table_name} VALUES({', '.join(query_param_columns)})"

    elif SqlDialect(dialect) == SqlDialect.POSTGRES:
        set_statements = []
        for col in columns:
            if col not in primary_key_columns:
                set_statements.append(f"{col} = :{col}")
        
        handle_conflict_str = (
            f"DO UPDATE SET {', '.join(set_statements)}"
            if len(columns) > 1
            else "DO NOTHING"
        )
        return (
             f"INSERT INTO {table_name}({', '.join(columns)}) VALUES({', '.join(query_param_columns)}) "
             f"ON CONFLICT ({', '.join(primary_key_columns)}) "
             f"{handle_conflict_str}"
         )
    else:
        raise Exception("Invalid or unsupported sql dialect")