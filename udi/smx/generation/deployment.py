from __future__ import annotations

import logging

from config import DROP_BEFORE_CREATE, DROP_TABLE_TEMPLATE
from functions import log_error_decorator, time_elapsed_decorator
from model import DataBaseEngine


@log_error_decorator()
@time_elapsed_decorator
def deploy():
    for db in DataBaseEngine.get_all_instances():
        db.conn = db.get_connection()
        if db.conn:
            for schema in db.schemas:
                for table in schema.kind_T_tables:
                    if DROP_BEFORE_CREATE:
                        db.execute(DROP_TABLE_TEMPLATE.format(schema_name=schema.schema_name, table_name=table.table_name))
                    db.execute(table.ddl)
            for schema in db.schemas:
                for table in schema.kind_V_tables:
                    cursor = db.execute(table.ddl)
                    if cursor:
                        db.execute(f'select top 1 * from {schema.schema_name}.{table.table_name}')
            if db.conn:
                db.conn.close()
        else:
            logging.warning('Unable to deploy scripts, failed to connect!')
