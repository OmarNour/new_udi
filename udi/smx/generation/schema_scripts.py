from __future__ import annotations

from config import DELETE_DATABASE_TEMPLATE, DROP_DATABASE_TEMPLATE
from functions import WriteFile, log_error_decorator
from model import Schema


@log_error_decorator()
def generate_schemas_ddl(smx: SMX):
    schema: Schema
    db_create_file = WriteFile(smx.current_scripts_path, 'create_schemas', 'sql')
    db_drop_file = WriteFile(smx.current_scripts_path, 'drop_schemas', 'sql')
    db_create_file.write(smx.MAIN_DATABASE_TEMPLATE + '\n')
    for schema in Schema.get_all_instances():
        ddl = schema.ddl
        delete_schema_ddl = DELETE_DATABASE_TEMPLATE.format(db_name=schema.schema_name)
        drop_schema_ddl = DROP_DATABASE_TEMPLATE.format(db_name=schema.schema_name)
        if ddl:
            db_drop_file.write(delete_schema_ddl + '\n' + drop_schema_ddl + '\n\n')
            db_create_file.write(ddl + '\n')
    db_drop_file.write(DROP_DATABASE_TEMPLATE.format(db_name=smx.MAIN_DB_NAME) + '\n')
    db_create_file.close()
    db_drop_file.close()
    grants_file = WriteFile(smx.current_scripts_path, 'grants', 'sql')
    grants_file.write(smx.GRANTS)
    grants_file.close()
