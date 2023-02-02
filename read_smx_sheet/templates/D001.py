from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def d001(cf, source_output_path, source_name, STG_tables):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    f.write("--delete from " + cf.GCFR_t + "." + cf.SOURCE_TABLES_LKP_table + " where SOURCE_NAME = '" + source_name + "';\n\n")
    stg_tables_df = funcs.get_stg_tables(STG_tables, source_name=None)
    for STG_tables_index, STG_tables_row in stg_tables_df.iterrows():
        Table_name = STG_tables_row['Table name']
        f.write("insert into " + cf.GCFR_t + "." + cf.SOURCE_TABLES_LKP_table + "(SOURCE_NAME, TABLE_NAME)\n")
        f.write(
            "SELECT " + "'" + source_name + "', '" + Table_name + "'" + " WHERE  NOT EXISTS (SELECT TABLE_NAME FROM " + cf.GCFR_t + "." + cf.SOURCE_TABLES_LKP_table + " WHERE SOURCE_NAME = " + "'" + source_name + "' );\n"
        )
        # f.write("VALUES ('" + source_name + "', '" + Table_name + "')" + ";\n")
        f.write("\n")
    f.close()
