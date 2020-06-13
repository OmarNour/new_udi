from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator


@Logging_decorator
def data_mart_DDL(cf,source_name,source_output_path,STG_tables, Data_types):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    stg_tables_df = funcs.get_sama_stg_tables(STG_tables, None)

    for stg_tables_df_index, stg_tables_df_row in stg_tables_df.iterrows():
        Table_name = stg_tables_df_row['Table Name']
        create_stg_table = "create multiset table " + str(stg_tables_df_row['Schema Name']) + "." + Table_name + "\n" + "(\n"

        STG_table_columns = funcs.get_sama_stg_table_columns(STG_tables, source_name, Table_name)
        pi_columns = ""
        partition_columns = ""
        for STG_table_columns_index, STG_table_columns_row in STG_table_columns.iterrows():
            Column_name = STG_table_columns_row['Column Name']
            comma = ',' if STG_table_columns_index > 0 else ' '
            comma_Column_name = comma + Column_name

            for data_type_index,data_type_row in Data_types.iterrows():
                if data_type_row['Source data type'] == STG_table_columns_row['Source Data Type']:
                 Data_type = str(data_type_row['TERADATA data type'])

            character_set = " CHARACTER SET UNICODE NOT CASESPECIFIC " if "CHAR" in Data_type.upper() or "VARCHAR" in Data_type.upper() else ""
            not_null = " not null " if STG_table_columns_row['Teradata PK'].upper() == 'Y' else " "

            create_stg_table = create_stg_table + comma_Column_name + " " + Data_type + character_set + not_null + "\n"

            if STG_table_columns_row['Teradata PK'].upper() == 'Y':
                pi_columns = pi_columns + ',' + Column_name if pi_columns != "" else Column_name
            if STG_table_columns_row['Teradata partition'].upper() == 'Y':
                partition_columns = partition_columns + ',' + Column_name if partition_columns != "" else Column_name

        if pi_columns != "" and partition_columns == "":
            Primary_Index = ")Unique Primary Index (" + pi_columns + ")\n"
        elif pi_columns != "" and partition_columns != "":
            Primary_Index = ")Primary Index (" + pi_columns + ")\n"
        else:
            Primary_Index = ")"

        if partition_columns != "":
            partition_by = "Partition by (" + partition_columns + ")\n"
        else:
            partition_by = " "

        create_stg_table = create_stg_table + Primary_Index + partition_by
        create_stg_table = create_stg_table + ";\n\n"
        f.write(create_stg_table)

    f.close()
