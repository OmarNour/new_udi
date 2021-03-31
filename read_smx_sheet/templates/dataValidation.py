from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import TransformDDL as TDDL


@Logging_decorator
def dataValidation(cf, source_output_path, source_name, System, STG_tables,LOADING_TYPE):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    smx_path = cf.smx_path
    template_path = cf.templates_path + "/" + pm.dataValidation_template_filename
    template_string = ""
    try:
        REJ_TABLE_NAME = System['Rejection Table Name']
    except:
        REJ_TABLE_NAME = ''
    try:
        REJ_TABLE_RULE = System['Rejection Table Rules']
    except:
        REJ_TABLE_RULE = ''

    try:
        source_DB = System['Source DB']
    except:
        source_DB = ''

    try:
        template_file = open(template_path, "r")
    except:
        template_file = open(smx_path, "r")

    for i in template_file.readlines():
        if i != "":
            template_string = template_string + i
    stg_table_names = funcs.get_stg_tables(STG_tables)
    if LOADING_TYPE == 'ONLINE':
        LOADING_TYPE = 'STG_ONLINE'
    else:
        LOADING_TYPE = 'STG_LAYER'
    for stg_tables_df_index, stg_tables_df_row in stg_table_names[(stg_table_names['Table name'] != REJ_TABLE_NAME) & (stg_table_names['Table name'] != REJ_TABLE_RULE)].iterrows():
        TABLE_NAME = stg_tables_df_row['Table name']
        TABLE_COLUMNS = funcs.get_stg_table_columns(STG_tables, source_name, TABLE_NAME)
        TBL_PKs = TDDL.get_trgt_pk(STG_tables, TABLE_NAME)
        IBM_STG_TABLE_COLUMNS = ""
        TERADATA_STG_TABLE_COLUMNS = ""
        COUNT_COLS = ''
        lengthh = len(TABLE_COLUMNS)
        for stg_tbl_index, stg_tbl_row in TABLE_COLUMNS.iterrows():
            align = '' if stg_tbl_index >= lengthh - 1 else '\n\t\t'
            IBM_STG_TABLE_COLUMNS += 'IBM_STG_TABLE.' + '"' + stg_tbl_row['Column name'] + '"' + ',' + align
            TERADATA_STG_TABLE_COLUMNS += 'TERADATA_STG_TABLE.' + '"' + stg_tbl_row['Column name'] + '"' + ',' + align
            COUNT_COLS += str(stg_tbl_index+1) + ','
        COUNT_COLS = COUNT_COLS[0:len(COUNT_COLS) - 1]
        IBM_STG_TABLE_COLUMNS = IBM_STG_TABLE_COLUMNS[0:len(IBM_STG_TABLE_COLUMNS) - 1]
        TERADATA_STG_TABLE_COLUMNS = TERADATA_STG_TABLE_COLUMNS[0:len(TERADATA_STG_TABLE_COLUMNS) - 1]
        output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                               IBM_STG_TABLE_COLUMNS=IBM_STG_TABLE_COLUMNS,
                                               TERADATA_STG_TABLE_COLUMNS=TERADATA_STG_TABLE_COLUMNS,
                                               STG_DATABASE=cf.T_STG,
                                               REJ_TABLE_NAME=REJ_TABLE_NAME,
                                               REJ_TABLE_RULE=REJ_TABLE_RULE,
                                               TBL_PKs=TBL_PKs,
                                               source_DB=source_DB,
                                               LOADING_TYPE=LOADING_TYPE,
                                               COUNT_COLS=COUNT_COLS
                                               )
        seperation_line = '--------------------------------------------------------------------------------------------------------------------------------------------------------------------'
        output_script = output_script.upper() + '\n' + seperation_line + '\n' + seperation_line + '\n'
        f.write(output_script.replace('Â', ' '))
    f.close()
