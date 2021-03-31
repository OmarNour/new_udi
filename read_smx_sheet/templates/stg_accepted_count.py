from read_smx_sheet.app_Lib import functions as funcs
from read_smx_sheet.Logging_Decorator import Logging_decorator
from read_smx_sheet.parameters import parameters as pm
from read_smx_sheet.app_Lib import TransformDDL as TDDL


@Logging_decorator
def stg_accepted_count(cf, source_output_path, System, STG_tables, LOADING_TYPE):
    file_name = funcs.get_file_name(__file__)
    f = funcs.WriteFile(source_output_path, file_name, "sql")
    smx_path = cf.smx_path
    template_path = cf.templates_path + "/" + pm.compareSTGacccounts_template_filename
    template_string = ""
    try:
        REJ_TABLE_NAME = System['Rejection Table Name']
    except:
        REJ_TABLE_NAME = 'REJECTIONS_TERADATA'
    try:
        REJ_TABLE_RULE = System['Rejection Table Rules']
    except:
        REJ_TABLE_RULE = 'BUSINESS_RULES_TERADATA'
    try:
        source_DB = System['Source DB']
    except:
        source_DB = 'STG_ONLINE'

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
    for stg_tables_df_index, stg_tables_df_row in stg_table_names[(stg_table_names['Table name'] != REJ_TABLE_NAME) & (
            stg_table_names['Table name'] != REJ_TABLE_RULE)].iterrows():
        TABLE_NAME = stg_tables_df_row['Table name']
        TBL_PKs = TDDL.get_trgt_pk(STG_tables, TABLE_NAME)
        output_script = template_string.format(TABLE_NAME=TABLE_NAME,
                                               STG_DATABASE=cf.T_STG,
                                               source_DB=source_DB,
                                               LOADING_TYPE=LOADING_TYPE,
                                               REJ_TABLE_NAME=REJ_TABLE_NAME,
                                               REJ_TABLE_RULE=REJ_TABLE_RULE,
                                               TBL_PKs=TBL_PKs
                                               )
        seperation_line = '--------------------------------------------------------------------------------------------------------------------------------------------------------------------'
        output_script = output_script.upper() + '\n' + seperation_line + '\n'  + seperation_line + '\n'
        f.write(output_script.replace('Â', ' '))
    f.close()
