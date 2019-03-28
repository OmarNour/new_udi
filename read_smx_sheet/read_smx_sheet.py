import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import templates as tmp
import os



# file_name = 'ACA_phase_2_ECONOMIC_UNIT_SMX_06_03_2019.xlsx'



#

# BMAP = pd.read_excel(smx_path+file_name, sheet_name='BMAP')
# BMAP_values = pd.read_excel(smx_path+file_name, sheet_name='BMAP values')
# Core_tables = pd.read_excel(smx_path+file_name, sheet_name='Core tables')

# Column_mapping = pd.read_excel(smx_path+file_name, sheet_name='Column mapping')

# print(list(STG_tables))
# print(list(BKEY))
# print(list(BMAP_values))
# print(list(Table_mapping))
# print(list(System))


# print(os.listdir(smx_path))
for smx in md.get_files_in_dir(pm.smx_path,pm.smx_ext):
    smx_file_name = os.path.splitext(smx)[0]

    System = pd.read_excel(pm.smx_path + smx, sheet_name='System')
    Table_mapping = pd.read_excel(pm.smx_path + smx, sheet_name='Table mapping')
    STG_tables = pd.read_excel(pm.smx_path + smx, sheet_name='STG tables')
    BKEY = pd.read_excel(pm.smx_path + smx, sheet_name='BKEY')
    Supplements = pd.read_excel(pm.smx_path + smx, sheet_name='Supplements')

    for system_index, system_row in System[System['Source type'] == 'TERADATA'].iterrows():
        # print(row['Source system name'])
        source_name = system_row['Source system name']
        source_output_path = pm.output_path + smx_file_name + '/' + source_name

        md.remove_folder(source_output_path)
        md.create_folder(source_output_path)

        tmp.D000.d000(source_output_path, source_name, Table_mapping, STG_tables, BKEY)
        tmp.D001.d001(source_output_path, source_name, STG_tables)

        tmp.D200.d200(source_output_path, source_name, STG_tables, Supplements)
        tmp.D210.d210(source_output_path, source_name, STG_tables, Supplements)

        tmp.D300.d300(source_output_path, source_name, STG_tables, BKEY)
        tmp.D320.d320(source_output_path, source_name, STG_tables, BKEY)
        tmp.D330.d330(source_output_path, source_name, STG_tables, BKEY)
        tmp.D340.d340(source_output_path, source_name, STG_tables, BKEY)
