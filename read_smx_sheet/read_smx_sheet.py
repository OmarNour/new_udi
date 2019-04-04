import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import templates as tmp
import os
from dask import compute, delayed
from dask.diagnostics import ProgressBar
import datetime


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
start_time = datetime.datetime.now()

parallel_rmf = []
parallel_crf = []
parallel_templates = []
count_sources = 0

for smx in md.get_files_in_dir(pm.smx_path,pm.smx_ext):
    smx_file_name = os.path.splitext(smx)[0]

    System = pd.read_excel(pm.smx_path + smx, sheet_name='System')
    teradata_sources = System[System['Source type'] == 'TERADATA']
    count_sources = len(teradata_sources.index)
    Table_mapping = delayed(pd.read_excel)(pm.smx_path + smx, sheet_name='Table mapping')
    STG_tables = delayed(pd.read_excel)(pm.smx_path + smx, sheet_name='STG tables')
    BKEY = delayed(pd.read_excel)(pm.smx_path + smx, sheet_name='BKEY')
    Supplements = delayed(pd.read_excel)(pm.smx_path + smx, sheet_name='Supplements')

    for system_index, system_row in teradata_sources.iterrows():
        # print(row['Source system name'])
        run_time = datetime.datetime.now()
        source_name = system_row['Source system name']
        source_output_path = pm.output_path + smx_file_name + '/' + source_name

        delayed_rmf = delayed(md.remove_folder)(source_output_path)
        delayed_crf = delayed(md.create_folder)(source_output_path)

        parallel_rmf.append(delayed_rmf)
        parallel_crf.append(delayed_crf)

        parallel_templates.append(delayed(tmp.D000.d000)(source_output_path, source_name, Table_mapping, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D001.d001)(source_output_path, source_name, STG_tables))

        parallel_templates.append(delayed(tmp.D200.d200)(source_output_path, source_name, STG_tables, Supplements))
        parallel_templates.append(delayed(tmp.D210.d210)(source_output_path, source_name, STG_tables, Supplements))

        parallel_templates.append(delayed(tmp.D300.d300)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D320.d320)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D330.d330)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D340.d340)(source_output_path, source_name, STG_tables, BKEY))

        parallel_templates.append(delayed(tmp.D400.d400)(source_output_path, source_name, STG_tables, Supplements))
        parallel_templates.append(delayed(tmp.D410.d410)(source_output_path, source_name, STG_tables, Supplements))
        parallel_templates.append(delayed(tmp.D415.d415)(source_output_path, source_name, STG_tables, Supplements))
        parallel_templates.append(delayed(tmp.D420.d420)(source_output_path, source_name, STG_tables, Supplements))

if len(parallel_templates) > 0:
    compute(*parallel_rmf)
    compute(*parallel_crf)
    with ProgressBar():
        print("Start generating " + str(len(parallel_templates)) + " script for " + str(count_sources) + " sources")
        compute(*parallel_templates)
# print('####################                Total time:', datetime.datetime.now() - start_time, '      ####################')