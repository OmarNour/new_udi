import pandas as pd
import app_Lib.manage_directories as md
import parameters.parameters as pm
import templates as tmp
import os
from dask import compute, delayed
from dask.diagnostics import ProgressBar
import datetime
import app_Lib.functions as funcs


start_time = datetime.datetime.now()
parallel_rmf = []
parallel_crf = []
parallel_templates = []
count_sources = 0
count_smx = 0

for smx in md.get_files_in_dir(pm.smx_path,pm.smx_ext):
    count_smx = count_smx + 1
    smx_file_path = pm.smx_path + smx
    smx_file_name = os.path.splitext(smx)[0]

    System = pd.read_excel(smx_file_path, sheet_name='System')
    teradata_sources = System[System['Source type'] == 'TERADATA']
    count_sources = count_sources + len(teradata_sources.index)

    Supplements = delayed(pd.read_excel)(smx_file_path, sheet_name='Supplements')
    Table_mapping = delayed(pd.read_excel)(smx_file_path, sheet_name='Table mapping')
    BKEY = delayed(pd.read_excel)(smx_file_path, sheet_name='BKEY')
    STG_tables = delayed(pd.read_excel)(smx_file_path, sheet_name='STG tables')
    STG_tables = delayed(funcs.rename_sheet_reserved_word)(STG_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])

    for system_index, system_row in teradata_sources.iterrows():
        run_time = datetime.datetime.now()
        source_name = system_row['Source system name']
        source_output_path = pm.output_path + smx_file_name + '/' + source_name

        delayed_rmf = delayed(md.remove_folder)(source_output_path)
        delayed_crf = delayed(md.create_folder)(source_output_path)

        parallel_rmf.append(delayed_rmf)
        parallel_crf.append(delayed_crf)

        parallel_templates.append(delayed(tmp.D000.d000)(source_output_path, source_name, Table_mapping, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D001.d001)(source_output_path, source_name, STG_tables))

        parallel_templates.append(delayed(tmp.D200.d200)(source_output_path, source_name, STG_tables))
        parallel_templates.append(delayed(tmp.D210.d210)(source_output_path, source_name, STG_tables))

        parallel_templates.append(delayed(tmp.D300.d300)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D320.d320)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D330.d330)(source_output_path, source_name, STG_tables, BKEY))
        parallel_templates.append(delayed(tmp.D340.d340)(source_output_path, source_name, STG_tables, BKEY))

        parallel_templates.append(delayed(tmp.D400.d400)(source_output_path, source_name, STG_tables))
        parallel_templates.append(delayed(tmp.D410.d410)(source_output_path, source_name, STG_tables))
        parallel_templates.append(delayed(tmp.D415.d415)(source_output_path, source_name, STG_tables))
        parallel_templates.append(delayed(tmp.D420.d420)(source_output_path, source_name, STG_tables, BKEY))

if len(parallel_templates) > 0:
    compute(*parallel_rmf)
    compute(*parallel_crf)
    with ProgressBar():
        print("Start generating " + str(len(parallel_templates)) + " script for " + str(count_sources) + " sources from " + str(count_smx) + " smx files")
        compute(*parallel_templates)