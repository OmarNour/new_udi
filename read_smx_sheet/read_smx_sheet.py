import os
import sys
sys.path.append(os.getcwd())
from app_Lib import manage_directories as md, functions as funcs
from parameters import parameters as pm
from dask import compute, delayed
from dask.diagnostics import ProgressBar
import traceback
from templates import gcfr, D002, D210, D300, D320, D420, D000, D001, D200, D330, D340, D400,D410,D415, D615, D600, D607, D608, D610,D620, D630, D640

def generate_scripts():
    parallel_remove_output_home_path = []
    parallel_create_output_home_path = []
    parallel_create_output_source_path = []
    parallel_templates = []
    count_sources = 0
    count_smx = 0

    print("Reading from: ", pm.smx_path)
    print(pm.smx_ext + " files:")
    try:
        for smx in md.get_files_in_dir(pm.smx_path,pm.smx_ext):
            count_smx = count_smx + 1
            smx_file_path = pm.smx_path + smx
            smx_file_name = os.path.splitext(smx)[0]
            print("\t"+smx_file_name)

            System = funcs.read_excel(smx_file_path, sheet_name='System')
            teradata_sources = System[System['Source type'] == 'TERADATA']
            count_sources = count_sources + len(teradata_sources.index)

            Supplements = delayed(funcs.read_excel)(smx_file_path, sheet_name='Supplements')

            Column_mapping = delayed(funcs.read_excel)(pm.smx_path + smx, sheet_name='Column mapping')
            # Column_mapping = delayed(funcs.replace_nan)(Column_mapping)

            BMAP_values = delayed(funcs.read_excel)(smx_file_path, sheet_name='BMAP values')
            BMAP = delayed(funcs.read_excel)(smx_file_path, sheet_name='BMAP')
            BKEY = delayed(funcs.read_excel)(smx_file_path, sheet_name='BKEY')

            Core_tables = delayed(funcs.read_excel)(smx_file_path, sheet_name='Core tables')
            Core_tables = delayed(funcs.rename_sheet_reserved_word)(Core_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])

            home_output_path = pm.output_path + smx_file_name + '/'
            delayed_remove_home_output_path = delayed(md.remove_folder)(home_output_path)
            delayed_create_home_output_path = delayed(md.create_folder)(home_output_path)

            parallel_remove_output_home_path.append(delayed_remove_home_output_path)
            parallel_create_output_home_path.append(delayed_create_home_output_path)

            parallel_templates.append(delayed(gcfr.gcfr)(home_output_path))

            for system_index, system_row in teradata_sources.iterrows():
                try:
                    Loading_Type = system_row['Loading type']
                    source_name = system_row['Source system name']
                    source_name_filter = [['Source', [source_name]]]
                    stg_source_name_filter = [['Source system name', [source_name]]]

                    Table_mapping = delayed(funcs.read_excel)(smx_file_path, 'Table mapping', source_name_filter, False)

                    STG_tables = delayed(funcs.read_excel)(smx_file_path, 'STG tables', stg_source_name_filter, False)
                    STG_tables = delayed(funcs.rename_sheet_reserved_word)(STG_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])

                    source_output_path = home_output_path + Loading_Type + '/' + source_name

                    delayed_create_source_output_path = delayed(md.create_folder)(source_output_path)
                    parallel_create_output_source_path.append(delayed_create_source_output_path)

                    parallel_templates.append(delayed(D000.d000)(source_output_path, source_name, Table_mapping, STG_tables, BKEY))
                    parallel_templates.append(delayed(D001.d001)(source_output_path, source_name, STG_tables))
                    parallel_templates.append(delayed(D002.d002)(source_output_path, Core_tables, Table_mapping))

                    parallel_templates.append(delayed(D200.d200)(source_output_path, STG_tables))
                    parallel_templates.append(delayed(D210.d210)(source_output_path, STG_tables))

                    parallel_templates.append(delayed(D300.d300)(source_output_path, STG_tables, BKEY))
                    parallel_templates.append(delayed(D320.d320)(source_output_path, STG_tables, BKEY))
                    parallel_templates.append(delayed(D330.d330)(source_output_path, STG_tables, BKEY))
                    parallel_templates.append(delayed(D340.d340)(source_output_path, STG_tables, BKEY))

                    parallel_templates.append(delayed(D400.d400)(source_output_path, STG_tables))
                    parallel_templates.append(delayed(D410.d410)(source_output_path, STG_tables))
                    parallel_templates.append(delayed(D415.d415)(source_output_path, STG_tables))
                    parallel_templates.append(delayed(D420.d420)(source_output_path, STG_tables, BKEY, BMAP))

                    parallel_templates.append(delayed(D600.d600)(source_output_path, source_name, Table_mapping, Core_tables))
                    parallel_templates.append(delayed(D607.d607)(source_output_path, Core_tables, BMAP_values))
                    parallel_templates.append(delayed(D608.d608)(source_output_path, Core_tables, BMAP_values))
                    parallel_templates.append(delayed(D610.D610)(source_output_path, source_name, Table_mapping, Core_tables))
                    parallel_templates.append(delayed(D615.d615)(source_output_path, Core_tables))
                    parallel_templates.append(delayed(D620.d620)(source_output_path, source_name, Table_mapping, Column_mapping, Core_tables, Loading_Type))
                    parallel_templates.append(delayed(D630.d630)(source_output_path, source_name, Table_mapping))
                    parallel_templates.append(delayed(D640.d640)(source_output_path, source_name, Table_mapping))
                except Exception as error:
                    # print(error)
                    # traceback.print_exc()
                    count_sources = count_sources - 1
    except Exception as error:
        # print(error)
        # traceback.print_exc()
        count_smx = count_smx - 1

    if len(parallel_templates) > 0:
        compute(*parallel_remove_output_home_path)
        compute(*parallel_create_output_home_path)
        compute(*parallel_create_output_source_path)
        with ProgressBar():
            smx_files = " smx files" if count_smx > 1 else " smx file"
            print("Start generating " + str(len(parallel_templates)) + " script for " + str(count_sources) + " sources from " + str(count_smx) + smx_files)
            compute(*parallel_templates)

        os.startfile(pm.output_path)
        print("Output: " + pm.output_path)
    else:
        print("No SMX sheets found!")


if __name__ == '__main__':
    generate_scripts()