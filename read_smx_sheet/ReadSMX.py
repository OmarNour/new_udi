import os
import sys
sys.path.append(os.getcwd())
from app_Lib import manage_directories as md, functions as funcs
from dask import compute, delayed
from parameters import parameters as pm
# from dask.diagnostics import ProgressBar
# import traceback
import datetime as dt
from templates import gcfr, D003, D002, D210, D300, D320, D420, D000, D001, D200, D330, D340, D400,D410,D415, D615, D600, D607, D608, D610,D620, D630, D640
import multiprocessing
import warnings
warnings.filterwarnings("ignore")


class ReadSmx:
    def __init__(self):
        self.cpu_count = multiprocessing.cpu_count()
        self.parallel_save_sheet_data = []
        self.parallel_templates = []

    def read_smx_sheets(self, output_path, smx_file_path):
        try:
            teradata_sources_filter = [['Source type', ['TERADATA']]]
            teradata_sources = delayed(funcs.read_excel)(smx_file_path, sheet_name='System', filter=teradata_sources_filter)

            Supplements = delayed(funcs.read_excel)(smx_file_path, sheet_name='Supplements')
            Column_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name='Column mapping')
            BMAP_values = delayed(funcs.read_excel)(smx_file_path, sheet_name='BMAP values')
            BMAP = delayed(funcs.read_excel)(smx_file_path, sheet_name='BMAP')
            BKEY = delayed(funcs.read_excel)(smx_file_path, sheet_name='BKEY')
            Core_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
            Core_tables = delayed(funcs.read_excel)(smx_file_path, 'Core tables', None, Core_tables_reserved_words)
            STG_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
            STG_tables = delayed(funcs.read_excel)(smx_file_path, 'STG tables', None, STG_tables_reserved_words)
            Table_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name='Table mapping')

            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(teradata_sources, smx_file_path, output_path, 'System'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Supplements, smx_file_path, output_path, 'Supplements'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Column_mapping, smx_file_path, output_path, 'Column mapping'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP_values, smx_file_path, output_path, 'BMAP values'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP, smx_file_path, output_path, 'BMAP'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BKEY, smx_file_path, output_path, 'BKEY'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Core_tables, smx_file_path, output_path, 'Core tables'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(STG_tables, smx_file_path, output_path, 'STG tables'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Table_mapping, smx_file_path, output_path, 'Table mapping'))

        except Exception as error:
            # print("0", error)
            pass

        if len(self.parallel_save_sheet_data) > 0:
            compute(*self.parallel_save_sheet_data, num_workers= self.cpu_count)

    def build_source_scripts(self, smx_file_path, output_path, source_output_path, source_name, Loading_Type):
        Column_mapping = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "Column mapping")
        BMAP_values = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "BMAP values")
        BMAP = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "BMAP")
        BKEY = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "BKEY")
        Core_tables = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "Core tables")

        source_name_filter = [['Source', [source_name]]]
        stg_source_name_filter = [['Source system name', [source_name]]]

        Table_mapping = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "Table mapping", source_name_filter)
        STG_tables = delayed(funcs.get_sheet_data)(smx_file_path, output_path, "STG tables", stg_source_name_filter)

        self.parallel_templates.append(delayed(D000.d000)(source_output_path, source_name, Table_mapping, STG_tables, BKEY))
        self.parallel_templates.append(delayed(D001.d001)(source_output_path, source_name, STG_tables))
        self.parallel_templates.append(delayed(D002.d002)(source_output_path, Core_tables, Table_mapping))
        self.parallel_templates.append(delayed(D003.d003)(source_output_path, BMAP_values, BMAP))

        self.parallel_templates.append(delayed(D200.d200)(source_output_path, STG_tables))
        self.parallel_templates.append(delayed(D210.d210)(source_output_path, STG_tables))

        self.parallel_templates.append(delayed(D300.d300)(source_output_path, STG_tables, BKEY))
        self.parallel_templates.append(delayed(D320.d320)(source_output_path, STG_tables, BKEY))
        self.parallel_templates.append(delayed(D330.d330)(source_output_path, STG_tables, BKEY))
        self.parallel_templates.append(delayed(D340.d340)(source_output_path, STG_tables, BKEY))

        self.parallel_templates.append(delayed(D400.d400)(source_output_path, STG_tables))
        self.parallel_templates.append(delayed(D410.d410)(source_output_path, STG_tables))
        self.parallel_templates.append(delayed(D415.d415)(source_output_path, STG_tables))
        self.parallel_templates.append(delayed(D420.d420)(source_output_path, STG_tables, BKEY, BMAP))

        self.parallel_templates.append(delayed(D600.d600)(source_output_path, Table_mapping, Core_tables))
        self.parallel_templates.append(delayed(D607.d607)(source_output_path, Core_tables, BMAP_values))
        self.parallel_templates.append(delayed(D608.d608)(source_output_path, Core_tables, BMAP_values))
        self.parallel_templates.append(delayed(D610.d610)(source_output_path, Table_mapping))
        self.parallel_templates.append(delayed(D615.d615)(source_output_path, Core_tables))
        self.parallel_templates.append(delayed(D620.d620)(source_output_path, Table_mapping, Column_mapping, Core_tables, Loading_Type))
        self.parallel_templates.append(delayed(D630.d630)(source_output_path, Table_mapping))
        self.parallel_templates.append(delayed(D640.d640)(source_output_path, source_name, Table_mapping))

        if len(self.parallel_templates) > 0:
            compute(*self.parallel_templates, num_workers=self.cpu_count)

    def validate_smx_sheet(self):
        pass


if __name__ == '__main__':
    read_smx = ReadSmx()
    inputs = funcs.string_to_dict(sys.argv[1])

    i_task = inputs['task']
    i_smx_file_path = inputs['smx_file_path']
    i_output_path = inputs['output_path']

    if i_task == '1':
        read_smx.read_smx_sheets(i_output_path, i_smx_file_path)

    if i_task == '2':
        i_source_output_path = inputs['source_output_path']
        i_source_name = inputs['source_name']
        i_Loading_Type = inputs['Loading_Type']

        read_smx.build_source_scripts(i_smx_file_path, i_output_path, i_source_output_path, i_source_name, i_Loading_Type)
