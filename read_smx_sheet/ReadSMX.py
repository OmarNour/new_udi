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

        self.parallel_templates = []

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
            cpu_count = multiprocessing.cpu_count()
            compute(*self.parallel_templates, num_workers=cpu_count)

    def validate_smx_sheet(self):
        pass


if __name__ == '__main__':
    read_smx = ReadSmx()
    inputs = funcs.string_to_dict(sys.argv[1])

    i_smx_file_path = inputs['smx_file_path']
    i_output_path = inputs['output_path']
    i_source_output_path = inputs['source_output_path']
    i_source_name = inputs['source_name']
    i_Loading_Type = inputs['Loading_Type']

    read_smx.build_source_scripts(i_smx_file_path, i_output_path, i_source_output_path, i_source_name, i_Loading_Type)
