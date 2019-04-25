import os
import sys
sys.path.append(os.getcwd())
from dask import compute, delayed
import multiprocessing
import warnings
warnings.filterwarnings("ignore")

class ReadSmx:
    def __init__(self):
        self.cpu_count = multiprocessing.cpu_count()
        self.parallel_templates = []
        self.parallel_save_sheet_data = []

    def read_smx_file(self, output_path, smx_file_path):
        try:
            teradata_sources_filter = [['Source type', ['TERADATA']]]
            teradata_sources = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.System_sht, filter=teradata_sources_filter)

            Supplements = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Supplements_sht)
            Column_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Column_mapping_sht)
            BMAP_values = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BMAP_values_sht)
            BMAP = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BMAP_sht)
            BKEY = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BKEY_sht)
            Core_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
            Core_tables = delayed(funcs.read_excel)(smx_file_path, pm.Core_tables_sht, None, Core_tables_reserved_words)
            STG_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
            STG_tables = delayed(funcs.read_excel)(smx_file_path, pm.STG_tables_sht, None, STG_tables_reserved_words)
            Table_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Table_mapping_sht)

            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(teradata_sources, smx_file_path, output_path, pm.System_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Supplements, smx_file_path, output_path, pm.Supplements_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Column_mapping, smx_file_path, output_path, pm.Column_mapping_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP_values, smx_file_path, output_path, pm.BMAP_values_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP, smx_file_path, output_path, pm.BMAP_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BKEY, smx_file_path, output_path, pm.BKEY_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Core_tables, smx_file_path, output_path, pm.Core_tables_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(STG_tables, smx_file_path, output_path, pm.STG_tables_sht))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Table_mapping, smx_file_path, output_path, pm.Table_mapping_sht))

        except Exception as error:
            # print("0", error)
            pass

        if len(self.parallel_save_sheet_data) > 0:
            compute(*self.parallel_save_sheet_data, num_workers=self.cpu_count)

    def read_smx_sheets(self, output_path, smx_file_path, sheet_name):
        try:
            if sheet_name == pm.Supplements_sht:
                Supplements = funcs.read_excel(smx_file_path, sheet_name=pm.Supplements_sht)
                funcs.save_sheet_data(Supplements, smx_file_path, output_path, pm.Supplements_sht)
            elif sheet_name == pm.System_sht:
                teradata_sources_filter = [['Source type', ['TERADATA']]]
                teradata_sources = funcs.read_excel(smx_file_path, sheet_name=pm.System_sht, filter=teradata_sources_filter)
                funcs.save_sheet_data(teradata_sources, smx_file_path, output_path, pm.System_sht)
            elif sheet_name == pm.Table_mapping_sht:
                Table_mapping = funcs.read_excel(smx_file_path, sheet_name=pm.Table_mapping_sht)
                funcs.save_sheet_data(Table_mapping, smx_file_path, output_path, pm.Table_mapping_sht)
            elif sheet_name == pm.Column_mapping_sht:
                Column_mapping = funcs.read_excel(smx_file_path, sheet_name=pm.Column_mapping_sht)
                funcs.save_sheet_data(Column_mapping, smx_file_path, output_path, pm.Column_mapping_sht)
            elif sheet_name == pm.BMAP_values_sht:
                BMAP_values = funcs.read_excel(smx_file_path, sheet_name=pm.BMAP_values_sht)
                funcs.save_sheet_data(BMAP_values, smx_file_path, output_path, pm.BMAP_values_sht)
            elif sheet_name == pm.BMAP_sht:
                BMAP = funcs.read_excel(smx_file_path, sheet_name=pm.BMAP_sht)
                funcs.save_sheet_data(BMAP, smx_file_path, output_path, pm.BMAP_sht)
            elif sheet_name == pm.BKEY_sht:
                BKEY = funcs.read_excel(smx_file_path, sheet_name=pm.BKEY_sht)
                funcs.save_sheet_data(BKEY, smx_file_path, output_path, pm.BKEY_sht)
            elif sheet_name == pm.Core_tables_sht:
                Supplements = funcs.read_excel(smx_file_path, sheet_name=pm.Supplements_sht)
                Core_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
                Core_tables = funcs.read_excel(smx_file_path, pm.Core_tables_sht, None, Core_tables_reserved_words)
                funcs.save_sheet_data(Core_tables, smx_file_path, output_path, pm.Core_tables_sht)
            elif sheet_name == pm.STG_tables_sht:
                Supplements = funcs.read_excel(smx_file_path, sheet_name=pm.Supplements_sht)
                STG_tables_reserved_words = [Supplements, 'TERADATA', ['Column name', 'Table name']]
                STG_tables = funcs.read_excel(smx_file_path, pm.STG_tables_sht, None, STG_tables_reserved_words)
                funcs.save_sheet_data(STG_tables, smx_file_path, output_path, pm.STG_tables_sht)

        except Exception as error:
            # print("0", error)
            pass

    def build_source_scripts(self, smx_file_path, output_path, source_output_path, source_name, Loading_Type):
        Column_mapping = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.Column_mapping_sht)
        BMAP_values = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.BMAP_values_sht)
        BMAP = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.BMAP_sht)
        BKEY = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.BKEY_sht)
        Core_tables = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.Core_tables_sht)

        source_name_filter = [['Source', [source_name]]]
        stg_source_name_filter = [['Source system name', [source_name]]]

        Table_mapping = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.Table_mapping_sht, source_name_filter)
        STG_tables = delayed(funcs.get_sheet_data)(smx_file_path, output_path, pm.STG_tables_sht, stg_source_name_filter)

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


if __name__ == '__main__':
    from read_smx_sheet.app_Lib import functions as funcs
    from read_smx_sheet.parameters import parameters as pm
    from read_smx_sheet.templates import D300, D320, D200, D330, D400, D610, D640
    from read_smx_sheet.templates import D410, D415, D003, D630, D420, D210, D608, D615, D000, D620, D001, D600, D607, D002, D340

    read_smx = ReadSmx()
    inputs = funcs.string_to_dict(sys.argv[1], pm.sys_argv_separator)

    i_task = inputs['task']
    i_smx_file_path = inputs['smx_file_path']
    i_output_path = inputs['output_path']

    if i_task == '0':
        read_smx.read_smx_file(i_output_path, i_smx_file_path)

    if i_task == '1':
        i_sheet_name = inputs['sheet_name']
        read_smx.read_smx_sheets(i_output_path, i_smx_file_path, i_sheet_name)

    if i_task == '2':
        i_source_output_path = inputs['source_output_path']
        i_source_name = inputs['source_name']
        i_Loading_Type = inputs['Loading_Type']
        read_smx.build_source_scripts(i_smx_file_path, i_output_path, i_source_output_path, i_source_name, i_Loading_Type)
