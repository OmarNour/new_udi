import os
import sys
sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
from read_smx_sheet.parameters import parameters as pm
from dask import compute, delayed, config
from dask.diagnostics import ProgressBar
import datetime as dt
from read_smx_sheet.templates import D300, D320, D200, D330, D400, D610, D640
from read_smx_sheet.templates import D410, D415, D003, D630, D420, D210, D608, D615, D000, gcfr, D620, D001, D600, D607, D002, D340
import multiprocessing


class GenerateScripts:
    def __init__(self):
        self.parallel_remove_output_home_path = []
        self.parallel_create_output_home_path = []
        self.parallel_create_output_source_path = []
        self.parallel_templates = []
        self.count_sources = 0
        self.count_smx = 0
        self.output_path = pm.output_path

    def generate_scripts(self):
        print("Reading from: \t" + pm.smx_path)
        print("Output folder: \t" + self.output_path)
        print(pm.smx_ext + " files:")
        try:
            smx_files = funcs.get_smx_files(pm.smx_path, pm.smx_ext, pm.sheets)
            for smx in smx_files:
                self.count_smx = self.count_smx + 1
                smx_file_path = pm.smx_path + "/" + smx
                smx_file_name = os.path.splitext(smx)[0]
                print("\t" + smx_file_name)
                home_output_path = self.output_path + "/" + smx_file_name + "/"

                self.parallel_remove_output_home_path.append(delayed(md.remove_folder)(home_output_path))
                self.parallel_create_output_home_path.append(delayed(md.create_folder)(home_output_path))

                self.parallel_templates.append(delayed(gcfr.gcfr)(home_output_path))
                ##################################### end of read_smx_folder ################################
                if pm.source_names:
                    System_sht_filter = [['Source system name', pm.source_names]]
                else:
                    System_sht_filter = None

                System = funcs.read_excel(smx_file_path, sheet_name=pm.System_sht)
                teradata_sources = System[System['Source type'] == 'TERADATA']
                teradata_sources = funcs.df_filter(teradata_sources, System_sht_filter, False)
                self.count_sources = self.count_sources + len(teradata_sources.index)

                Supplements = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Supplements_sht)
                Column_mapping = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Column_mapping_sht)
                BMAP_values = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BMAP_values_sht)
                BMAP = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BMAP_sht)
                BKEY = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.BKEY_sht)
                Core_tables = delayed(funcs.read_excel)(smx_file_path, sheet_name=pm.Core_tables_sht)
                Core_tables = delayed(funcs.rename_sheet_reserved_word)(Core_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])
                ##################################### end of read_smx_sheet ################################

                for system_index, system_row in teradata_sources.iterrows():
                    try:
                        Loading_Type = system_row['Loading type'].upper()
                        source_name = system_row['Source system name']
                        source_name_filter = [['Source', [source_name]]]
                        stg_source_name_filter = [['Source system name', [source_name]]]

                        Table_mapping = delayed(funcs.read_excel)(smx_file_path, pm.Table_mapping_sht, source_name_filter)

                        STG_tables = delayed(funcs.read_excel)(smx_file_path, pm.STG_tables_sht, stg_source_name_filter)
                        STG_tables = delayed(funcs.rename_sheet_reserved_word)(STG_tables, Supplements, 'TERADATA', ['Column name', 'Table name'])

                        source_output_path = home_output_path + "/" + Loading_Type + "/" + source_name

                        self.parallel_create_output_source_path.append(delayed(md.create_folder)(source_output_path))

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
                    except Exception as error:
                        # print(error)
                        # traceback.print_exc()
                        self.count_sources = self.count_sources - 1
        except Exception as error:
            # print(error)
            # traceback.print_exc()
            self.count_smx = self.count_smx - 1

        if len(self.parallel_templates) > 0:
            scheduler_value = 'processes' if pm.read_sheets_parallel else ''
            with config.set(scheduler=scheduler_value):
                compute(*self.parallel_remove_output_home_path)
                compute(*self.parallel_create_output_home_path)
                compute(*self.parallel_create_output_source_path)

                with ProgressBar():
                    smx_files = " smx files" if self.count_smx > 1 else " smx file"
                    print("Start generating " + str(len(self.parallel_templates)) + " script for " + str(self.count_sources) + " sources from " + str(self.count_smx) + smx_files)
                    compute(*self.parallel_templates)

            os.startfile(pm.output_path)
        else:
            print("No SMX sheets found!")


if __name__ == '__main__':
    multiprocessing.freeze_support()
    start_time = dt.datetime.now()
    g = GenerateScripts()
    g.generate_scripts()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)
    k = input("")