import os
import sys
sys.path.append(os.getcwd())
from parameters import parameters as pm
from app_Lib import manage_directories as md, functions as funcs
import datetime as dt
from templates import gcfr
import subprocess
from dask import compute, delayed
import multiprocessing
import warnings
warnings.filterwarnings("ignore")
from dask.diagnostics import ProgressBar


class ReadSmxFolder:
    def __init__(self):
        self.output_path = pm.output_path
        self.process_no = 0
        self.processes_names = {}
        self.processes_run_status = {}
        self.processes_numbers = []

        self.count_smx = 0
        self.count_sources = 0
        self.parallel_read_smx_sheets = []
        self.parallel_build_source_scripts = []
        self.parallel_save_sheet_data = []

    def read_smx_sheets(self, smx_file_path):
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

            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(teradata_sources, smx_file_path, self.output_path, 'System'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Supplements, smx_file_path, self.output_path, 'Supplements'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Column_mapping, smx_file_path, self.output_path, 'Column mapping'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP_values, smx_file_path, self.output_path, 'BMAP values'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BMAP, smx_file_path, self.output_path, 'BMAP'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(BKEY, smx_file_path, self.output_path, 'BKEY'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Core_tables, smx_file_path, self.output_path, 'Core tables'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(STG_tables, smx_file_path, self.output_path, 'STG tables'))
            self.parallel_save_sheet_data.append(delayed(funcs.save_sheet_data)(Table_mapping, smx_file_path, self.output_path, 'Table mapping'))

            # self.parallel_read_smx_source.append(delayed(self.read_smx_source)(home_output_path, smx_file_path))

        except Exception as error:
            # print("0", error)
            pass

    def read_smx_source(self, home_output_path, smx_file_path):

        module_path = os.path.dirname(sys.modules['__main__'].__file__)
        teradata_sources = funcs.get_sheet_data(smx_file_path, self.output_path, "System")

        for system_index, system_row in teradata_sources.iterrows():
            try:
                Loading_Type = system_row['Loading type'].upper()
                source_name = system_row['Source system name']

                source_output_path = home_output_path + "/" + Loading_Type + "/" + source_name
                md.create_folder(source_output_path)
                self.count_sources = self.count_sources + 1
                #################################################################################
                smx_file_name = funcs.get_file_name(smx_file_path)
                self.processes_names[self.process_no] = smx_file_name + "_" + source_name
                self.processes_numbers.append(self.process_no)
                main_inputs = " smx_file_path=" + funcs.single_quotes(smx_file_path)
                main_inputs = main_inputs + " source_output_path=" + funcs.single_quotes(source_output_path)
                main_inputs = main_inputs + " output_path=" + funcs.single_quotes(self.output_path)
                main_inputs = main_inputs + " source_name=" + funcs.single_quotes(source_name)
                main_inputs = main_inputs + " Loading_Type=" + funcs.single_quotes(Loading_Type)
                to_run = module_path + '/ReadSMX.py'
                self.processes_run_status[self.process_no] = subprocess.Popen(['python',
                                                                          to_run,
                                                                          main_inputs])
                self.process_no += 1
                #################################################################################

            except Exception as error:
                # print("1", error)
                pass
                # traceback.print_exc()
                self.count_sources = self.count_sources - 1

    def read_smx_folder(self):
        print("Reading from: \t", pm.smx_path)
        print("Output folder: \t", self.output_path)
        print(pm.smx_ext + " files:")

        read_smx_source_inputs = []
        for smx in md.get_files_in_dir(pm.smx_path, pm.smx_ext):
            self.count_smx = self.count_smx + 1
            smx_file_name = os.path.splitext(smx)[0]
            print("\t" + smx_file_name)
            home_output_path = self.output_path + "/" + smx_file_name + "/"
            smx_file_path = pm.smx_path + "/" + smx
            read_smx_source_inputs.append([home_output_path,smx_file_path])

            md.remove_folder(home_output_path)
            md.create_folder(home_output_path)
            gcfr.gcfr(home_output_path)

            self.parallel_read_smx_sheets.append(delayed(self.read_smx_sheets)(smx_file_path))

        if len(self.parallel_read_smx_sheets) > 0:
            cpu_count = multiprocessing.cpu_count()
            compute(*self.parallel_read_smx_sheets, num_workers=cpu_count)
            with ProgressBar():
                print("\nReading SMX Sheets...")
                compute(*self.parallel_save_sheet_data, num_workers=cpu_count)

        for i in read_smx_source_inputs:
            i_home_output_path = i[0]
            i_smx_file_path = i[1]
            self.read_smx_source(i_home_output_path, i_smx_file_path)

        # smx_files = " smx files" if self.count_smx > 1 else " smx file"
        # print("\nStart generating scripts for " + str(self.count_sources) + " sources from " + str(self.count_smx) + smx_files)
        funcs.wait_for_processes_to_finish(self.processes_numbers, self.processes_run_status, self.processes_names)
        os.startfile(self.output_path)


if __name__ == '__main__':
    start_time = dt.datetime.now()

    read_smx = ReadSmxFolder()
    read_smx.read_smx_folder()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)