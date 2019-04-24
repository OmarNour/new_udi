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
        self.module_path = os.path.dirname(sys.modules['__main__'].__file__)
        self.output_path = pm.output_path

        self.smx_process_no = 0
        self.smx_processes_names = {}
        self.smx_processes_run_status = {}
        self.smx_processes_numbers = []

        self.build_scripts_process_no = 0
        self.build_scripts_processes_names = {}
        self.build_scripts_processes_run_status = {}
        self.build_scripts_processes_numbers = []

        self.count_smx = 0
        self.count_sources = 0

    def read_smx_folder(self):
        print("\nReading from: \t", pm.smx_path)
        print("Output folder: \t", self.output_path)
        print(pm.smx_ext + " files:")

        read_smx_source_inputs = []
        for smx in md.get_files_in_dir(pm.smx_path, pm.smx_ext):
            self.count_smx = self.count_smx + 1
            smx_file_name = os.path.splitext(smx)[0]
            print("\t" + smx_file_name)
            home_output_path = self.output_path + "/" + smx_file_name
            smx_file_path = pm.smx_path + "/" + smx
            read_smx_source_inputs.append([home_output_path,smx_file_path])

            md.remove_folder(home_output_path)
            md.create_folder(home_output_path)
            gcfr.gcfr(home_output_path)

            self.read_smx_sheets(smx_file_path)

        print("\nStart reading " + str(len(self.smx_processes_numbers)) + " SMX sheets...")
        print("---------------------------------------------------------------------------")
        funcs.wait_for_processes_to_finish(self.smx_processes_numbers, self.smx_processes_run_status, self.smx_processes_names)

        for i in read_smx_source_inputs:
            i_home_output_path = i[0]
            i_smx_file_path = i[1]
            self.build_smx_source_scripts(i_home_output_path, i_smx_file_path)

        print("\nStart building scripts for " + str(len(self.build_scripts_processes_numbers)) + " sources...")
        print("---------------------------------------------------------------------------")
        funcs.wait_for_processes_to_finish(self.build_scripts_processes_numbers, self.build_scripts_processes_run_status, self.build_scripts_processes_names)
        os.startfile(self.output_path)

    def read_smx_sheets(self, smx_file_path):
        #################################################################################
        smx_file_name = funcs.get_file_name(smx_file_path)
        self.smx_processes_names[self.smx_process_no] = smx_file_name
        self.smx_processes_numbers.append(self.smx_process_no)
        main_inputs = " smx_file_path=" + funcs.single_quotes(smx_file_path)
        main_inputs = main_inputs + " output_path=" + funcs.single_quotes(self.output_path)
        main_inputs = main_inputs + " task=" + funcs.single_quotes(1)

        to_run = self.module_path + '/ReadSMX.py'
        self.smx_processes_run_status[self.smx_process_no] = subprocess.Popen(['python',
                                                                               to_run,
                                                                               main_inputs])
        self.smx_process_no += 1
        #################################################################################

    def build_smx_source_scripts(self, home_output_path, smx_file_path):
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
                self.build_scripts_processes_names[self.build_scripts_process_no] = smx_file_name + "\t" + source_name
                self.build_scripts_processes_numbers.append(self.build_scripts_process_no)
                main_inputs = " smx_file_path=" + funcs.single_quotes(smx_file_path)
                main_inputs = main_inputs + " source_output_path=" + funcs.single_quotes(source_output_path)
                main_inputs = main_inputs + " output_path=" + funcs.single_quotes(self.output_path)
                main_inputs = main_inputs + " source_name=" + funcs.single_quotes(source_name)
                main_inputs = main_inputs + " Loading_Type=" + funcs.single_quotes(Loading_Type)
                main_inputs = main_inputs + " task=" + funcs.single_quotes(2)

                to_run = self.module_path+ '/ReadSMX.py'
                self.build_scripts_processes_run_status[self.build_scripts_process_no] = subprocess.Popen(['python',
                                                                                                           to_run,
                                                                                                           main_inputs])
                self.build_scripts_process_no += 1
                #################################################################################

            except Exception as error:
                # print("1", error)
                pass
                # traceback.print_exc()
                self.count_sources = self.count_sources - 1


if __name__ == '__main__':
    start_time = dt.datetime.now()

    read_smx = ReadSmxFolder()
    read_smx.read_smx_folder()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)