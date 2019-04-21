import os
import sys
sys.path.append(os.getcwd())
from parameters import parameters as pm
from app_Lib import manage_directories as md, functions as funcs
import datetime as dt
from templates import gcfr
import subprocess


class ReadSmxFolder:
    def __init__(self):
        self.count_smx = 0
        self.count_sources = 0


    def read_smx_folder(self):
        dt_now = dt.datetime.now()
        dt_folder = dt_now.strftime("%Y") + "_" + dt_now.strftime("%b").upper() + "_" + dt_now.strftime("%d") + "_" + dt_now.strftime("%H") + "_" + dt_now.strftime("%M")
        output_path = pm.output_path + "/" + dt_folder
        print("Reading from: \t", pm.smx_path)
        print("Output folder: \t", output_path)
        print(pm.smx_ext + " files:")

        process_dict = {}
        process_list = []
        module_path = os.path.dirname(sys.modules['__main__'].__file__)
        for process_no, smx in enumerate(md.get_files_in_dir(pm.smx_path, pm.smx_ext)):
            self.count_smx = self.count_smx + 1
            smx_file_name = os.path.splitext(smx)[0]
            print("\t" + smx_file_name)
            home_output_path = output_path + "/" + smx_file_name + "/"
            smx_file_path = pm.smx_path + "/" + smx

            md.remove_folder(home_output_path)
            md.create_folder(home_output_path)
            gcfr.gcfr(home_output_path)
            #################################################################################
            process_list.append(process_no)
            main_inputs = " home_output_path=" + funcs.single_quotes(home_output_path) + " smx_file_path=" + funcs.single_quotes(smx_file_path) + " "
            to_run = module_path + '/ReadSMX.py'
            process_dict[str(process_no)] = subprocess.Popen(['python',
                                                              to_run,
                                                              main_inputs])
        funcs.wait_for_processes_to_finish(process_list, process_dict)
        #################################################################################

        os.startfile(pm.output_path)


if __name__ == '__main__':
    start_time = dt.datetime.now()

    read_smx = ReadSmxFolder()
    read_smx.read_smx_folder()

    end_time = dt.datetime.now()
    print("\nTotal Elapsed time: ", end_time - start_time)