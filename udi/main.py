# from .smx import *
from model import *
from functions import time_elapsed_decorator
from smx import SMX, generate_scripts, generate_metadata_scripts
import logging
import time
from datetime import datetime

@time_elapsed_decorator
def start(run_id, source_layer0, db_prefix, smx_path: str, output_path: str, source_name: str | list | None, with_scripts=True, with_deploy=False):
    # start_time = time.perf_counter()
    start_time = datetime.now()
    smx = SMX(smx_path, run_id, output_path, db_prefix, source_layer0)
    # print(smx)
    smx.parse_file()
    smx.populate_model(source_name=source_name)

    if with_scripts:
        # generate_schemas_ddl(smx)
        generate_scripts(smx)
        generate_metadata_scripts(smx)
        if with_deploy:
            deploy()
        # generate_fake_data()

    myid_summary = "\n\nSummary:\n######################\n\n"
    for class_name in MyID.get_all_classes_instances().keys():
        cls_instances_cout = eval(f"{class_name}.count_instances()")
        class_count = f'{class_name} count: {cls_instances_cout}\n'
        myid_summary += class_count

    # reset class instances
    MyID.reset_class_variable()

    end_time = datetime.now()

    time_elapsed = end_time-start_time
    myid_summary += f"Time Elapsed: {time_elapsed}\n"

    # del smx
    logging.info(myid_summary)

    # remove all logger handlers
    logging.getLogger().handlers = []
    # logging.shutdown()

    # open_folder()
    # return smx.current_scripts_path


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # pipreqs /Users/omarnour/PycharmProjects/tdUDI --force --ignore tests
    try:
        start(source_name=['NEW_COLLECTIONS'], with_scripts=False, with_deploy=False)
        # start(source_name=['Insurance_Pension'], with_scripts=True, with_deploy=False)
        # start(source_name=[], with_scripts=False)
        # start(source_name=[], with_scripts=True)
    except KeyboardInterrupt:
        print("Ops!..")
