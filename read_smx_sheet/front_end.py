import os
import sys

sys.path.append(os.getcwd())
from read_smx_sheet.app_Lib import manage_directories as md, functions as funcs
import multiprocessing
from tkinter import *
from tkinter import messagebox, filedialog, ttk
from read_smx_sheet.parameters import parameters as pm
import read_smx_sheet.generate_scripts as gs
import datetime as dt
import traceback
import time
import threading
import random


class FrontEnd:
    def __init__(self):
        self.root = Tk()
        img_icon = PhotoImage(file=os.path.join(md.get_dirs()[0], 'script_icon.png'))
        self.root.tk.call('wm', 'iconphoto', self.root._w, img_icon)
        self.root.wm_title("Need for speed" + pm.ver_no)
        self.root.resizable(width="false", height="false")
        self.msg_no_config_file = "No Destinations File Found!"
        self.color_msg_no_config_file = "red"
        self.msg_ready = "Ready"
        self.color_msg_ready = "green"
        self.msg_generating = "In Progress... "
        self.color_msg_generating = "blue"
        self.msg_done = "Done, Elapsed Time: "
        self.color_msg_done = "green"
        self.color_msg_done_with_error = "red"
        self.color_error_messager = "red"
        self.project_generation_flag = "Project ACA"

        frame_row0 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row0.grid(column=0, row=0)

        frame_row1 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row1.grid(column=0, row=1, sticky=W)

        frame_row2 = Frame(self.root, borderwidth="2", relief="ridge")
        frame_row2.grid(column=0, row=2, sticky=W + E)

        frame_row2.grid_columnconfigure(0, weight=1, uniform="group1")
        frame_row2.grid_columnconfigure(1, weight=1, uniform="group1")
        frame_row2.grid_rowconfigure(0, weight=1)

        frame_row2_l = Frame(frame_row2, borderwidth="2", relief="ridge")
        frame_row2_l.grid(column=0, row=3, sticky=W + E)

        frame_row2_r = Frame(frame_row2, borderwidth="2", relief="ridge")
        frame_row2_r.grid(column=1, row=3, sticky=W + E)

        self.status_label_text = StringVar()
        self.status_label = Label(frame_row2_l)
        self.status_label.grid(column=0, row=0, sticky=W)

        self.server_info_label_text = StringVar()
        self.server_info_label = Label(frame_row2_r)
        self.server_info_label.grid(column=1, row=0, sticky=E)

        desinations_file_label = Label(frame_row0, text="Destinations file")
        desinations_file_label.grid(row=0, column=0, sticky='e')

        self.config_file_browse_button = Button(frame_row0, text="...", command=self.browsefunc)
        self.config_file_browse_button.grid(row=0, column=3, sticky='w')

        self.config_file_entry_txt = StringVar()
        self.config_file_entry = Entry(frame_row0, textvariable=self.config_file_entry_txt, width=100)
        config_file_path = os.path.join(funcs.get_config_file_path(), pm.default_config_file_name)
        try:
            x = open(config_file_path)
        except:
            config_file_path = ""
        self.config_file_entry.insert(END, config_file_path)
        self.config_file_entry.grid(row=0, column=1)

        frame_buttons = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_buttons.grid(column=1, row=0)
        self.generate_button = Button(frame_buttons, text="Start", width=12, height=2, command=self.start)
        self.generate_button.grid(row=2, column=0)
        # close_button = Button(frame_buttons, text="Abort", width=12, height=1, command=self.close)
        # close_button.grid(row=3, column=0)
        close_button = Button(frame_buttons, text="Exit", width=12, height=2, command=self.close)
        close_button.grid(row=4, column=0)

        frame_config_file_values = Frame(frame_row1, borderwidth="2", relief="ridge")
        frame_config_file_values.grid(column=0, row=0, sticky="w")

        frame_checkboxes_values = Frame(frame_config_file_values, relief="ridge")
        frame_checkboxes_values.grid(column=1, row=6, sticky="W")

        frame_radiobuttons_values = Frame(frame_config_file_values, relief="ridge")
        frame_radiobuttons_values.grid(column=1, row=5, sticky="W")

        self.get_config_file_values()
        frame_config_file_values_entry_width = 84

        source_location = Label(frame_config_file_values, text="Source Location")
        source_location.grid(row=0, column=0, sticky='e')

        self.source_location_entry = StringVar()
        self.entry_field_source_location = Entry(frame_config_file_values, textvariable=self.source_location_entry,
                                                 width=frame_config_file_values_entry_width)
        self.entry_field_source_location.grid(row=0, column=1, sticky="w")

        destination_location = Label(frame_config_file_values, text="Destinations File Path")
        destination_location.grid(row=1, column=0, sticky='e')

        self.destination_location_entry = StringVar()
        self.entry_field_destionation_location = Entry(frame_config_file_values,
                                                       textvariable=self.destination_location_entry,
                                                       width=frame_config_file_values_entry_width)
        self.entry_field_destionation_location.grid(row=1, column=1, sticky="w")

        output_path_label = Label(frame_config_file_values, text="Output Folder Name")
        output_path_label.grid(row=2, column=0, sticky='e')

        self.output_folder_name_entry = StringVar()
        self.entry_field_output_folder_name = Entry(frame_config_file_values,
                                                    textvariable=self.output_folder_name_entry,
                                                    width=frame_config_file_values_entry_width)
        self.entry_field_output_folder_name.grid(row=2, column=1, sticky="w")

        source_names_label = Label(frame_config_file_values, text="Output Folder Path")
        source_names_label.grid(row=3, column=0, sticky='e')

        self.output_folder_path_entry = StringVar()
        self.entry_field_output_folder_path = Entry(frame_config_file_values,
                                                    textvariable=self.output_folder_path_entry,
                                                    width=frame_config_file_values_entry_width)
        self.entry_field_output_folder_path.grid(row=3, column=1, sticky="w", columnspan=1)

        self.populate_config_file_values()
        self.config_file_entry_txt.trace("w", self.refresh_config_file_values)

        thread0 = GenerateScriptsThread(0, "Thread-0", self)
        thread0.start()

        self.root.mainloop()

    def change_status_label(self, msg, color):
        self.status_label_text.set(msg)
        self.status_label.config(fg=color, text=self.status_label_text.get())

    def change_server_info_label(self, msg, color):
        try:
            self.server_info_label_text.set(msg)
            self.server_info_label.config(fg=color, text=self.server_info_label_text.get())
        except RuntimeError:
            pass

    def get_config_file_values(self):
        try:
            self.config_file_values = funcs.get_config_file_values(self.config_file_entry_txt.get())
            self.source_location = self.config_file_values["source_location"]
            self.output_path = self.config_file_values["output_folder_path"]
            self.output_file = self.config_file_values["output_folder_name"]
            self.destinations = self.config_file_values["destinations_file_path"]
            self.generate_button.config(state=NORMAL)
            self.change_status_label(self.msg_ready, self.color_msg_ready)

        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)
            self.generate_button.config(state=DISABLED)
            self.source_location = ""
            self.output_path = ""
            self.output_file = ""
            self.destinations = ""

    def refresh_config_file_values(self, *args):
        self.get_config_file_values()
        self.populate_config_file_values()

    def populate_config_file_values(self):
        self.entry_field_source_location.config(state=NORMAL)
        self.entry_field_source_location.delete(0, END)
        self.entry_field_source_location.insert(END, self.source_location)
        self.entry_field_source_location.config(state=DISABLED)

        self.entry_field_destionation_location.config(state=NORMAL)
        self.entry_field_destionation_location.delete(0, END)
        self.entry_field_destionation_location.insert(END, self.destinations)
        self.entry_field_destionation_location.config(state=DISABLED)

        self.entry_field_output_folder_name.config(state=NORMAL)
        self.entry_field_output_folder_name.delete(0, END)
        self.entry_field_output_folder_name.insert(END, self.output_file)
        self.entry_field_output_folder_name.config(state=DISABLED)

        self.entry_field_output_folder_path.config(state=NORMAL)
        self.entry_field_output_folder_path.delete(0, END)
        self.entry_field_output_folder_path.insert(END, self.output_path)
        self.entry_field_output_folder_path.config(state=DISABLED)

    def browsefunc(self):
        current_file = self.config_file_entry_txt.get()
        filename = filedialog.askopenfilename(initialdir=md.get_dirs()[1])
        filename = current_file if filename == "" else filename
        self.config_file_entry.delete(0, END)
        self.config_file_entry.insert(END, filename)
        self.refresh_config_file_values()

    def pb(self, tasks, task_len):
        self.progress_var = IntVar()
        pb = ttk.Progressbar(self.root, orient="horizontal",
                             length=300, maximum=task_len - 1,
                             mode="determinate",
                             var=self.progress_var)
        pb.grid(row=3, column=1)

        for i, task in enumerate(tasks):
            self.progress_var.set(i)
            i += 1
            # time.sleep(1 / 60)
            # compute(task)
            self.root.update_idletasks()

    def enable_disable_fields(self, f_state):
        self.generate_button.config(state=f_state)
        self.config_file_entry.config(state=f_state)
        self.config_file_browse_button.config(state=f_state)

    def generate_scripts_thread(self):
        try:
            config_file_path = self.config_file_entry_txt.get()
            x = open(config_file_path)
            try:
                self.enable_disable_fields(DISABLED)
                self.g.generate_scripts()
                self.enable_disable_fields(NORMAL)

                print("Total Elapsed time: ", self.g.elapsed_time, "\n")
            except Exception as error:
                try:
                    error_messager = self.g.error_message
                except:
                    error_messager = error
                self.change_status_label(error_messager, self.color_error_messager)
                self.generate_button.config(state=NORMAL)
                self.config_file_entry.config(state=NORMAL)
                traceback.print_exc()
        except:
            self.change_status_label(self.msg_no_config_file, self.color_msg_no_config_file)

    def destroyer(self):
        self.root.quit()
        self.root.destroy()
        sys.exit()

    def close(self):
        self.root.protocol("WM_DELETE_WINDOW", self.destroyer())

    def abort(self):
        if self.runningthread is not None:
            self.runningthread.terminate()

    def start(self):
        self.refresh_config_file_values()
        self.g = gs.GenerateScripts(None, self.config_file_values)

        thread1 = GenerateScriptsThread(1, "Thread-1", self)
        thread1.start()

        thread2 = GenerateScriptsThread(2, "Thread-2", self, thread1)
        thread2.start()

    def generating_indicator(self, thread):
        def r():
            return random.randint(0, 255)

        while thread.is_alive():
            elapsed_time = dt.datetime.now() - self.g.start_time
            msg = self.msg_generating + str(elapsed_time)
            # color_list = ["white", "black", "red", "green", "blue", "cyan", "yellow", "magenta"]
            # color = random.choice(color_list)
            color = '#%02X%02X%02X' % (r(), r(), r())
            self.change_status_label(msg, color)

        message = self.g.error_message if self.g.error_message != "" else self.msg_done + str(self.g.elapsed_time)
        color = self.color_msg_done_with_error if self.g.error_message != "" else self.color_msg_done
        self.change_status_label(message, color)

    def display_server_info(self, thread):
        color = "blue"
        while True:
            server_info = funcs.server_info()
            msg = "CPU " + str(server_info[0]) + "%" + " Memory " + str(server_info[1]) + "%"
            self.change_server_info_label(msg, color)


class GenerateScriptsThread(threading.Thread):
    def __init__(self, threadID, name, front_end_c, thread=None):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.FrontEndC = front_end_c
        self.thread = thread
        self.daemon = True

    def run(self):
        if self.threadID == 1:
            self.FrontEndC.generate_scripts_thread()
        if self.threadID == 2:
            self.FrontEndC.generating_indicator(self.thread)
        if self.threadID == 0:
            self.FrontEndC.display_server_info(self.thread)


if __name__ == '__main__':
    multiprocessing.freeze_support()
    FrontEnd()
