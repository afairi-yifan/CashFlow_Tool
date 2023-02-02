from Config.Location import location
import os
from Config.Paths import *
import datetime
from datetime import date
import time

def creat_exact_file_date(path, name):
    today = date.today()
    sec = int(time.time())
    path_out = os.path.join(path, name+'_'+str(today)+'_'+str(sec)+'.xlsx')
    return path_out


##################
#Input data path
##################
data_path = os.path.join(path_input, 'Input_example_2023_02_01_01.xlsx')
var_path = os.path.join(path_input, 'Variables_format.xlsx')

# data_pitch_path = os.path.join(path_input, 'data_pitch.xlsx')
# data_pitch_best_case_path = os.path.join(path_input, 'data_pitch_best_case.xlsx')
##################
#Output data path
##################
save_path = creat_exact_file_date(path_output, 'output_data')
test_path_1 = creat_exact_file_date(path_output, 'first_month_cohort')
test_path_2 = creat_exact_file_date(path_output, 'random_month_cohort')