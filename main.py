import os
import csv
import time
import logging
import threading
from waiting import wait
import pandas as pd
from parser import YamlParser

logging.basicConfig(filename='./logs/2B_final.log', format="%(asctime)s.%(msecs)06d;%(message)s", datefmt='%Y-%m-%d %H:%M:%S', filemode='w')
log = logging.getLogger("my-logger")
log.setLevel(logging.DEBUG)

FILE_NAME = './Milestone2B.yaml'

global_execution_time = 0
op_tasks = {}

def load_data(filename):
    DataTable = csv_loader('./Milestone2', '/' + filename)
    return DataTable

def validate_data(cond):
    if(cond[0] not in op_tasks):
        return False
    return True

def csv_loader(path, name):
    DataTable = []
    filepath = os.path.join(os.getcwd(), path)
    with open(filepath + name,'r') as data:
        for line in csv.DictReader(data):
            DataTable.append(line)
    return DataTable

# def load_data(filepath):
#     print(filepath)
#     # filepath = "./Examples/Milestone2/"
#     file = open(filepath)
#     csvreader = csv.reader(file)
#     header = []
#     header = next(csvreader)
#     rows = []
#     for row in csvreader:
#         rows.append(row)
#     print(len(rows))    
#     return len(rows)

def func_time(curr_execution_time):
    global global_execution_time
    global_execution_time += curr_execution_time
    time.sleep(curr_execution_time)

def tasks_exec(idx, events, exec_type):
    log.info(idx + " Entry")
    ip = events['Inputs']
    func_name = events['Function']
    check = 0
    if("Condition" in events):
        cond = events["Condition"]
        cond = cond.split()
        if(cond[0] not in op_tasks):
            validate = wait(lambda: validate_data(cond), timeout_seconds=60, waiting_for="Task Output")
            if(validate == False):
                check = 1
        else:
            if(cond[1] == '>'):
                if(op_tasks[cond[0]] < int(cond[2])):
                    log.info(idx + " Skipped")
                    check = 1
            elif(cond[1] == '<'):
                if(op_tasks[cond[0]] > int(cond[2])):
                    log.info(idx + " Skipped")
                    check = 1
    if(check == 0):
        if(func_name == "TimeFunction"):
            ex_time = ip['ExecutionTime']
            f_ip = ip['FunctionInput']
            if(f_ip in op_tasks):
                log.info(idx + " Executing " + func_name + " ("+str(op_tasks[str(f_ip)])+", "+ex_time+")")
            else:
                log.info(idx + " Executing " + func_name + " ( " + f_ip + ", " + ex_time+")")
            func_time(int(ex_time))
        elif(func_name == "DataLoad"):
            f_name = ip["Filename"]
            log.info(idx + " Executing " + func_name + " (" + f_name+")")
            DataTable = load_data(f_name)
            op_tasks[idx + ".DataTable"] = DataTable
            op_tasks["$("+idx+".NoOfDefects"+")"] = len(DataTable)
        elif(func_name == "Binning"):
            rule_f_name = ip["RuleFilename"]
            data_set = ip["DataSet"]
            log.info(idx + " Executing " + f_name + " (" + rule_f_name + ", " + data_set + ")")
            data_set = binning(rule_f_name, data_set)
            op_tasks["$(" + idx + ".BinningResultsTable" + ")"] = data_set
    log.info(idx + " Exit")

def exec_seq(prev_idx, map):
    log.info(prev_idx + " Entry")
    for idx, values in map.items():
            new_idx = prev_idx + "." + idx
            if(values['Type'] == 'Flow'):
                if(values['Execution'] == 'Sequential'):
                    exec_seq(new_idx, values['Activities'])
                else:
                    exec_par(new_idx, values['Activities'])
            else:
                tasks_exec(new_idx,values,"Serial")
    log.info(prev_idx + " Exit")

def exec_par(prev_idx, map):
    iterables = []
    log.info(prev_idx + " Entry")
    for idx, values in map.items():
            new_idx = prev_idx + "." + idx
            if(values['Type'] == 'Flow'):
                if(values['Execution'] == 'Sequential'):
                    threads = threading.Thread(target=exec_seq, args=(new_idx,values['Activities'],))
                else:
                    threads = threading.Thread(target=exec_par, args=(new_idx,values['Activities'],))
                iterables.append(threads)
            else:
                threads = threading.Thread(target=tasks_exec, args=(new_idx,values,"Parallel",))
                iterables.append(threads)
    for item in iterables:
        item.start()
    for item in iterables:
        item.join()
    log.info(prev_idx + " Exit")

if __name__ == '__main__':
    parser = YamlParser('./Examples/Milestone3/Milestone3A.yaml')
    data = parser.open()
    curr_items = []
    curr_items.append(data)
    for map in curr_items:
        for idx, values in map.items():
            if(values['Type'] == 'Flow'):
                if(values['Execution'] == 'Sequential'):
                    exec_seq(idx, values['Activities'])
                else:
                    exec_par(idx, values['Activities'])


    # df0 = pd.read_csv('./Examples/Milestone3/Milestone3A_BinningRule_500.csv')
    # df1 = pd.read_csv('./Examples/Milestone3/Milestone3A_BinningRule_501.csv')
    # dict = {}
    # stx = str(df0['RULE'])
    # res1 = [int(i) for i in stx.split() if i.isdigit()]
    # mn = mx = -1
    # if(len(res1) > 2):
    #     mn = res1[1]
    #     mx = res1[2]
    # else:
    #     if(stx.find('<') != -1):
    #         mx = res1[1]
    #     if(mn == -1): mn = 0
    #     elif(mx == -1): mx = 1e9
    # dict[500] = (mn, mx)

    # stx = str(df1['RULE'])
    # res1 = [int(i) for i in stx.split() if i.isdigit()]
    # mn = mx = -1
    # if(len(res1) > 2):
    #     mn = res1[1]
    #     mx = res1[2]
    # else:
    #     if(stx.find('<') != -1):
    #         mx = res1[1]
    #     elif(stx.find('>') != -1):
    #         mn = res1[1]
    #     if(mn == -1): mn = 0
    #     elif(mx == -1): mx = 1e9
    # dict[501] = (mn, mx)

    # print(dict)