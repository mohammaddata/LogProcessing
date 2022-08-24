import pandas as pd
import os
import glob

from logfunction import LogsFunction
import numpy as np
import json


def run(fileName):
    """ check and detect anomalies

        :param filename: name of the json file
        :return: an excel file with (logstatus.csv), in the excel file anomalies are determined with 1.
    """
    # load model
    connections = pd.read_csv("./model/graph_model/" + "connections.csv")
    nodes = pd.read_csv("./model/graph_model/" + "nodes.csv")
    start = pd.read_csv("./model/graph_model/" + "start.csv")

    extension = fileName.split(".")[1]
    if extension == 'json':
        f = open(fileName)
        json_read = json.load(f)
        print("start processing ...")
        files = glob.glob('./anomalies/*')
        for f in files:
            print(f)
            os.remove(f)

        new_json = []
        for i in range(len(json_read)):
            if json_read[i]["user_agent"] != "Swift":
                new_json.append(json_read[i])

        # make standard logs (for example p-get-a)
        pure_logs, transaction_ids, datetime = LogsFunction().purifylogs(new_json)
        # extract flows
        logs_flows, datetime, tx = LogsFunction().extractFlows(pure_logs, transaction_ids, datetime)
        # sort logs
        sorted_log_flows = LogsFunction().sortlogs(logs_flows, datetime)


        # detect anomalies
        count = np.zeros(len(sorted_log_flows), dtype=object)
        temp_tx = []
        for i in range(len(sorted_log_flows)):
            count[i] = 0
            priority = []
            for u in range(len(sorted_log_flows[i])):
                for j in range(len(nodes)):
                    if sorted_log_flows[i][u] == nodes["0"][j]:
                        priority.append(j)

            sorted_priority = []
            for b in range(len(priority)):
                sorted_priority.append(priority[len(priority) - 1 - b])

            if start["0"][sorted_priority[0]] == 0:
                count[i] = 1
                temp_tx.append(tx[i][0])
                continue

            counter = np.zeros(len(sorted_priority) - 1)
            for k in range(len(sorted_priority) - 1):
                for m in range(len(connections[f"{priority[k]}"])):
                    if str(connections[f"{m}"][priority[k]]) == 'nan':
                        continue
                    if int(connections[f"{m}"][priority[k]]) == int(priority[k + 1]):
                        if counter[k] != 1:
                            counter[k] = counter[k] + 1

            if (sum(counter) != len(sorted_priority) - 1) or (sum(counter) == 0):
                count[i] = 1
                temp_tx.append(tx[i][0])
                continue
            else:
                count[i] = 0
                temp_tx.append(tx[i][0])
                continue
        temp_tx = np.array(temp_tx)
        logs_status = np.concatenate((count.reshape(-1, 1), temp_tx.reshape(-1, 1)), axis=1)
        df = pd.DataFrame(logs_status)
        # save logs status in logs_status.csv, 1 for anomalies
        filename = 'logs_status.csv'
        df.to_csv(filename)

        # save anomalies in anomalies folder
        for i in range(len(tx)):
            exact_logs = []
            if count[i]:
                for j in range(len(json_read)):
                    if json_read[j]["transaction_id"] == tx[i][0]:
                        exact_logs.append(json_read[j])
                df = pd.DataFrame(exact_logs)
                filename = "./anomalies/" + str(i) + "________________" + str(tx[i][0]) + '.csv'
                df.to_csv(filename)

        print("processing finished")


print('Enter the filename:')
filename = input()
try:
    extension = filename.split(".")[1]
    if extension != "json":
        print("not a json file")
        exit(0)

    run(filename)
except:
    print(f"There is an error, please check the following items:\n")
    print("1 - The file must be exist in this directory\n")
    print("2- The input file must be a json file. please check the extension of the file and make sure that the data "
          "structure of file is json.")
