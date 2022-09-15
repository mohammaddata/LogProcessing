import pandas as pd
import os
import glob

from logfunction import LogsFunction
import numpy as np
import json


def run(json_read):
    """ model modification

        :param filename: name of the json file
        :return: status of operations.
    """
    # load model
    connections = pd.read_csv("./model/graph_model/" + "connections.csv", index_col=[0])
    nodes = pd.read_csv("./model/graph_model/" + "nodes.csv", index_col=[0])
    start = pd.read_csv("./model/graph_model/" + "start.csv", index_col=[0])

    # new_json = []
    # for i in range(len(json_read)):
    #     if json_read[i]["user_agent"] != "Swift":
    #         new_json.append(json_read[i])

    # make standard logs (for example p-get-a)
    pure_logs, transaction_ids, datetime = LogsFunction().purifylogs(json_read)
    # extract flows
    logs_flows, datetime, tx = LogsFunction().extractFlows(pure_logs, transaction_ids, datetime)
    # sort logs
    sorted_log_flows = LogsFunction().sortlogs(logs_flows, datetime)

    # detect anomalies
    anomaly_flag = 0
    count = np.zeros(len(sorted_log_flows), dtype=object)
    for i in range(len(sorted_log_flows)):
        if len(sorted_log_flows) > 1:
            print("please enter a file with one process not more!")
            exit(0)
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
            # repair anomaly
            start["0"][sorted_priority[0]] = 1
            anomaly_flag = 1

        counter = np.zeros(len(sorted_priority) - 1)
        for k in range(len(sorted_priority) - 1):
            for m in range(connections.shape[1]):
                if str(connections[f"{m}"][priority[k]]) == 'nan':
                    continue
                if int(connections[f"{m}"][priority[k]]) == int(priority[k + 1]):
                    if counter[k] != 1:
                        counter[k] = counter[k] + 1
            # repair anomaly
            if counter[k] == 0:
                index_of_new_column = connections.shape[1]
                new_column_list = np.zeros(connections.shape[0])
                for u in range(connections.shape[0]):
                    new_column_list[u] = 100
                    if u == priority[k]:
                        new_column_list[u] = priority[k + 1]
                anomaly_flag = 1
                connections[f"{index_of_new_column}"] = new_column_list

    df = pd.DataFrame(connections)
    filename = './model/graph_model/connections.csv'
    df.to_csv(filename)
    df = pd.DataFrame(start)
    filename = './model/graph_model/start.csv'
    df.to_csv(filename)
    return anomaly_flag

