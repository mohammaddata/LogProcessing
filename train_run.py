# -*- coding: utf-8 -*-
"""
Created on Fri Jan  1 08:47:14 2021

@author: mohammad
"""
import pandas as pd
import numpy as np
from logfunction import LogsFunction
# from draw_cfg import DRAWGRAPH
from neo4jhandler import Neo4jHandler
import json
import re


def run(fileName):
    """ train graph for anomalies, draw graph
        :param filename: name of the json file
    """
    credentials = {
        "uri": "bolt://localhost:7687",
        "userName": "neo4j",
        "password": "test"
    }

    neo = Neo4jHandler(credentials)
    extension = fileName.split(".")[1]
    if extension == 'json':
        f = open(fileName)
        json_read = json.load(f)
        print("start processing....")
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
        clustered_sorted_log_flows, tx, anomalies = LogsFunction().clusterlogs(sorted_log_flows, tx)
        LogsFunction().generate_excel_exact_logs(json_read, tx)
        # extract nodes and connections for graph
        nodes, connections, flow_index, start, end = LogsFunction().extractconnections(clustered_sorted_log_flows)
        "save model"
        df = pd.DataFrame(nodes)
        filename = 'nodes.csv'
        df.to_csv(filename)

        df = pd.DataFrame(connections)
        filename = 'connections.csv'
        df.to_csv(filename)

        df = pd.DataFrame(start)
        filename = 'start.csv'
        df.to_csv(filename)

        df = pd.DataFrame(end)
        filename = 'end.csv'
        df.to_csv(filename)

        df = pd.DataFrame(flow_index)
        filename = 'flow_index.csv'
        df.to_csv(filename)

        # draw graph
        unique_flow_index = []
        for i in range(len(connections)):
            unique_connection_i = np.unique(connections[i])
            unique_flow_index_i = []
            for k in range(len(unique_connection_i)):
                unique_flow_index_i_k = []
                for j in range(len(connections[i])):
                    if unique_connection_i[k] == connections[i][j]:
                        unique_flow_index_i_k.append(flow_index[i][j])

                unique_flow_index_i.append(unique_flow_index_i_k)

            unique_flow_index.append(unique_flow_index_i)
            connections[i] = unique_connection_i

        neo.clear_graph()
        count = 0
        for i in range(len(nodes)):
            neo.create_new_node(str(nodes[i]).replace("-", "_"), start[i], end[i])
            count += 1
        for i in range(len(nodes)):
            for j in range(len(connections[i])):
                neo.create_new_edge(
                    start[connections[i][j]],
                    end[connections[i][j]],
                    str(nodes[connections[i][j]]).replace("-", "_"),
                    start[i],
                    end[i],
                    str(nodes[i]).replace("-", "_"),
                    "Next",
                    str(np.unique(np.array(unique_flow_index[i][j]))))

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

