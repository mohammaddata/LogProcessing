import os
import glob
from change_model import run
import numpy as np
import pandas as pd
import csv
from neo4jhandler import Neo4jHandler
import json


def csv_to_json(csvFilePath):
    jsonArray = []

    # read csv file
    with open(csvFilePath, encoding='utf-8') as csvf:
        # load csv file data using csv library's dictionary reader
        csvReader = csv.DictReader(csvf)

        # convert each csv row into python dict
        for row in csvReader:
            # add this python dict to json array
            jsonArray.append(row)
    return jsonArray


def retrain(folder_flag, name):
    """ model modification report

        :param folder_flag: determines if the input is folder or file/ 1 for folder and 0 for file
        :param name: name of the file or folder containing csv files
        :return: nothing but save model status to file system. if model status is 1 model is changed.
    """
    print("start processing ...")
    if folder_flag == 1:
        files = glob.glob('./' + f"{name}" + "/*")  # test
        change_model_flag = np.zeros(len(files))
        counter = 0
        for f in files:
            if "csv" not in f:
                print("not a csv file")
                exit(0)
            else:
                jsonArray = csv_to_json(f)  # test
                change_model_flag[counter] = run(jsonArray)
                counter += 1

    else:
        counter = 0
        change_model_flag = np.zeros(1)
        if "csv" not in name:
            print("not a csv file")
            exit(0)
        else:
            jsonArray = csv_to_json(name)
            change_model_flag[counter] = run(jsonArray)
            counter += 1

    # df = pd.DataFrame(change_model_flag)
    # filename = 'model_status.csv'  # with tx
    # df.to_csv(filename)
    if sum(change_model_flag) > 0:
        print("model changed!")
    else:
        print("nothing changed!")


print('Enter the foldername(containing csv files)/filename(csv):')
folder_flags = 0
input_name = input()
if os.path.isdir(input_name):
    folder_flags = 1
retrain(folder_flags, input_name)

# render graph

print("making graph...")

credentials = {
    "uri": "bolt://localhost:7687",
    "userName": "neo4j",
    "password": "test"
}
neo = Neo4jHandler(credentials)
connections = np.array(pd.read_csv("./model/graph_model/" + "connections.csv", index_col=[0]))
nodes = np.array(pd.read_csv("./model/graph_model/" + "nodes.csv", index_col=[0]))
start = np.array(pd.read_csv("./model/graph_model/" + "start.csv", index_col=[0]))
end = np.array(pd.read_csv("./model/graph_model/" + "end.csv", index_col=[0]))
flow_index = np.array(pd.read_csv("./model/graph_model/" + "flow_index.csv", index_col=[0]))
unique_flow_index = []
final_connection = []
for i in range(len(connections)):
    unique_connection_i = np.unique(connections[i])
    unique_flow_index_i = []
    for k in range(len(unique_connection_i)):
        if str(unique_connection_i[k]) != "nan":
            if int(unique_connection_i[k]) != 100:
                unique_flow_index_i_k = []
                for j in range(len(flow_index[i])):
                    if unique_connection_i[k] == connections[i][j]:
                        unique_flow_index_i_k.append(flow_index[i][j])

                unique_flow_index_i.append(unique_flow_index_i_k)

    unique_flow_index.append(unique_flow_index_i)
    new_connection = []
    for m in range(len(unique_connection_i)):
        if str(unique_connection_i[m]) != "nan":
            if int(unique_connection_i[m]) != 100:
                new_connection.append(unique_connection_i[m])
    final_connection.append(new_connection)

neo.clear_graph()
count = 0
for i in range(len(nodes)):
    neo.create_new_node(str(nodes[i][0]).replace("-", "_"), str(start[i][0]), str(end[i][0]))
    count += 1
for i in range(len(nodes)):
    for j in range(len(final_connection[i])):
        neo.create_new_edge(
            str(start[int(final_connection[i][j])][0]),
            str(end[int(final_connection[i][j])][0]),
            str(nodes[int(final_connection[i][j])][0]).replace("-", "_"),
            str(start[i][0]),
            str(end[i][0]),
            str(nodes[i][0]).replace("-", "_"),
            "Next",
            "Next")
        # str(np.unique(np.array(unique_flow_index[i][j]))))

print("processing finished")
