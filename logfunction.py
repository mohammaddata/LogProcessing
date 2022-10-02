import numpy as np
import copy
import pandas as pd


class LogsFunction:

    def purifylogs(self, json_read):
        """ convert logs to standard form (for example p-get-a)

            :param json_read: json data of logs
            :return: standard logs, transaction Id, datetime of logs.
        """
        pure_logs = []
        transaction_ids = []
        datetime = []
        for i in range(len(json_read)):
            # convert logs to standard format based on action (Head- Get - Put - Delete) in the logs
            if "HEAD" in json_read[i]["request_method"]:
                try:
                    str1 = json_read[i]["user_agent"]
                    str2 = json_read[i]["programname"]
                    pure_logs.append(self.define(str1) + "-" + "HEAD" + "-" + self.define(str2))
                    transaction_ids.append(json_read[i]['transaction_id'])
                    datetime.append(json_read[i]['@timestamp'])
                    continue
                except:
                    pass

            if "GET" in json_read[i]["request_method"]:
                try:
                    str1 = json_read[i]["user_agent"]
                    str2 = json_read[i]["programname"]
                    pure_logs.append(self.define(str1) + "-" + "GET" + "-" + self.define(str2))
                    transaction_ids.append(json_read[i]['transaction_id'])
                    datetime.append(json_read[i]['@timestamp'])
                    continue
                except:
                    pass

            if "DELETE" in json_read[i]["request_method"]:
                try:
                    str1 = json_read[i]["user_agent"]
                    str2 = json_read[i]["programname"]
                    pure_logs.append(self.define(str1) + "-" + "DELETE" + "-" + self.define(str2))
                    transaction_ids.append(json_read[i]['transaction_id'])
                    datetime.append(json_read[i]['@timestamp'])
                    continue
                except:
                    pass

            if "PUT" in json_read[i]["request_method"]:
                try:
                    str1 = json_read[i]["user_agent"]
                    str2 = json_read[i]["programname"]
                    pure_logs.append(self.define(str1) + "-" + "PUT" + "-" + self.define(str2))
                    transaction_ids.append(json_read[i]['transaction_id'])
                    datetime.append(json_read[i]['@timestamp'])
                    continue
                except:
                    pass

        return np.array(pure_logs), np.array(transaction_ids), np.array(datetime)

    @staticmethod
    def define(string):
        """ convert keywords to standard form

            :param string: not standard keyword
            :return: standard keyword
        """
        # convert keywords to standard form (abbreviation name of the server)
        if "proxy" in string:
            return "p"
        if "account" in string:
            return "a"
        if "container" in string:
            return "c"
        if "object" in string:
            return "o"
        if "python" in string:
            return "s"
        if "Swift" in string:
            return "s"

    @staticmethod
    def extractFlows(pure_logs, transaction_ids, datetime):
        """ extract flows from bul of logs

            :param pure logs: logs in standard form (for egxample p-get-a)
            :param transaction_ids: transaction id of pure logs
            :param datetime: datetime of pure logs
            :return: final_logs: array of flows
            :return: transaction_ids: transaction id of pure logs
            :return: datetime: datetime of pure logs
        """

        final_logs = []
        final_date_time = []
        final_tx = []
        unique_transaction_ids = np.unique(transaction_ids)
        # extract flows based on the transaction id (transactions with equal transaction id are in one process)
        for i in range(len(unique_transaction_ids)):
            temp_logs = []
            temp_datetime = []
            temp_tx = []
            for j in range(len(transaction_ids)):
                if transaction_ids[j] == unique_transaction_ids[i]:
                    temp_logs.append(pure_logs[j])
                    temp_datetime.append(datetime[j])
                    temp_tx.append(transaction_ids[j])

            final_logs.append(temp_logs)
            final_date_time.append(temp_datetime)
            final_tx.append(temp_tx)

        return final_logs, final_date_time, final_tx

    def clusterlogs(self, sorted_log_flows, tx):
        """ cluster similar flows
            :param sorted_log_flows: sorted flows based on datetime
            :param tx: transaction id of pure logs
            :return: his: one sample of each cluster
            :return: his_tx: transaction id of the sample cluster
            :return: anomalies: logs anomalies
        """
        his = [sorted_log_flows[0]]
        his_tx = [tx[0]]
        # choose one representative from a cluster group.
        for i in range(len(sorted_log_flows)):
            found = 0
            for j in range(len(his)):
                if len(his[j]) == len(sorted_log_flows[i]):
                    count = 0
                    for k in range(len(his[j])):
                        if his[j][k] == sorted_log_flows[i][k]:
                            count = count + 1
                            continue
                    if count == len(his[j]):
                        found = 1

            if found == 0 and len(sorted_log_flows[i]) > 1:
                his.append(sorted_log_flows[i])
                his_tx.append(tx[i])

        count = np.zeros(len(his))
        for i in range(len(his)):
            for j in range(len(sorted_log_flows)):
                flag = 0
                if len(his[i]) == len(sorted_log_flows[j]):
                    for k in range(len(his[i])):
                        if his[i][k] != sorted_log_flows[j][k]:
                            flag = 1
                    if flag == 0:
                        count[i] = count[i] + 1

        anomalies = []
        count = count / len(tx)
        for i in range(len(count)):
            if count[i] < 0.004:
                anomalies.append(his[i])

        return his, his_tx, anomalies

    @staticmethod
    def sortlogs(logs_flows, datetime):
        """ sort logs based on datetime
            :param datetime: datetime of flows
            :param logs_flows: flow of logs
            :return: logs_flows: sorted flows of logs
        """
        # sort logs based on the datetime field in ascending order.
        temp_date_time_sorted = np.empty(len(logs_flows), dtype=object)
        for i in range(len(logs_flows)):
            temp_logs_flows = copy.deepcopy(logs_flows[i])
            temp_date_time = copy.deepcopy(datetime[i])
            for k in range(len(temp_date_time)):
                try:
                    temp_date_time[k] = int((temp_date_time[k].split("T")[1].split(":")[0])) * 3600 + (
                        int(temp_date_time[k].split("T")[1].split(":")[1])) * 60 + \
                                        float(temp_date_time[k].split("T")[1].split(":")[2].split("Z")[0])
                    # int((temp_date_time[k].split("2022:")[1].split(":")[0])) * 3600 + (
                    # int(temp_date_time[k].split("2022:")[1].split(":")[1])) * 60 + \
                    # int(temp_date_time[k].split("2022:")[1].split(":")[2])
                except:
                    temp_date_time[k] = 0

            sorted_temp_date_time = np.array([y for y, x in sorted(zip(temp_date_time, temp_logs_flows))])
            sorted_temp_logs_flows = np.array([x for y, x in sorted(zip(temp_date_time, temp_logs_flows))])

            logs_flows[i] = sorted_temp_logs_flows
            temp_date_time_sorted[i] = sorted_temp_date_time

        return logs_flows, temp_date_time_sorted

    @staticmethod
    def extractconnections(logs_flows):
        """ extract node and connections for neo4j
            :param logs_flows: flow of logs
            :return nodes: nodes of graph
            :return flow_index: flow of index
            :return: connections: connections in graph
            :return start: if a node is start
            :return: end: if a node is end
        """
        nodes = []
        for i in range(len(logs_flows)):
            for j in range(len(logs_flows[i])):
                if logs_flows[i][j] not in nodes:
                    nodes.append(logs_flows[i][j])
        nodes = np.array(nodes)
        start = np.empty(len(nodes), dtype=object)
        end = np.empty(len(nodes), dtype=object)
        # extract main nodes of the graph
        for i in range(len(nodes)):
            flag_end = 0
            flag_start = 0
            for j in range(len(logs_flows)):
                if nodes[i] == logs_flows[j][0]:
                    end[i] = "1"
                    flag_end = 1
                if nodes[i] == logs_flows[j][len(logs_flows[j]) - 1]:
                    start[i] = "1"
                    flag_start = 1

            if flag_end == 0:
                end[i] = "0"
            if flag_start == 0:
                start[i] = "0"

        connections = []
        flow_index = []
        # extract connections of the graph
        for i in range(len(nodes)):
            temp_connections = []
            temp_flow_index = []
            for k in range(len(logs_flows)):
                for p in range(len(logs_flows[k])):
                    if nodes[i] == logs_flows[k][p] and p < len(logs_flows[k]) - 1:
                        index_of_node = np.where(nodes == logs_flows[k][p + 1])[0]
                        temp_connections.append(index_of_node[0])
                        temp_flow_index.append(k)
            connections.append((np.array(temp_connections)))
            flow_index.append((np.array(temp_flow_index)))

        return nodes, connections, flow_index, start, end

    def generate_excel_pure_logs(self, clustered_sorted_log_flows, tx):
        """ extract excel of pure logs
            :param clustered_sorted_log_flows: clustered_sorted_log_flows
            :return tx: transaction ids

        """
        for i in range(len(clustered_sorted_log_flows)):
            df = pd.DataFrame(clustered_sorted_log_flows[i])
            filename = str(i) + '.csv'
            df.to_csv(filename)

    def generate_excel_exact_logs(self, json_data, tx):
        """ extract excel of exact logs
            :param json_data: json form of data
            :return tx: transaction ids
        """
        for i in range(len(tx)):
            exact_logs = []
            for j in range(len(json_data)):
                if json_data[j]["transaction_id"] == tx[i][0]:
                    exact_logs.append(json_data[j])
            df = pd.DataFrame(exact_logs)
            filename = str(i) + "________________" + str(tx[i][0]) + '.csv'
            df.to_csv(filename)
