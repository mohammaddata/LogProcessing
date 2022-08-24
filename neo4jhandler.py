from neo4j import GraphDatabase
from typing import Dict


class Neo4jHandler:
    graphDB_Driver = None

    def __init__(self, config: Dict) -> None:
        uri = config["uri"]
        userName = config["userName"]
        password = config["password"]
        self.graphDB_Driver = GraphDatabase.driver(
            uri, auth=(userName, password)
        )

    def create_new_node(self, node, start, end):
        """ create node for neo4j

            :param node: node name
            :param start: determining if a node if start
            :param end: determining if a node if end
            :return: create_node: node format in create_node
        """
        useragent = node.split("_")[0]
        method = node.split("_")[1]
        programname = node.split("_")[2]

        if useragent == "p":
            useragent = "proxy_server"
        elif useragent == "a":
            useragent = "account_server"
        elif useragent == "o":
            useragent = "object_server"
        elif useragent == "c":
            useragent = "container_server"
        else:
            useragent = "s"

        if programname == "p":
            programname = "proxy_server"
        elif programname == "a":
            programname = "account_server"
        elif programname == "o":
            programname = "object_server"
        elif programname == "c":
            programname = "container_server"
        else:
            programname = "s"

        if (start == '1') and (end == '1'):
            create_node = \
                "CREATE \n" + \
                f"({node}:both " + "{ name: " + f"\"{node}\" , start: " + f"\"{start}\", programmname: " + f"\"{programname}\" , end: " + f"\"{end}\"  , method: " + f"\"{method}\" , useragent: " + f"\"{useragent}\"" + "})"
        if (start == '0') and (end == '1'):
            create_node = \
                "CREATE \n" + \
                f"({node}:end " + "{ name: " + f"\"{node}\" , start: " + f"\"{start}\", programmname: " + f"\"{programname}\" , end: " + f"\"{end}\"  , method: " + f"\"{method}\" , useragent: " + f"\"{useragent}\"" + "})"
        if (start == '1') and (end == '0'):
            create_node = \
                "CREATE \n" + \
                f"({node}:start " + "{ name: " + f"\"{node}\" , start: " + f"\"{start}\", programmname: " + f"\"{programname}\" , end: " + f"\"{end}\"  , method: " + f"\"{method}\" , useragent: " + f"\"{useragent}\"" + "})"
        if (start == '0') and (end == '0'):
            create_node = \
                "CREATE \n" + \
                f"({node}:mid " + "{ name: " + f"\"{node}\" , start: " + f"\"{start}\", programmname: " + f"\"{programname}\" , end: " + f"\"{end}\"  , method: " + f"\"{method}\" , useragent: " + f"\"{useragent}\"" + "})"
        with self.graphDB_Driver.session() as graphDB_Session:
            graphDB_Session.run(create_node)

        return create_node

    def create_new_edge(self, start1, end1, node_1, start2, end2, node_2, edge, edge_name):

        """ create new edge for neo4j
            :param start1: check if first node is start
            :param end1: check if first node is end
            :param node_1: first node name
            :param start2: check if second node is start
            :param end2: check if second node is end
            :param node_2: second node name
            :param edge: endge features
            :param edge_name: determining if a node if end
            :return: create_edge: node create_edge in create_node
        """

        if (start1 == '1') and (end1 == '1'):
            name1 = 'both'
        if (start1 == '1') and (end1 == '0'):
            name1 = "start"
        if (start1 == '0') and (end1 == '1'):
            name1 = "end"
        if (start1 == '0') and (end1 == '0'):
            name1 = "mid"

        if (start2 == '1') and (end2 == '1'):
            name2 = 'both'
        if (start2 == '1') and (end2 == '0'):
            name2 = "start"
        if (start2 == '0') and (end2 == '1'):
            name2 = "end"
        if (start2 == '0') and (end2 == '0'):
            name2 = "mid"

        create_edge = \
            "MATCH (u: " + f"{name1}" + " {name:" + f"'{node_1}'" + "}), (r: " + f"{name2}" + "  {name: " + \
            f"'{node_2}'" + "}) " + \
            f"CREATE(u)-[:{edge}" + \
            " {flow_number: " + f"'{edge_name}'" + "}]->(r)"

        with self.graphDB_Driver.session() as graphDB_Session:
            graphDB_Session.run(create_edge)

        return create_edge

    def clear_graph(self):
        """ clear graph neo4j before making new graph for resolving overwriting

        """
        cqldelete1 = "match (a) -[r] -> () delete a, r"
        cqldelete2 = "match (a) delete a"
        with self.graphDB_Driver.session() as graphDB_Session:
            graphDB_Session.run(cqldelete1)
            graphDB_Session.run(cqldelete2)
