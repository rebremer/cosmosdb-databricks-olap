# Databricks notebook source
# DBTITLE 1,Insert data to CosmosDB for OLTP using Python and GremlinAPI
# see https://github.com/Azure-Samples/azure-cosmos-db-graph-python-getting-started/blob/master/connect.py
from gremlin_python.driver import client, serializer
import sys
import traceback

client = client.Client('wss://<<your cosmosdb>>.gremlin.cosmos.azure.com:443/', 'g',
                           username="/dbs/PeopleDB/colls/friends",
                           password="<<your key>>",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                       )

def insert_vertices(client):
    for query in _gremlin_insert_vertices:
        print("\tRunning this Gremlin query:\n\t{0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this vertex:\n\t{0}\n".format(
                callback.result().one()))
        else:
            print("Something went wrong with this query: {0}".format(query))
    print("\n")


def insert_edges(client):
    for query in _gremlin_insert_edges:
        print("\tRunning this Gremlin query:\n\t{0}\n".format(query))
        callback = client.submitAsync(query)
        if callback.result() is not None:
            print("\tInserted this edge:\n\t{0}\n".format(
                callback.result().one()))
        else:
            print("Something went wrong with this query:\n\t{0}".format(query))
    print("\n")

_gremlin_insert_vertices = [
    "g.addV('person').property('id', 'a').property('name', 'Alice').property('age', 34)",
    "g.addV('person').property('id', 'c').property('name', 'Charlie').property('age', 30)",
    "g.addV('person').property('id', 'd').property('name', 'David').property('age', 29)",
    "g.addV('person').property('id', 'e').property('name', 'Esther').property('age', 32)",
    "g.addV('person').property('id', 'f').property('name', 'Fanny').property('age', 36)",
    "g.addV('person').property('id', 'g').property('name', 'Gab').property('age', 60)"
]

_gremlin_insert_edges = [
    "g.V().hasLabel('person').has('id', 'a').addE('friend').to(g.V().hasLabel('person').has('id', 'b'))",
    "g.V().hasLabel('person').has('id', 'b').addE('follow').to(g.V().hasLabel('person').has('id', 'c'))",
    "g.V().hasLabel('person').has('id', 'c').addE('follow').to(g.V().hasLabel('person').has('id', 'b'))",
    "g.V().hasLabel('person').has('id', 'f').addE('follow').to(g.V().hasLabel('person').has('id', 'c'))",
    "g.V().hasLabel('person').has('id', 'e').addE('follow').to(g.V().hasLabel('person').has('id', 'f'))",
    "g.V().hasLabel('person').has('id', 'e').addE('friend').to(g.V().hasLabel('person').has('id', 'd'))",
    "g.V().hasLabel('person').has('id', 'd').addE('friend').to(g.V().hasLabel('person').has('id', 'a'))",
    "g.V().hasLabel('person').has('id', 'a').addE('friend').to(g.V().hasLabel('person').has('id', 'e'))"
]

insert_vertices(client)
insert_edges(client)

# COMMAND ----------

# DBTITLE 1,Drop graph
from gremlin_python.driver import client, serializer
import sys
import traceback


client = client.Client('wss://<<your cosmosdb>>.gremlin.cosmos.azure.com:443/', 'g',
                           username="/dbs/PeopleDB/colls/friends",
                           password="<<your key>>",
                           message_serializer=serializer.GraphSONSerializersV2d0()
                       )

_gremlin_cleanup_graph = "g.V().drop()"

def cleanup_graph(client):
    print("\tRunning this Gremlin query:\n\t{0}".format(
        _gremlin_cleanup_graph))
    callback = client.submitAsync(_gremlin_cleanup_graph)
    if callback.result() is not None:
        print("\tCleaned up the graph!")
    print("\n")
                      
cleanup_graph(client)

# COMMAND ----------


