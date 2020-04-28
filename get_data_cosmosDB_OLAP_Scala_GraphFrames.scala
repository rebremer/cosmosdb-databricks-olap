// Databricks notebook source
// DBTITLE 1,Get data from CosmosDB for OLAP with Scala and GraphFrames
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.graphframes._

// Databricks notebook source
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark.config.Config
import org.apache.spark.sql.SparkSession

val edgesConfig = Config(Map(
      "Endpoint" -> "https://<<your cosmosdb>>.documents.azure.com:443/",
      "Masterkey" -> "<<your key>>",
      "Database" -> "PeopleDB",
      "Collection" -> "friends",
      "query_pagesize" -> "1000",
      "consistencylevel" -> "Eventual",
      "query_custom" -> "select * from c where c._isEdge = true"
))

var edges = spark.read.cosmosDB(edgesConfig)
edges.createOrReplaceTempView("edges")
//edges = edges.select(("_vertexId").as.("src"), ("_sink").as.("dst"), ("_sink").as.("relationship"))
edges = edges.select("_vertexId", "_sink", "label")
edges = edges.withColumnRenamed("_vertexId", "src")
edges = edges.withColumnRenamed("_sink", "dst")
edges = edges.withColumnRenamed("label", "relationship")
//edges = edges.withColumnRenamed("_sink", "dst")
//print(edges.schema)

val verticesConfig = Config(Map(
    "Endpoint" -> "https://<<your cosmosdb>>.documents.azure.com:443/",
    "Masterkey" -> "<<your key>>",
    "Database" -> "PeopleDB",
    "Collection" -> "friends",
    "query_pagesize" -> "1000",
    "consistencylevel" -> "Eventual",
    "query_custom" -> "select * from c where NOT IS_DEFINED(c._isEdge)"
))

var vertices = spark.read.cosmosDB(verticesConfig)
vertices.createOrReplaceTempView("vertices")
vertices = vertices.select($"age".getItem(0).getItem("_value").as("age"),$"id", $"name")

print(vertices.schema)

import org.graphframes.GraphFrame

val g = GraphFrame(vertices, edges)

// COMMAND ----------

// DBTITLE 1,Show data in DataFrames
g.vertices.show()
g.edges.show()

// COMMAND ----------

// DBTITLE 1,Simple queries on GraphFrames
// import Spark SQL package
import org.apache.spark.sql.DataFrame

// Get a DataFrame with columns "id" and "inDeg" (in-degree)
val vertexInDegrees: DataFrame = g.inDegrees

// Find the youngest user's age in the graph.
// This queries the vertex DataFrame.
g.vertices.groupBy().min("age").show()

// Count the number of "follows" in the graph.
// This queries the edge DataFrame.
val numFollows = g.edges.filter("relationship = 'follow'").count()

// COMMAND ----------

// DBTITLE 1,More advanced queries on GraphFrames, e.g. LPA base for community detection
// See also https://graphframes.github.io/graphframes/docs/_site/user-guide.html
val result = g.labelPropagation.maxIter(5).run()
result.select("id", "label").show()

// COMMAND ----------


