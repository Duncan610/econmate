# Cluster Configuration

**Databricks Runtime:** 15.4 LTS (Spark 3.5.0, Scala 2.12)
**Cluster mode:** Single node (Serverless)
**Python version:** 3.11+

**Pre-installed by Databricks Runtime:**
- PySpark 3.5
- Delta Lake 3.x
- dbutils
- Unity Catalog client

**Additional dependencies:** requests==2.31.0 (see requirements.txt)

**Unity Catalog namespace:** workspace.default
**Table read pattern:** spark.table("workspace.default.<tablename>")
**Table write pattern:** df.write.format("delta").saveAsTable("workspace.default.<tablename>")

**To reproduce this pipeline:**
1. Create a Databricks workspace with Unity Catalog enabled
2. Create a Single Node cluster with Runtime 15.4 LTS
3. Clone this repo into your Databricks workspace via Git integration
4. Run notebooks in order: bronze/ → silver/ → gold/
5. Run 99_pipeline_tests to validate all outputs
