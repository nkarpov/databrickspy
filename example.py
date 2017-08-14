from pydb import DatabricksAPI
import json
import os

host = os.environ['DATABRICKS_HOST']
user = os.environ['DATABRICKS_USER']
password = os.environ['DATABRICKS_PASSWORD']

# init this guy
db = DatabricksAPI(host,user,password)

json = {
  "cluster_name": "nickzclusta",
  "spark_version": "2.0.x-scala2.10",
  "node_type_id": "r3.xlarge",
  "spark_conf": {
    "spark.speculation": True
  },
  "aws_attributes": {
    "availability": "SPOT",
    "zone_id": "us-west-2a"
  },
  "num_workers": 1
}

# this will block until cluster is actually running
result = db.create_cluster(json=json)

#result = db.list_clusters()

print result