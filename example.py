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

run_submit_json = { 
  "run_name": "my spark task",
  "new_cluster": {
    "spark_version": "2.2.x-scala2.11",
    "node_type_id": "r3.xlarge",
    "aws_attributes": {
      "availability": "ON_DEMAND"
    },
    "num_workers": 1
  },
  "libraries": [
    {
      "jar": "dbfs:/sparkexample-1.0-SNAPSHOT.jar"
    },
    {
      "maven": {
        "coordinates": "org.jsoup:jsoup:1.7.2"
      }
    }
  ],
  "timeout_seconds": 3600,
  "spark_jar_task": {
    "main_class_name": "com.nickkarpov.scala.MyApp"
  }
}

run_submit_json_existing_cluster = { 
  "run_name": "my spark task",
  "existing_cluster_id": '0815-205623-tyke206',
  "libraries": [
    {
      "jar": "dbfs:/sparkexample-1.0-SNAPSHOT.jar"
    },
    {
      "maven": {
        "coordinates": "org.jsoup:jsoup:1.7.2"
      }
    }
  ],
  "timeout_seconds": 3600,
  "spark_jar_task": {
    "main_class_name": "com.nickkarpov.scala.MyApp"
  }
}

# this will block until cluster is actually running
#result = db.create_cluster(json=json)

#result = db.list_clusters()

result = db.run_submit(run_submit_json_existing_cluster)

print result