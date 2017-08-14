```python
from pydb import DatabricksAPI

# init
db = DatabricksAPI(host,user,password)

# list all clusters (clusters/list)
clusters = db.list_clusters()

# get a cluster (clusters/get?cluster_id=...)
cluster_info = db.get_cluster({cluster_id:'cluster-123'})

# create a cluster 
config = {
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
result = db.create_cluster(json=config)
```
