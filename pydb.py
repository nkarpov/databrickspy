import json
import requests
from requests.auth import HTTPBasicAuth
import time

try:
	from urllib import parse as urlparse
except ImportError:
	import urlparse

CLUSTER_LIST_ENDPOINT = ('GET','api/2.0/clusters/list')
CLUSTER_GET_ENDPOINT = ('GET', 'api/2.0/clusters/get')
CLUSTER_CREATE_ENDPOINT = ('POST', 'api/2.0/clusters/create')
JOBS_RUNS_SUBMIT = ('POST', 'api/2.0/jobs/runs/submit')
JOBS_RUNS_GET = ('GET', 'api/2.0/jobs/runs/get')


class DatabricksAPI:

	def __init__(self, host, user, password):
		self._host = self._parse_host(host)
		self._user = user
		self._password = password
		self._polling_period = 30

	def get_running_clusters(self):
		raw = self.list_clusters()
		running_clusters = [cluster for cluster in raw['clusters'] if cluster['state']=='RUNNING']
		return running_clusters


	def list_clusters(self):
		#result = request_func.get('https://{host}/{endpoint}'.format(host=self._host,endpoint=endpoint) , auth=HTTPBasicAuth(self._user, self._password))
		json = {}
		response = self._do_api_call(CLUSTER_LIST_ENDPOINT, json)
		return response.json()

	def create_cluster(self, json):
		# create a cluster
		cluster_id = self._do_api_call(CLUSTER_CREATE_ENDPOINT, json).json()['cluster_id']
		
		# poll the cluster until state is 'RUNNING'
		while True:
			time.sleep(self._polling_period)
			response = self.get_cluster({'cluster_id':cluster_id})
			if response['state'] == 'RUNNING':
				break
			elif response['state'] == 'TERMINATING' or response['state'] == 'TERMINATED':
				raise Exception("Cluster failed to start")

		return cluster_id

	def get_cluster(self, json):
		response = self._do_api_call(CLUSTER_GET_ENDPOINT, json)
		return response.json()

	def run_submit(self, json):
		# submit the job
		run_id = self._do_api_call(JOBS_RUNS_SUBMIT, json).json()['run_id']

		# poll job status until state is 'TERMINATED'
		while True:
			time.sleep(self._polling_period)
			response = self.get_run({'run_id': run_id})
			if response['state']['life_cycle_state'] == 'TERMINATED':
				print response
				break
			elif response['state']['life_cycle_state'] == 'SKIPPED' or response['state']['life_cycle_state'] == 'INTERNAL_ERROR':
				print result
				raise Exception("Job failed to complete")

		return run_id

	def get_run(self, json):
		response = self._do_api_call(JOBS_RUNS_GET, json)
		return response.json()

	def _do_api_call(self, endpoint_info, json):
		method, endpoint = endpoint_info

		if method == 'GET':
			request_func = requests.get
		elif method == 'POST':
			request_func = requests.post
		else:
			raise Exception("Unsupported method: " + method)

		response = request_func('https://{host}/{endpoint}'.format(host=self._host,endpoint=endpoint) , auth=HTTPBasicAuth(self._user, self._password), json = json)
		return response

	def _parse_host(self, host):
		urlparse_host = urlparse.urlparse(host).hostname
		if urlparse_host:
			# In this case, host = https://xx.cloud.databricks.com
			return urlparse_host
		else:
			# In this case, host = xx.cloud.databricks.com
			return host