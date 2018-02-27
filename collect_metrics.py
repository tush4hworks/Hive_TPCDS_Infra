import sys
import requests
from collections import defaultdict
import json

class getQueryMetrics:
	def __init__(self,host,port,query,servicedump,hostdump):
		self.query=query
		self.prefix='http://{}:{}/ws/v1/timeline/metrics'.format(host,port)
		self.host_stats=defaultdict(lambda:defaultdict(lambda:'NA'))
		self.service_stats=defaultdict(lambda:'NA')
		self.servicedump=servicedump
		self.hostdump=hostdump

	def fetch_stats(self,metricType,metricNames,startTime,endTime,dumpfile,hostname=None,precision=None,,appId=None):
		url=self.prefix+'?metricNames={}{}{}{}&startTime={}&endTime={}'.format(metricNames,'&appId='+appId if appId else '','&hostname='+hostname if hostname else '','&precision='+precision if precision else '',startTime,endTime)
		resp=requests.get(url,headers={'Accept':'application/json'})
		if resp.status_code==200:
			self.addToMetrics(metricType,json.loads(resp.content)['metrics'])
		self.dumpStats(metricType,dumpfile)

	def addToMetrics(self,metricType,metricList):
		if metricType=='host':
			for stat in metricList:
				self.host_stats[stat['hostname']][stat['metricname']]=stat['metrics'].values()[0]
		elif metricType=='service':
			for stat in metricList:
				self.service_stats[stat['metricname']]=stat['metrics'].values()[0]

	def dumptofile(self,metricType,dumpfile):
		if metricType=='host':
			self.dumpHostStats(self,dumpfile)
		elif metricType=='service':
			self.dumpServiceStats(self,dumpfile)
	
	def dumpServiceStats(self,dumpfile):
		with open(dumpfile,'a+') as f:
			f.write('query,'+','.join(sorted(self.service_stats.keys()))+'\n')
			f.write(','.join([self.query]+[str(self.service_stats[key]) for key in sorted(self.service_stats.keys())])+'\n')

	def dumpHostStats(self,dumpfile):
		with open(dumpfile,'a+') as f:
			f.write('query,'+','.join(['-'.join([host,metric]) for host in sorted(self.host_stats.keys()) for metric in sorted(self.host_stats[host].keys())])+'\n')
			f.write(','.join([self.query]+[str(self.host_stats[host][metric]) for host in sorted(self.host_stats.keys()) for metric in sorted(self.host_stats[host].keys())])+'\n')

