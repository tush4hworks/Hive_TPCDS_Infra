import sys
import requests
from collections import defaultdict
import json

class getQueryMetrics:
	def __init__(self,host,port,logger):
		self.prefix='http://{}:{}/ws/v1/timeline/metrics'.format(host,port)
		self.host_stats=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:'NA')))
		self.service_stats=defaultdict(lambda:defaultdict(lambda:'NA'))
		self.host_stats_max=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:'NA')))
		self.service_stats_max=defaultdict(lambda:defaultdict(lambda:'NA'))
		self.logger=logger
		self.ifheaderservice=False
		self.ifheaderhost=False
		self.ifheaderservicemax=False
		self.ifheaderhostmax=False

	def fetch_stats(self,query,metricType,metricNames,startTime,endTime,dumpfile,hostname=None,precision=None,appId=None):
		url=self.prefix+'?metricNames={}{}{}{}&startTime={}&endTime={}'.format(metricNames,'&appId='+appId if appId else '','&hostname='+hostname if hostname else '','&precision='+precision if precision else '',startTime,endTime)
		resp=requests.get(url,headers={'Accept':'application/json'})
		self.logger.info(url)
		if resp.status_code==200:
			self.addToMetrics(query,metricType,json.loads(resp.content)['metrics'])
		self.dumptofile(query,metricType,metricNames,dumpfile)

	def addToMetrics(self,query,metricType,metricList):
		if metricType=='host':
			for stat in metricList:
				if len(stat['metrics'].keys())>0:
					self.host_stats[query][stat['hostname']][stat['metricname']]=str(int(sum(stat['metrics'].values())/len(stat['metrics'].keys())))
					self.host_stats_max[query][stat['hostname']][stat['metricname']]=str(max(stat['metrics'].values()))
		elif metricType=='service':
			for stat in metricList:
				if len(stat['metrics'].keys())>0:
					self.service_stats[query][stat['metricname']]=str(int(sum(stat['metrics'].values())/len(stat['metrics'].keys())))
					self.service_stats_max[query][stat['metricname']]=str(max(stat['metrics'].values()))

	def dumptofile(self,query,metricType,metricNames,dumpfile):
		if metricType=='host':
			self.dumpHostStats(query,metricNames,dumpfile)
		elif metricType=='service':
			self.dumpServiceStats(query,metricNames,dumpfile)
	
	def dumpServiceStats(self,query,metricList,dumpfile):
		with open(dumpfile,'a+') as f:
			if not self.ifheaderservice:
				f.write(','.join(['query']+sorted(metricList.split(',')))+'\n')
				self.ifheaderservice=True
			f.write(','.join([query]+[str(self.service_stats[query][key]) for key in sorted(metricList.split(','))])+'\n')
		with open('_'.join(['max',dumpfile]),'a+') as f:
			if not self.ifheaderservicemax:
				f.write(','.join(['query']+sorted(metricList.split(',')))+'\n')
				self.ifheaderservicemax=True
			f.write(','.join([query]+[str(self.service_stats_max[query][key]) for key in sorted(metricList.split(','))])+'\n')

	def dumpHostStats(self,query,metricList,dumpfile):
		with open(dumpfile,'a+') as f:
			if not self.ifheaderhost:
				f.write('query,'+','.join(['-'.join([host,metric]) for host in sorted(self.host_stats[query].keys()) for metric in sorted(metricList.split(','))])+'\n')
				self.ifheaderhost=True
			f.write(','.join([query]+[str(self.host_stats[query][host][metric]) for host in sorted(self.host_stats[query].keys()) for metric in sorted(metricList.split(','))])+'\n')
		with open('_'.join(['max',dumpfile]),'a+') as f:
			if not self.ifheaderhostmax:
				f.write('query,'+','.join(['-'.join([host,metric]) for host in sorted(self.host_stats_max[query].keys()) for metric in sorted(metricList.split(','))])+'\n')
				self.ifheaderhostmax=True
			f.write(','.join([query]+[str(self.host_stats_max[query][host][metric]) for host in sorted(self.host_stats_max[query].keys()) for metric in sorted(metricList.split(','))])+'\n')

