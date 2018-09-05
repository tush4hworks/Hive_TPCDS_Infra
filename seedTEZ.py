import sys
from collections import defaultdict
import modifyConfig
import confectionary
import math
import random
from string import ascii_lowercase
import json

class tezseed:
	def __init__(self,cluserdetails,startwith=2):
		self.confobj=confectionary.confectionary()
		self.store={}
		host,cluster,user,password=cluserdetails
		self.fetcher=modifyConfig.ambariConfig(host,cluster,user,password)
		self.startwith=startwith
		self.baseconf= {
	        "config": {
	          "ambari": {
	             "tez-site": {
	              "tez.runtime.io.sort.mb":None,
	              "tez.container.max.java.heap.fraction":"0.8",
	              "tez.am.resource.memory.mb":None,
	              "tez.task.resource.memory.mb":None,
	              "tez.grouping.min-size":None,
	              "tez.grouping.max-size":None,
	              "tez.runtime.report.partition.stats":"true"
	            },
	            "hive-site": {
	              "hive.tez.input.format":"org.apache.hadoop.hive.ql.io.HiveInputFormat",
	              "hive.tez.container.size":None,
	              "hive.auto.convert.join.noconditionaltask.size":None,
	              "hive.tez.cpu.vcores":None,
	              "hive.exec.parallel":"true",
	              "hive.exec.parallel.thread.number":"16",
	              "hive.exec.reducers.bytes.per.reducer":None,
	              "hive.exec.dynamic.partition.mode":"nonstrict",
	              "hive.compute.query.using.stats":"true",
	              "hive.stats.fetch.column.stats":"true",
	              "hive.mapjoin.hybridgrace.hashtable":"false",
	              "hive.smbjoin.cache.rows":"10000",
	              "hive.mapred.reduce.tasks.speculative.execution":"false",
	            }
	          },
	          "system":[],
	          "hiveconf": {
	            "hive.auto.convert.join": "true",
	            "hive.auto.convert.join.noconditionaltask": "true",
	            "hive.cbo.enable": "true",
	            "hive.stats.autogather": "true",
	            "hive.vectorized.execution.enabled": "true",
	            "hive.vectorized.execution.reduce.enabled": "true",
	            "hive.vectorized.execution.reduce.groupby.enabled": "true",
	            "hive.optimize.index.filter": "true",
	            "hive.tez.exec.print.summary":"true",
	            "hive.enforce.bucketing":"true",
	            "hive.enforce.sorting":"true",
	            "hive.tez.auto.reducer.parallelism":"true",
	            "hive.optimize.sort.dynamic.partition":"true",
	            "hive.optimize.ppd.storage":"true",
	            "hive.optimize.ppd":"true"
	          },
	          "restart": {
	            "components": [
	            ],
	            "services": ["ALL"]
	          }
	        },
	        "name":None
	      }

	def getRequiredProps(self):
		for prop in ["yarn.nodemanager.resource.memory-mb","yarn.nodemanager.resource.cpu-vcores"]:
			self.store[prop]=int(self.ambariFetch("yarn-site",prop))
		self.store["num_nodemanager"]=len(self.fetcher.getHostsRunningComponent("NODEMANAGER"))

	def ambariFetch(self,conf,prop):
		try:
			return self.fetcher.getConfig(conf)[prop]
		except Exception as e:
			print "Can not get value for {} in {} because {}. Please try manual add".format(prop,conf,e.__str__())

	def validConf(self,num_containers,container_mem,max_containers_per_nm):
		if container_mem<4 or container_mem>12 or num_containers<2:
			return False 
		return True
		
	def getCoresForExecutor(self,num_containers,container_mem,cores_per_nm):
		max_container_cores=int(math.floor(cores_per_nm/num_containers))
		while not(float(container_mem/max_container_cores)>=2) and not(max_container_cores==1):
			max_container_cores=max_container_cores-1
		return max_container_cores

	def addConfToObj(self,container_size,container_cores):
		tmp=dict(self.baseconf)
		tmp["config"]["ambari"]["tez-site"]["tez.runtime.io.sort.mb"]=str(int(container_size*1024*0.3))
		tmp["config"]["ambari"]["tez-site"]["tez.am.resource.memory.mb"]=str(int(container_size*1024))
		tmp["config"]["ambari"]["tez-site"]["tez.task.resource.memory.mb"]=str(int(container_size*1024))
		tmp["config"]["ambari"]["tez-site"]["tez.grouping.min-size"]="134217728"
		tmp["config"]["ambari"]["tez-site"]["tez.grouping.max-size"]="1073741824"
		tmp["config"]["ambari"]["hive-site"]["hive.tez.container.size"]=str(int(container_size*1024))
		tmp["config"]["ambari"]["hive-site"]["hive.auto.convert.join.noconditionaltask.size"]=str(int(container_size*(1024**3)*0.27))
		tmp["config"]["ambari"]["hive-site"]["hive.tez.cpu.vcores"]=str(container_cores)
		tmp["config"]["ambari"]["hive-site"]["hive.exec.reducers.bytes.per.reducer"]="268435456"
		tmp["name"]="{}_TEZCONTAINER_{}_VCORES_{}".format(str(container_size)+'GB',str(container_cores),''.join([random.choice(ascii_lowercase) for i in range(5)]))
		self.confobj.addConfig(json.dumps(tmp))

	def saucemix(self,max_containers_per_nm=None,mem_per_nm=None):
		count=0
		if not max_containers_per_nm:
			max_containers_per_nm=int(self.store["yarn.nodemanager.resource.cpu-vcores"])
		if not mem_per_nm:
			mem_per_nm=int(math.floor(int(self.store["yarn.nodemanager.resource.memory-mb"])/(1024)))
		while count<self.startwith:
			for num_containers,container_mem in self.twoproduct(self.getFactors(mem_per_nm)):
				if self.validConf(num_containers,container_mem,max_containers_per_nm):
					self.addConfToObj(container_mem,self.getCoresForExecutor(min(num_containers,max_containers_per_nm),container_mem,max_containers_per_nm))
					count+=1
				if not(num_containers==container_mem):
					if self.validConf(container_mem,num_containers,max_containers_per_nm):
						self.addConfToObj(num_containers,self.getCoresForExecutor(min(container_mem,max_containers_per_nm),num_containers,max_containers_per_nm))
						count+=1
			mem_per_nm=mem_per_nm-1

	def getFactors(self,n):
		factors=[]
		for i in range(1,n+1):
			if n%i==0:
				factors.append(i)
		return factors

	def twoproduct(self,factors):
		return [[factors[i],factors[len(factors)-1-i]] for i in range((len(factors)/2)+len(factors)%2)]

