import sys
import modifyConfig
from collections import defaultdict
import random
import math
import confectionary
from string import ascii_lowercase
import json

class sparkseed:
	
	def __init__(self,clusterdetails,startwith=2):
		self.confobj=confectionary.confectionary()
		self.store={}
		host,cluster,user,password=clusterdetails
		self.fetcher=modifyConfig.ambariConfig(host,cluster,user,password)
		self.startwith=startwith
		self.baseconf={
	        "config": {
	          "ambari": {
	            "spark2-thrift-sparkconf": {
	              "spark.executor.instances":None,
	              "spark.driver.memory":None,
	              "spark.executor.memory":None,
	              "spark.driver.cores":None,
	              "spark.executor.cores":None,
	              "spark.memory.fraction":None,
	              "spark.executor.extraJavaOptions":None,
	              "spark.dynamicAllocation.enabled":None
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
	            "hive.optimize.index.filter": "true"
	          },
	          "restart": {
	            "components": [

	            ],
	            "services": ["SPARK2"]
	          }
	        },
	        "name":None
	      }

	def getRequiredProps(self):
		for prop in ["yarn.nodemanager.resource.memory-mb","yarn.nodemanager.resource.cpu-vcores"]:
			self.store[prop]=self.ambariFetch("yarn-site",prop)
		self.store["num_nodemanager"]=len(self.fetcher.getHostsRunningComponent("NODEMANAGER"))

	def ambariFetch(self,conf,prop):
		try:
			return self.fetcher.getConfig(conf)[prop]
		except Exception as e:
			print "Can not get value for {} in {} because {}. Please try manual add".format(prop,conf,e.__str__())

	def validConf(self,num_executors,executor_mem,max_executors_per_nm):
		if executor_mem<4 or executor_mem>31 or num_executors>max_executors_per_nm or num_executors<2:
			return False
		return True

	def getCoresForExecutor(self,num_executors,executor_mem,cores_per_nm):
		max_executor_cores=int(math.floor(cores_per_nm/num_executors))
		while not(float(executor_mem/max_executor_cores)>3.5) and not(max_executor_cores==1):
			max_executor_cores=max_executor_cores-1
		return max_executor_cores

	def addConfToObj(self,num_executors,executor_mem,max_executor_cores):
		tmp=dict(self.baseconf)
		tmp["name"]="{}_EXEC_{}_CORE_{}_MEM_PER_NODE_{}".format(str(num_executors),str(max_executor_cores),str(executor_mem),"".join([random.choice(ascii_lowercase) for i in range(5)]))
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.executor.instances"]=str(num_executors*self.store["num_nodemanager"])
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.executor.cores"]=str(max_executor_cores)
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.executor.memory"]=str(executor_mem)+"G"
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.driver.memory"]=str(executor_mem)+"G"
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.driver.cores"]=str(max_executor_cores)
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.dynamicAllocation.enabled"]="false"
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.executor.extraJavaOptions"]="-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
		tmp["config"]["ambari"]["spark2-thrift-sparkconf"]["spark.memory.fraction"]="0.6"
		self.confobj.addConfig(json.dumps(tmp))

	def saucemix(self,max_executors_per_nm=None,mem_after_overhead=None):
		count=0
		if not max_executors_per_nm:
			max_executors_per_nm=int(self.store["yarn.nodemanager.resource.cpu-vcores"])
		if not mem_after_overhead:
			mem_after_overhead=int(math.floor(int(self.store["yarn.nodemanager.resource.memory-mb"])/(1024*1.1)))
		while count<self.startwith:
			for num_executors,executor_mem in self.twoproduct(self.getFactors(mem_after_overhead)):
				if self.validConf(num_executors,executor_mem,max_executors_per_nm):
					self.addConfToObj(num_executors,executor_mem,self.getCoresForExecutor(num_executors,executor_mem,max_executors_per_nm))
					count+=1
				if not(num_executors==executor_mem):
					if self.validConf(executor_mem,num_executors,max_executors_per_nm):
						self.addConfToObj(executor_mem,num_executors,self.getCoresForExecutor(executor_mem,num_executors,max_executors_per_nm))
						count+=1
			mem_after_overhead=mem_after_overhead-1

	def getFactors(self,n):
		factors=[]
		for i in range(1,n+1):
			if n%i==0:
				factors.append(i)
		return factors

	def twoproduct(self,factors):
		return [[factors[i],factors[len(factors)-1-i]] for i in range((len(factors)/2)+len(factors)%2)]

