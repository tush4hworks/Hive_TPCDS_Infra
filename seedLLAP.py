import sys
import modifyConfig
from collections import defaultdict
import random
import math
import confectionary
from string import ascii_lowercase
import json

class llapseed:
	
	def __init__(self,clusterdetails,addCaches=[]):
		self.confobj=confectionary.confectionary()
		self.store={}
		host,cluster,user,password=clusterdetails
		self.fetcher=modifyConfig.ambariConfig(host,cluster,user,password)
		self.tradeoffs=[float(1)/float(9),float(3)/float(7),float(1)/float(1)]
		for addCache in addCaches:
			if addCache>=100 or addCache<=0:
				continue
			self.tradeoffs.append(float(addCache/(100-addCache)))
		self.baseconf= {
	        "config": {
	          "ambari": {
	             "tez-interactive-site": {
	              "tez.runtime.io.sort.mb":None
	            },
	            "hive-interactive-site": {
	              "hive.llap.daemon.yarn.container.mb":None,
	              "hive.llap.io.memory.size":None,
	              "hive.llap.io.threadpool.size": None,
	              "hive.tez.container.size":None,
	              "hive.llap.daemon.num.executors":None,
	              "hive.auto.convert.join.noconditionaltask.size":None
	            },
	            "hive-interactive-env": {
	              "llap_heap_size":None
	            }
	          },
	          "system":[],
	          "hiveconf": {
	            "hive.auto.convert.join": "true",
	            "hive.auto.convert.join.noconditionaltask": "true",
	            "hive.cbo.enable": "true",
	            "hive.llap.client.consistent.splits":"true",
	            "hive.stats.autogather": "true",
	            "hive.llap.auto.allow.uber": "true",
	            "hive.vectorized.execution.enabled": "true",
	            "hive.vectorized.execution.reduce.enabled": "true",
	            "hive.vectorized.execution.reduce.groupby.enabled": "true",
	            "hive.optimize.index.filter": "true"
	          },
	          "restart": {
	            "components": [
	              "HIVE/components/HIVE_SERVER_INTERACTIVE"
	            ],
	            "services": []
	          }
	        },
	        "name":None
	      }
	     
	def getRequiredProps(self):
		for prop in ["yarn.nodemanager.resource.memory-mb","yarn.nodemanager.resource.cpu-vcores"]:
			self.store[prop]=int(self.ambariFetch("yarn-site",prop))
		self.store["num_nodemanager"]=len(self.fetcher.getHostsRunningComponent("NODEMANAGER"))
		self.store["num_llap_nodes"]=int(self.ambariFetch("hive-interactive-env","num_llap_nodes"))
		self.store["hive.llap.daemon.yarn.container.mb"]=int(self.ambariFetch("hive-interactive-site","hive.llap.daemon.yarn.container.mb"))

	def ambariFetch(self,conf,prop):
		try:
			return self.fetcher.getConfig(conf)[prop]
		except Exception as e:
			print "Can not get value for {} in {} because {}. Please try manual add".format(prop,conf,e.__str__())

	def getCoresForContainer(self,max_cores,container_size,llap_heap_size):
		return min(int(math.floor(float(llap_heap_size)/float(container_size))),max_cores)

	def addConfToObj(self,llap_heap_size,llap_cache_size,llap_threadpool_size,num_executors,container_info):
		tmp=dict(self.baseconf)
		tmp["config"]["ambari"]["tez-interactive-site"]["tez.runtime.io.sort.mb"]=container_info["tez.runtime.io.sort.mb"]
		tmp["config"]["ambari"]["hive-interactive-env"]["llap_heap_size"]=str(llap_heap_size)
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.llap.daemon.yarn.container.mb"]=str(self.store["hive.llap.daemon.yarn.container.mb"])
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.llap.io.memory.size"]=str(llap_cache_size)
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.tez.container.size"]=container_info["hive.tez.container.size"]
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.llap.io.threadpool.size"]=str(llap_threadpool_size)
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.llap.daemon.num.executors"]=str(num_executors)
		tmp["config"]["ambari"]["hive-interactive-site"]["hive.auto.convert.join.noconditionaltask.size"]=container_info["hive.auto.convert.join.noconditionaltask.size"]
		tmp["name"]="{}_HEAP_{}_CACHE_{}_EXEC_{}".format(str(int(llap_heap_size/1024))+'GB',str(int(llap_cache_size/1024))+'GB',str(num_executors),''.join([random.choice(ascii_lowercase) for i in range(5)]))
		self.confobj.addConfig(json.dumps(tmp))

	def saucemix(self,daemon_memory=None):
		if not daemon_memory:
			daemon_memory=int(self.store["hive.llap.daemon.yarn.container.mb"])
		count=0
		for tradeoff in self.tradeoffs:
			llap_heap_size=int(math.floor((daemon_memory*0.97)/(1.1+tradeoff)))
			llap_cache_size=int(llap_heap_size*tradeoff)
			llap_threadpool_size=min(self.store["yarn.nodemanager.resource.cpu-vcores"],20)
			container_sizes={
			2048:{
			"tez.runtime.io.sort.mb":"550",
			"hive.tez.container.size":"2048",
			"hive.auto.convert.join.noconditionaltask.size":"550000000"
			},
			3072:{
			"tez.runtime.io.sort.mb":"800",
			"hive.tez.container.size":"2048",
			"hive.auto.convert.join.noconditionaltask.size":"900000000"
			},
			4096:{
			"tez.runtime.io.sort.mb":"1092",
			"hive.tez.container.size":"4096",
			"hive.auto.convert.join.noconditionaltask.size":"1145044992"
			}
			}
			for container_size in container_sizes.keys():
				num_executors=self.getCoresForContainer(self.store["yarn.nodemanager.resource.cpu-vcores"],container_size,llap_heap_size)
				self.addConfToObj(llap_heap_size,llap_cache_size,llap_threadpool_size,num_executors,container_sizes[container_size])

	def getFactors(self,n):
		factors=[]
		for i in range(1,n+1):
			if n%i==0:
				factors.append(i)
		return factors

	def twoproduct(self,factors):
		return [[factors[i],factors[len(factors)-1-i]] for i in range((len(factors)/2)+len(factors)%2)]

