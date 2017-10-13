import sys
import subprocess
import re
from collections import defaultdict

class hiveUtil:

	queryDir='/home/hdfs/hive-testbench/sample-queries-tpcds'
	initDir='/home/hdfs/llap/TandE/llap_settings'

	def __init__(self):
		self.hivetrials=defaultdict(lambda:{})
		self.viaAmbari=defaultdict(lambda:defaultdict(lambda:{}))
		self.restarts=defaultdict(lambda:defaultdict(lambda:[]))

	def addSettings(self,name,setting):
		self.hivetrials[name]=setting

	def addAmbariConf(self,name,setting):
		for key in setting.keys():
			self.viaAmbari[name][key]=setting[key]

	def addRestart(self,name,setting):
		for key in setting.keys():
			self.restarts[name][key]=setting[key]

	def getInitFile(self,setting):
		return '/'.join([hiveUtil.initDir,setting])

	def getQueryFile(self,hiveql):
		return '/'.join([hiveUtil.queryDir,hiveql])

	def getHiveConfString(self,setting):
		if setting in self.hivetrials.keys():
			confdir=self.hivetrials[setting]
			return ' '.join([' '.join(['--hiveconf','='.join([key,confdir[key]])]) for key in confdir.keys()])
		return ''

	def setJDBCUrl(self,dbname):
		self.hs2url="'jdbc:hive2://c01s03.hadoop.local:2181,c01s02.hadoop.local:2181,c01s01.hadoop.local:2181/"+dbname+";serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2-hive2'"

	def BeelineCommand(self,setting,hiveql,initfile=True):
		if initfile:
			return ' '.join(['beeline','-u',self.hs2url,'-i',self.getInitFile(setting),'-f',self.getQueryFile(hiveql)])
		else:
			return ' '.join(['beeline','-u',self.hs2url,'-f',self.getQueryFile(hiveql),self.getHiveConfString(setting)])
