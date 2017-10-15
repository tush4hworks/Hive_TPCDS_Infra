import sys
import subprocess
import re
from collections import defaultdict
import itertools
import hiveUtil
import logging
import modifyConfig
import analyzeResults
import InputParser
import json

class controls:

	success_regex=r'([\d\.]+)\s+seconds'
	failed_regex=r'(FAILED|ERROR)'
	dag_regex=r'100%\s+ELAPSED TIME:\s+\d+.\d+\s+s'

	def __init__(self,host,cluster):
		"""Init Function for class controls"""
		FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
		logging.basicConfig(format=FORMAT,filename='hivetests.log',filemode='w',level='INFO')
		logging.getLogger("requests").setLevel(logging.WARNING)
		self.logger=logging.getLogger(__name__)
		self.results=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:[])))
		self.hive=hiveUtil.hiveUtil()
		self.hiveconfs=[]
		self.modconf=modifyConfig.ambariConfig(host,cluster)

	def addResult(self,queryOut,dbname,setting,hiveql):
		"""Parse beeline output"""
		for line in queryOut.split('\n')[-1:0:-1]:
			if re.search(controls.success_regex,line,re.I):
				self.results[dbname][hiveql][setting].append(float(re.search(controls.success_regex,line,re.I).group().split('seconds')[0].strip()))
				return
		self.results[dbname][hiveql][setting].append('NA')

	def dumpResultsToCsv(self,numRuns):
		"""Create CSV"""
		self.logger.info(self.results)
		with open('hiveResults.csv','w+') as f:
			f.write(','.join(['DB','QUERY',','.join([','.join(item) for item in [[hiveconf]*numRuns for hiveconf in self.hiveconfs]])])+'\n')
			for db in self.results.keys():
				for ql in self.results[db].keys():
					f.write(','.join([db,ql,','.join([','.join([str(exec_time) for exec_time in self.results[db][ql][hiveconf]]) for hiveconf in self.hiveconfs])])+'\n')

	def runAnalysis(self):
		"""Run basic analysis, sort by total time taken, gather best configuration for every query"""
		try:
			self.analysis=analyzeResults.analyze(self.results)
			self.analysis.rank_average_execution_time()
			self.analysis.rank_optimized_queries()
			self.analysis.closeFile()
		except Exception as e:
			self.logger.info(e)

	def runCmd(self,cmd,dbname,setting,hiveql,run):
		"""Wrapper to run shell"""
		try:
			self.logger.info('+ Executing command '+cmd)
			result=subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
			with open('History/'+'_'.join([dbname,hiveql,setting,run]),'w+') as f:
				f.write(result)
			self.logger.info('- Finished executing command '+cmd)
			self.addResult(result,dbname,setting,hiveql)
		except Exception as e:
			if hasattr(e,'returncode'):
				self.logger.info(e.returncode)
			if hasattr(e,'output'):
				with open('History/'+'_'.join([dbname,hiveql,setting,run]),'w+') as f:
					f.write(result)
					return
		
	def modifySettingsAndRestart(self,ambariSetting,services,components):
		"""Calling ambari API to change configuration and restart services/components"""
		reset=False
		for key in ambariSetting.keys():
			if self.modconf.putConfig(key,ambariSetting[key]):
				reset=True
		if reset:
			self.logger.info('+ Config changed. Going to restart services/components if any! +')
			for service in services:
				self.logger.info('+ Restarting '+service+' +')
				self.modconf.restartService(service)
				self.logger.info('- Restarted '+service+' -')
			for component in components:
				self.logger.info('+ Restarting '+component+' +')
				self.modconf.restartComponent(component)
				self.logger.info('- Restarted '+component+' -')

	def runTests(self,dbname,settings,hiveqls,numRuns,initfile=True):
		"""Main entry function to run TPCDS suite"""
		self.hive.setJDBCUrl(dbname)
		for setting,hiveql in list(itertools.product(settings,hiveqls)):
			currSet=None
			try:
				self.logger.info('+ BEGIN EXECUTION '+' '.join([hiveql,dbname,setting])+' +')
				if setting in self.hive.viaAmbari.keys():
					self.logger.info('+ Comparing with existing configurations via ambari for '+setting+' +')
					self.modifySettingsAndRestart(self.hive.viaAmbari[setting],self.hive.restarts[setting]['services'],self.hive.restarts[setting]['components'])
				self.logger.info('Starting execution with below configurations for '+setting)
				if not(currSet) or not(setting==currSet):
					self.logger.info(json.dumps(self.modconf.getConfig('hive-interactive-site'),indent=4,sort_keys=True))
					self.logger.info(json.dumps(self.modconf.getConfig('tez-interactive-site'),indent=4,sort_keys=True))
					self.logger.info(json.dumps(self.modconf.getConfig('hive-interactive-env'),indent=4,sort_keys=True))
					currSet=setting
				beelineCmd=self.hive.BeelineCommand(setting,hiveql,initfile)
				for i in xrange(numRuns):
					self.runCmd(beelineCmd,dbname,setting,hiveql,str(i))
				self.logger.info('- FINISHED EXECUTION '+' '.join([hiveql,dbname,setting])+' -')
			except Exception as e:
				self.logger.info(e.__str__())
				self.logger.info('- FINISHED EXECUTION WITH EXCEPTION'+' '.join([hiveql,dbname,setting])+' -')


	def addHiveSettings(self,name,runSettings):
		"""Segregate settings and add"""
		if 'hiveconf' in runSettings.keys():
			self.hive.addSettings(name,runSettings['hiveconf'])
		if 'ambari' in runSettings.keys():
			self.hive.addAmbariConf(name,runSettings['ambari'])
		if 'restart' in runSettings.keys():
			self.hive.addRestart(name,runSettings['restart'])
		self.hiveconfs.append(name)

	def fetchParams(self,fileloc):
		"""Parse input json"""
		iparse=InputParser.parseInput(fileloc)
		for setting in iparse.specified_settings():
			self.addHiveSettings(setting['name'],setting['config'])
		self.numRuns=iparse.numRuns()
		self.db=iparse.db()
		self.queries=iparse.queries()

if __name__=='__main__':
	C=controls('localhost','DPH')
	C.fetchParams('params.json')
	C.runTests(C.db,C.hiveconfs,C.queries,C.numRuns,False)
	C.dumpResultsToCsv(C.numRuns)
	C.runAnalysis()
