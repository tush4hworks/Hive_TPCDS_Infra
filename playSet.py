import sys
import subprocess
import re
from collections import defaultdict
import itertools
import json
import threading
import hiveUtil
import logging
import modifyConfig
import analyzeResults
import InputParser
import notes


class controls:

	success_regex=r'([\d\.]+)\s+seconds'
	failed_regex=r'(FAILED|ERROR)'
	dag_regex=r'100%\s+ELAPSED TIME:\s+\d+.\d+\s+s'

	def __init__(self,jsonFile):
		"""Init Function for class controls"""
		FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
		logging.basicConfig(format=FORMAT,filename='hivetests.log',filemode='w',level='INFO')
		logging.getLogger("requests").setLevel(logging.WARNING)
		self.logger=logging.getLogger(__name__)
		self.results=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:[])))
		self.fetchParams(jsonFile)

	def addResult(self,queryOut,dbname,setting,hiveql):
		"""Parse beeline output"""
		for line in queryOut.split('\n')[-1:0:-1]:
			if re.search(controls.success_regex,line,re.I):
				self.results[dbname][hiveql][setting].append(float(re.search(controls.success_regex,line,re.I).group().split('seconds')[0].strip()))
				return
		self.results[dbname][hiveql][setting].append('NA')

	def dumpResultsToCsv(self):
		"""Create CSV"""
		self.logger.info(self.results)
		with open('hiveResults.csv','w+') as f:
			f.write(','.join(['DB','QUERY',','.join([','.join(item) for item in [[hiveconf]*self.numRuns for hiveconf in self.hiveconfs]])])+'\n')
			for db in self.results.keys():
				for ql in self.results[db].keys():
					f.write(','.join([db,ql,','.join([','.join([str(exec_time) for exec_time in self.results[db][ql][hiveconf]]) for hiveconf in self.hiveconfs])])+'\n')

	def toZeppelinAndTrigger(self):
		try:
			subprocess.check_output('hadoop fs -rm '+self.zepInputFile,stderr=subprocess.STDOUT,shell=True)
			with open(self.zepInputFile,'w+') as f:
				try:
					for db in self.results.keys():
						for ql in self.results[db].keys():
							for setting in self.results[db][ql].keys():
								for i in range(len(self.results[db][ql][setting])):
									f.write(','.join([ql,setting,str(i+1),self.results[db][ql][setting][i]])+'\n')
			subprocess.check_output('hadoop fs -put'+self.zepInputFile+' /tmp',stderr=subprocess.STDOUT,shell=True)
			self.zepObj.runParagraphs(self.zeppelinNote)
		except Exception as e:
			self.logger.info(e.__str__())

	def runAnalysis(self):
		"""Run basic analysis, sort by total time taken, gather best configuration for every query"""
		try:
			self.analysis=analyzeResults.analyze(self.results)
			self.analysis.rank_average_execution_time()
			self.analysis.rank_optimized_queries()
			self.analysis.closeFile()
		except Exception as e:
			self.logger.error(e)

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
			self.logger.error('- Finished executing command with exception '+cmd)
			self.addResult('NA',dbname,setting,hiveql)
			if hasattr(e,'output'):
				with open('History/'+'_'.join([dbname,hiveql,setting,run]),'w+') as f:
					f.write(e.output)
		
	def modifySettingsAndRestart(self,ambariSetting,services,components,force_restart=False):
		"""Calling ambari API to change configuration and restart services/components"""
		reset=False
		for key in ambariSetting.keys():
			if self.modconf.putConfig(key,ambariSetting[key]):
				reset=True
		if reset or force_restart:
			self.logger.warn('+ Config changed. Going to restart services/components if any! +')
			for service in services:
				self.logger.info('+ Restarting '+service+' +')
				self.modconf.restartService(service)
				self.logger.info('- Restarted '+service+' -')
			for component in components:
				self.logger.info('+ Restarting '+component+' +')
				self.modconf.restartComponent(component)
				self.logger.info('- Restarted '+component+' -')

	def updateNote(self):
		try:
			t=threading.Thread(target=toZeppelinAndTrigger,args=())
			t.start()
		except Exception as e:
			self.logger.info(e.__str__())

	def runTests(self,dbname,settings,hiveqls,numRuns,initfile=True):
		"""Main entry function to run TPCDS suite"""
		self.hive.setJDBCUrl(self.conn_str,dbname)
		currSet=None
		for setting,hiveql in list(itertools.product(settings,hiveqls)):
			try:
				self.logger.info('+ BEGIN EXECUTION '+' '.join([hiveql,dbname,setting])+' +')
				if not(currSet) or not(setting==currSet):
					force_restart=False
					updateZeppelin=True
					if setting in self.hive.viaAmbari.keys():
						if currSet and self.rollBack:
							self.logger.warn('+ Rolling back to base version before making changes for setting '+currSet+ '+')
							self.modconf.rollBackConfig(self.rollBack_service,self.base_version) 
							self.logger.info('- Rolled back to base version before making changes for setting '+currSet+ '-')
							force_restart=True
						self.logger.info('+ Comparing with existing configurations via ambari for '+setting+' +')
						self.modifySettingsAndRestart(self.hive.viaAmbari[setting],self.hive.restarts[setting]['services'],self.hive.restarts[setting]['components'],force_restart)
					self.logger.info('Starting execution with below configurations for '+setting)
					for toPrint in self.printer:
						self.logger.info(json.dumps(self.modconf.getConfig(toPrint),indent=4,sort_keys=True))
					currSet=setting
				beelineCmd=self.hive.BeelineCommand(setting,hiveql,initfile)
				for i in xrange(numRuns):
					self.runCmd(beelineCmd,dbname,setting,hiveql,str(i))
				self.logger.info('- FINISHED EXECUTION '+' '.join([hiveql,dbname,setting])+' -')
				if updateZeppelin:
					self.updateNote()
			except Exception as e:
				self.logger.error(e.__str__())
				self.logger.warn('- FINISHED EXECUTION WITH EXCEPTION'+' '.join([hiveql,dbname,setting])+' -')


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
		host,clustername,user,password=iparse.clusterInfo()
		self.modconf=modifyConfig.ambariConfig(host,clustername,user,password)
		queryDir,initDir=iparse.hiveDirs()
		self.hiveconfs=[]
		self.hive=hiveUtil.hiveUtil(queryDir,initDir)
		self.numRuns=iparse.numRuns()
		self.conn_str=iparse.conn_str()
		self.db=iparse.db()
		self.queries=iparse.queries()
		self.printer=iparse.printer()
		self.rollBack=iparse.rollBack()
		if self.rollBack:
			self.base_version=iparse.base_version()
			self.rollBack_service=iparse.rollBack_service()
		for setting in iparse.specified_settings():
			self.addHiveSettings(setting['name'],setting['config'])	
		if iparse.whetherZeppelin():
			host,user,password,note,zepInputFile=iparse.noteInfo()
			self.zeppelinNote=note
			self.zepInputFile=zepInputFile
			self.zepObj=notes(host,user,password)

if __name__=='__main__':
	C=controls('params.json')
	C.runTests(C.db,C.hiveconfs,C.queries,C.numRuns,False)
	C.dumpResultsToCsv()
	C.runAnalysis()
