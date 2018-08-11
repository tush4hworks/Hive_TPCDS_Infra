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
import datetime
import time
import collect_metrics
import partialStats
import initialize


class controls:

	success_regex=r'([\d\.]+)\s+seconds'
	failed_regex=r'(FAILED|ERROR)'
	dag_regex=r'100%\s+ELAPSED TIME:\s+\d+.\d+\s+s'
	numrows_regex=r'selected'
	
	def __init__(self):
		"""Init Function for class controls"""
		FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
		logging.basicConfig(format=FORMAT,filename='hivetests.log',filemode='w',level='INFO')
		logging.getLogger("requests").setLevel(logging.WARNING)
		self.logger=logging.getLogger(__name__)
		self.results=defaultdict(lambda:defaultdict(lambda:defaultdict(lambda:[])))
		self.rowsOnQuery=defaultdict(lambda:'NA')
		self.start_end=defaultdict(lambda:['NA','NA'])
		self.epochdict=defaultdict(lambda:defaultdict(lambda:['NA','NA']))
		self.containers=defaultdict(lambda:defaultdict(lambda:0))
		self.pstat=partialStats.pstats(self.logger)
		self.initializer=initialize.initialize(self.logger)

	def getDateTime(self,epochT=False):
		if epochT:
			return str(int(time.time()))
		return datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

	def addResult(self,queryOut,dbname,setting,hiveql,startEnd):
		"""Parse beeline output"""
		self.start_end[''.join([dbname,hiveql,setting,str(len(self.results[dbname][hiveql][setting]))])]=startEnd
		for line in queryOut.split('\n')[-1:0:-1]:
			if re.search(controls.success_regex,line,re.I):
				self.results[dbname][hiveql][setting].append(float(re.search(controls.success_regex,line,re.I).group().split('seconds')[0].strip()))
				if re.search(controls.numrows_regex,line,re.I):
					self.rowsOnQuery[hiveql]=re.sub(',','',re.split('\s+',line)[0])
				return
		self.results[dbname][hiveql][setting].append('NA')


	def dumpResultsToCsv(self):
		"""Create CSV"""
		self.logger.info(self.results)
		with open(self.getDateTime()+'hiveResults.csv','w+') as f:
			f.write(','.join(['DB','QUERY','ROWS',','.join([','.join(item) for item in [[hiveconf]*self.numRuns*3 for hiveconf in self.hiveconfs]])])+'\n')
			for db in self.results.keys():
				for ql in self.results[db].keys():
					f.write(','.join([db,ql,self.rowsOnQuery[ql],','.join([','.join([','.join(self.start_end[''.join([db,ql,hiveconf,str(i)])]+[str(self.results[db][ql][hiveconf][i])]) for i in range(len(self.results[db][ql][hiveconf]))]) for hiveconf in self.hiveconfs])])+'\n')

	def toZeppelinAndTrigger(self):
		try:
			subprocess.check_output('hadoop fs -rm /tmp/'+self.zepInputFile,stderr=subprocess.STDOUT,shell=True)
		except Exception as e:
			self.logger.info(e.__str__())
		try:
			with open(self.zepInputFile,'w+') as f:
				for db in self.results.keys():
					for ql in self.results[db].keys():
						for setting in self.results[db][ql].keys():
							for i in range(len(self.results[db][ql][setting])):
								f.write(','.join([ql,setting,str(i+1),str(self.results[db][ql][setting][i])
									])+'\n')
		except Exception as e:
			self.logger.info(e.__str__())
		try:			
			subprocess.check_output('hadoop fs -put '+self.zepInputFile+' /tmp',stderr=subprocess.STDOUT,shell=True)
		except Exception as e:
			self.logger.info(e.__str__())
		try:
			self.zepObj.runParagraphs()
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

	def addResourceStats(self,queryDict):
		cstat=collect_metrics.getQueryMetrics(self.metricsHost,self.metricsPort,self.logger)
		for setting in queryDict.keys():
			self.logger.info('+ Collecting stats for setting '+setting)
			for query in queryDict[setting].keys():
				try:
					self.logger.info('+ Collecting stats for query '+query)
					for key in self.collection.keys():
						cstat.fetch_stats(query,key,self.collection[key]['metrics'],queryDict[setting][query][0],queryDict[setting][query][1],'_'.join([setting,self.collection[key]['dumpfile']]),self.collection[key]['hostname'],self.collection[key]['precision'],self.collection[key]['appId'])
					self.logger.info('- Collected stats for query '+query)
				except Exception as e:
					self.logger.info(e.__str__())
			self.logger.info('- Collected stats for setting '+setting)	

	def runCmd(self,cmd,dbname,setting,hiveql,run):
		"""Wrapper to run shell"""
		start=self.getDateTime()
		startEpoch=str(int(time.time()*1000))
		try:
			self.logger.info('+ Executing command '+cmd)
			result=subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
			end=self.getDateTime()
			endEpoch=str(int(time.time()*1000))
			with open('History/'+'_'.join([dbname,hiveql,setting,run,self.getDateTime()]),'w+') as f:
				f.write(result)
			self.logger.info('- Finished executing command '+cmd)
			self.addResult(result,dbname,setting,hiveql,[start,end])
			self.epochdict[setting][hiveql]=[startEpoch,endEpoch]
		except Exception as e:
			end=self.getDateTime()
			endEpoch=str(int(time.time()*1000))
			self.epochdict[setting][hiveql]=[startEpoch,endEpoch]
			self.logger.error('- Finished executing command with exception '+cmd)
			self.addResult('NA',dbname,setting,hiveql,[start,end])
			if hasattr(e,'output'):
				with open('History/'+'_'.join([dbname,hiveql,setting,run,self.getDateTime()]),'w+') as f:
					f.write(e.output)

	def sysConf(self,cmds,setting=''):
		for cmd in cmds:
			try:
				self.logger.info('+ Running '+cmd+' for setting '+setting)
				subprocess.check_output(cmd,stderr=subprocess.STDOUT,shell=True)
				self.logger.info('- Finished executing command '+cmd)
			except Exception as e:
				self.logger.error('- Finished executing command with exception '+cmd)
		
	def modifySettingsAndRestart(self,ambariSetting,services,components,force_restart=False):
		"""Calling ambari API to change configuration and restart services/components"""
		reset=False
		for key in ambariSetting.keys():
			if self.modconf.putConfig(key,ambariSetting[key]):
				reset=True
		if reset or force_restart:
			self.logger.warn('+ Config changed. Going to restart services/components if any! +')
			for service in services:
				if service=="ALL":
					self.logger.info('+ Restarting all stale components')
					self.modconf.restartAllRequired()
					self.logger.info('- Restarted all stale components')
					break
				self.logger.info('+ Restarting '+service+' +')
				self.modconf.restartService(service)
				self.logger.info('- Restarted '+service+' -')
			for component in components:
				self.logger.info('+ Restarting '+component+' +')
				self.modconf.restartComponent(component)
				self.logger.info('- Restarted '+component+' -')

	def updateNote(self):
		try:
			t=threading.Thread(target=self.toZeppelinAndTrigger,args=())
			t.start()
		except Exception as e:
			self.logger.info(e.__str__())

	def statCollection(self,queryDict):
		try:
			threading.Thread(target=self.addResourceStats,args=[self.epochdict]).start()
		except Exception as e:
			self.logger.info(e.__str__())

	def adjustConf(self,setting):
		self.logger.info('+ Adjusting configurations')
		force_restart=False
		if setting in self.hive.sysMod.keys():
			self.sysConf(self.hive.sysMod[setting],setting)
		if setting in self.hive.viaAmbari.keys():
			if self.rollBack:
				self.logger.warn('+ Rolling back to base version +')
				for rollserv in self.rollBack_service:
					self.modconf.rollBackConfig(rollserv,self.rollBack_service[rollserv]) 
				self.logger.info('- Rolled back to base version before making changes -')
				force_restart=True
			self.logger.info('+ Comparing with existing configurations via ambari for '+setting+' +')
			self.modifySettingsAndRestart(self.hive.viaAmbari[setting],self.hive.restarts[setting]['services'],self.hive.restarts[setting]['components'],force_restart)
		self.logger.info('+ Starting execution with below configurations for '+setting)
		for toPrint in self.printer:
			self.logger.info(json.dumps(self.modconf.getConfig(toPrint),indent=4,sort_keys=True))
		self.logger.info('- Adjusted configurations -')


	def runTests(self):
		"""Main entry function to run TPCDS suite"""
		#for setting,hiveql in list(itertools.product(settings,hiveqls)):
		for setting in self.hiveconfs:
			try:
				self.adjustConf(setting)
			except Exception as e:
				self.logger.error(e.__str__())
				self.logger.error('- Skipping '+setting)
				continue
			self.tasks=list(self.queries)
			runners=[]
			for num_thread in range(self.num_threads):
				runners.append(threading.Thread(target=self.pRunner,args=(setting,str(num_thread),int(time.time()))))
			for runner in runners:
				runner.start()
			for runner in runners:
				runner.join()

	def runBeeline(self,setting,hiveql):
		try:
			self.logger.info('+ Begin executiom '+' '.join([hiveql,C.db,setting])+' +')
			beelineCmd=self.hive.BeelineCommand(setting,hiveql,True if setting in self.hive.initFile.keys() else False)
			for i in xrange(self.numRuns):
				self.runCmd(beelineCmd,self.db,setting,hiveql,str(i))
			self.logger.info('- Finished execution '+' '.join([hiveql,self.db,setting])+' -')
			if self.runZep:
				self.updateNote()
		except Exception as e:
			self.logger.error(e.__str__())
			self.logger.warn('- Finished execution with exception'+' '.join([hiveql,self.db,setting])+' -')

	def pRunner(self,setting,runner_id,start_time):
		self.logger.info('+ Started runner '+runner_id)
		currCount=0
		while True:
			if self.timeout and int(time.time())-start_time>self.timeout:
				self.logger.warn('- Runner '+runner_id+' exceeded timeout, hence quitting! Ran a total of '+str(currCount)+' queries')
				return
			try:
				hiveql=self.tasks.pop()
				self.logger.info('+ Runner '+runner_id+' is running '+hiveql)
				self.runBeeline(setting,hiveql)
				currCount=currCount+1
			except IndexError:
				self.logger.warn('- Runner '+runner_id+' exiting! No queries left to run. Ran a total of '+str(currCount)+' queries')
				return
			except Exception:
				self.logger.error('- Runner '+runner_id+' exited with exception '+e.__str__())
				self.logger.info('- Runner '+runner_id+' ran a total of '+str(currCount)+' queries')
				return

	def addHiveSettings(self,name,runSettings):
		"""Segregate settings and add"""
		if 'hiveconf' in runSettings.keys():
			self.hive.addSettings(name,runSettings['hiveconf'])
		if 'ambari' in runSettings.keys():
			self.hive.addAmbariConf(name,runSettings['ambari'])
		if 'restart' in runSettings.keys():
			self.hive.addRestart(name,runSettings['restart'])
		if 'initfile' in runSettings.keys():
			self.hive.addInitFile(name,runSettings['initfile'])
		if 'system' in runSettings.keys():
			self.hive.addSysMod(name,runSettings['system'])
		self.hiveconfs.append(name)

	def fetchParams(self,fileloc):
		"""Parse input json"""
		iparse=InputParser.parseInput(fileloc)
		host,clustername,user,password=iparse.clusterInfo()
		self.modconf=modifyConfig.ambariConfig(host,clustername,user,password)
		self.queryDir,initDir=iparse.hiveDirs()
		self.hiveconfs=[]
		self.hive=hiveUtil.hiveUtil(self.queryDir,initDir)
		self.numRuns=iparse.numRuns()
		self.conn_str=iparse.conn_str()
		self.num_threads,self.timeout=iparse.runConf()
		self.db=iparse.db()
		self.hive.setJDBCUrl(self.conn_str,self.db)
		self.queries=iparse.queries()
		self.printer=iparse.printer()
		self.rollBack=iparse.rollBack()
		self.collection=iparse.collectors()
		self.metricsHost,self.metricsPort=iparse.ametrics()
		if self.rollBack:
			self.base_version=iparse.base_version()
			self.rollBack_service=iparse.rollBack_service()
		for setting in iparse.specified_settings():
			self.addHiveSettings(setting['name'],setting['config'])
		self.runZep=False
		if iparse.whetherZeppelin():
			try:
				self.runZep=True
				host,user,password=iparse.noteInfo()
				self.zepInputFile='zepin.csv'
				self.zepObj=notes.zepInt(host,user,password)
				self.zepObj.createNote(self.db,[setting['name'] for setting in iparse.specified_settings()])
			except Exception as e:
				print 'Problems in creating zeppelin notebook. '+e.__str__()

if __name__=='__main__':
	C=controls()
	C.fetchParams(C.initializer.begin())
	C.pstat.performCheck(C.hive.hs2url,C.queryDir,C.queries)
	C.runTests()
	C.dumpResultsToCsv()
	C.statCollection()
	C.sysConf(['zip stats_'+C.getDateTime()+'.zip *stats.csv'])
	C.runAnalysis()

