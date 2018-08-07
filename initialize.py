import sys
import re
import seedSpark
import seedLLAP
import seedTEZ
import InputParser
import random
import time
import json
from string import ascii_lowercase

class initialize:

	def __init__(self,logger):
		self.logger=logger
		self.clusterdetails=[]
		
	def begin(self):
		choice=raw_input('Do you want auto-suggested configuratios? Press (Y). Press (N) to override and continue.\n')
		if choice=='N':
			return 'params.json'
		elif choice=='Y':
			self.logger.info('Auto-suggesting configuratios. Awaiting further input..')
			try:
				engine=raw_input('Enter Engine (SPARK), (LLAP) or (TEZ)\n')
				if engine.upper() not in ['SPARK','LLAP','TEZ']:
					print 'Please enter one of (SPARK), (LLAP) or (TEZ)'
					self.begin()
				self.logger.info('Chosen engine is {}'.format(engine))
				self.parsetemplate()
				if engine.upper()=='SPARK':
					self.startSpark()
				elif engine.upper()=='TEZ':
					self.startTEZ()
				elif engine.upper()=='LLAP':
					self.startLLAP()
				self.seed.getRequiredProps()
				self.seed.saucemix()
				self.seed.confobj.interact()
				return self.addSettingsToTemplate(engine)
			except Exception as e:
				print 'Something went wrong {}. Please try again'.format(e.__str__())
				self.begin()
		else:
			print 'Press either (Y) or (N)'
			self.begin()

	def parsetemplate(self):
		self.templateloc=raw_input('Enter location of template json\n')
		self.logger.info('Parsing {}'.format(self.templateloc))
		try:
			iparse=InputParser.parseInput(self.templateloc)
			self.clusterdetails=iparse.clusterInfo()
			if not(len(self.clusterdetails)==4):
				print 'Please specify Ambari IP, Cluster Name, Ambari Username, Ambari Password in template file and retry.'
				self.parsetemplate()
		except Exception as e:
			print 'Exception while parsing {}. {}. Please retry.'.format(self.templateloc,e.__str__())
			self.parsetemplate()

	def addSettingsToTemplate(self,engine):
		try:
			self.logger.info('Preparing run json with selected configurations')
			with open(self.templateloc) as template:
				addTo=json.load(template)
			for conf in self.seed.confobj.configs.keys():
				addTo['wrap']['settings'].append(self.seed.confobj.configs[conf])
			startjson='params_{}_{}.json'.format(engine.upper(),''.join([random.choice(ascii_lowercase) for i in range(5)]))
			with open(startjson,'w+') as f:
				f.write(json.dumps(addTo,indent=4,sort_keys=True))
				f.close()
			print 'Json input written to {}'.format(startjson)
			return startjson
		except Exception as e:
			print 'Exception while trying to add suggested configurations to template. {}.Please try again'.format(e.__str__())
			self.begin()

	def startSpark(self):
		self.seed=seedSpark.sparkseed(self.clusterdetails,input('Enter approx number of seed configuratios\n'))
		
	def startTEZ(self):
		self.seed=seedTEZ.tezseed(self.clusterdetails,input('Enter approx number of seed configuratios\n'))

	def startLLAP(self):
		self.seed=seedLLAP.llapseed(self.clusterdetails,[float(item) for item in re.split(',',raw_input('Script will generate conifgurations at cache (10%,30%,50%) of daemon,Enter additional comma separated cache percentages if required\n'))])

