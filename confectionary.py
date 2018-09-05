import sys
from collections import defaultdict
import json
import os
import re

class confectionary:

	def __init__(self):
		self.configs=defaultdict(lambda:defaultdict(lambda:{}))

	def addConfig(self,userinput):
		try:
			if os.path.isfile(userinput):
				for config in json.loads(open(userinput,'r+').read())['settings']:
					print config
					self.atomicAdd(config)
			else:
				self.atomicAdd(json.loads(userinput))
		except Exception as e:
			print 'Exception while adding configurations {}. Please check input'.format(e.__str__())

	def atomicAdd(self,conf):
		if conf['name'] in self.configs.keys():
			print 'Duplicate config found for name {}. Please give a new name'.format(name)
			return
		for key in conf.keys():
			self.configs[conf['name']].update(conf)

	def deleteConfig(self,names):
		for name in re.split(',',names):
			self.configs.pop(name,None)
			print 'Deleted'

	def deleteAll(self):
		self.configs=defaultdict(lambda:defaultdict(lambda:{}))

	def atomicChangePut(self,names,change):
		putJson=json.loads(change)
		for name in names:
			for category in putJson.keys():   
				if category in self.configs[name]['config'].keys():
					if isinstance(putJson[category],dict):
						for param in putJson[category].keys():
							if isinstance(putJson[category][param],dict):
								if param in self.configs[name]['config'][category].keys():
									for subparam in putJson[category][param].keys():
										self.configs[name]['config'][category][param][subparam]=putJson[category][param][subparam]
								else:
									self.configs[name]['config'][category][param]=putJson[category][param]
							else:
								self.configs[name]['config'][category][param]=putJson[category][param]
					else:
						self.configs[name]['config'][category]=putJson[category]
				else:
					self.configs[name]['config'][category]=putJson[category]

	def atomicChangeDelete(self,names,change):
		putJson=json.loads(change)
		for name in names:
			for category in putJson.keys():   
				if category in self.configs[name]['config'].keys():
					if isinstance(putJson[category],dict):
						for param in putJson[category].keys():
							if isinstance(putJson[category][param],dict):
								if param in self.configs[name]['config'][category].keys():
									for subparam in putJson[category][param].keys():
										self.configs[name]['config'][category][param].pop(subparam,None)
							else:
								self.configs[name]['config'][category].pop(param,None)
					else:
						self.configs[name]['config'][category]=putJson[category]
				
	def changeConfig(self,s_option):
		try:
			option=''
			while not(option=='d' or option=='m'):
				option=raw_input('Press (d) to delete parameters (m) to add/modify\n')
			funcDict={'m':self.atomicChangePut,'d':self.atomicChangeDelete}
			if s_option=='A':
				funcDict[option](self.configs.keys(),raw_input('Enter changes in json format\n'))
			elif s_option=='S':
				funcDict[option](re.split(',',raw_input('Enter comma separated list of configurations to change\n')),raw_input('Enter changes in json format\n'))
		except Exception as e:
			print 'Exception while changing configurations {}. Please check input\n'.format(e.__str__())

	def currentConfigs(self):
		print 'Current test configurations'
		print json.dumps(self.configs,indent=4,sort_keys=True)

	def interact(self):
		self.currentConfigs()
		try:
			while True:
				option=raw_input('Type (P) to print, Type (A) to add, Type (D) to delete, Type (M) to modify, Type (F) to finalize and start run\n')
				if option=='P':
					self.currentConfigs()
					print 'Done.'
				elif option=='A':
					self.addConfig(raw_input('Enter configuration json or path of file having configuration\n'))
					print 'Done.'
				elif option=='D':
					self.deleteConfig(raw_input('Enter comma separated config names to be deleted\n'))
					print 'Done.'
				elif option=='M':
					self.changeConfig(raw_input('Press (A) for changing in all configurations or (S) in selected\n'))
					print 'Done.'
				elif option=='F':
					if raw_input ('Are you sure to lock configurations? Press (Y) or (N)\n')=='Y':
						break
					else:
						self.interact()
				else:
					print 'Input not recognized, please try again. Type (A) to add, Type (D) to delete, Type (M) to modify, Type (F) to finalize and start run\n'
		except Exception as e:
			print 'Something went wrong {}. Please try again'.format(e.__str__())
			self.interact()
		print 'Starting Tests with selected configurations'

if __name__=='__main__':
	factory=confectionary()
	factory.interact()
