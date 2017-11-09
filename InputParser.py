import sys
import json

class parseInput:
	def __init__(self,fileloc):
		with open(fileloc) as runSet:
			self.params=json.load(runSet)

	def numRuns(self):
		if 'numRuns' in self.params['wrap'].keys():
			return self.params['wrap']['numRuns']
		return 1

	def specified_settings(self):
		return self.params['wrap']['settings']

	def db(self):
		return self.params['wrap']['database']

	def printer(self):
		return self.params['wrap']['printer']

	def queries(self):
		return self.params['wrap']['queries']

	def rollBack(self):
		return (self.params['wrap']['enableRollBack'].lower()=='true')

	def base_version(self):
		if 'base_version' in self.params['wrap'].keys():
			return self.params['wrap']['base_version']

	def conn_str(self):
		return self.params['wrap']['connection_url']

	def rollBack_service(self):
		return self.params['wrap']['rollBackService']




