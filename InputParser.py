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

	def queries(self):
		return self.params['wrap']['queries']




