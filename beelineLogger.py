import sys
import logging

class bLog:
	def __init__(self):
		pass

	def getLogger(self):
		return self.logger

	def setLogger(self,dbname,setting,hiveql,run):
		FORMAT = '%(asctime)-s-%(levelname)s-%(message)s'
		logging.basicConfig(format=FORMAT,filename='History/'+'_'.join([dbname,setting,hiveql,run])+'_beeline.log',level='INFO')
		self.logger=logging.getLogger(__name__)
