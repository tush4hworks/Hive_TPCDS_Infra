import sys
import subprocess
import re
from collections import defaultdict

class hiveUtil:

	def __init__(self,queryDir,initDir=None):
		self.hivetrials=defaultdict(lambda:{})
		self.viaAmbari=defaultdict(lambda:defaultdict(lambda:{}))
		self.restarts=defaultdict(lambda:defaultdict(lambda:[]))
		self.queryDir=queryDir
		self.initDir=initDir

	def addSettings(self,name,setting):
		self.hivetrials[name]=setting

	def addAmbariConf(self,name,setting):
		for key in setting.keys():
			self.viaAmbari[name][key]=setting[key]

	def addRestart(self,name,setting):
		for key in setting.keys():
			self.restarts[name][key]=setting[key]

	def getInitFile(self,setting):
		return '/'.join([self.initDir,setting])

	def getQueryFile(self,hiveql):
		return '/'.join([self.queryDir,hiveql])

	def getHiveConfString(self,setting):
		if setting in self.hivetrials.keys():
			confdir=self.hivetrials[setting]
			return ' '.join([' '.join(['--hiveconf','='.join([key,confdir[key]])]) for key in confdir.keys()])
		return ''

	def setJDBCUrl(self,conn_str,dbname):
		self.hs2url="'"+';'.join([conn_str.split(';')[0]+dbname,';'.join(conn_str.split(';')[1:])])+"'"

	def BeelineCommand(self,setting,hiveql,initfile=True):
		if initfile:
			return ' '.join(['beeline','-u',self.hs2url,'-i',self.getInitFile(setting),'-f',self.getQueryFile(hiveql)])
		else:
			return ' '.join(['beeline','-u',self.hs2url,'-f',self.getQueryFile(hiveql),self.getHiveConfString(setting)])
