import sys
import commands
import re

class pstats:

	def __init__(self,logger):
		self.logger=logger

	def runcmd(self,cmd):
		try:
			s,o=commands.getstatusoutput(cmd)
			return o
		except Exception as e:
			print "Exception while executing "+cmd
			sys.exit(1)

	def validateStats(self,out):
		stats=True
		line=out.split('\n')
		for i in range(len(line)):
			if re.search(r'TableScan',line[i],re.I):
				while True and i<len(out.split('\n'))-1:
					i=i+1
					if re.search(r'Statistics',line[i],re.I):
						if 'PARTIAL' in line[i].upper() or 'NONE' in line[i].upper():
							return False
						break
		return True

	def performCheck(self,conn_str,queryDir,queries):
		for query in queries:
			self.logger.info('+Checking PARTIAL/NONE Table Statistics in explain plan for query '+query)
			sql=open('/'.join([queryDir,query]),'r+').read()
			if not self.validateStats(self.runcmd(' '.join(['beeline','-u',conn_str,'-e','"explain',sql+'"']))):
				self.logger.info('WARNING! PARTIAL STATS ENCOUNTERED WHILE RUNNING EXPLAIN PLAN FOR QUERY '+query)
				sys.exit(1)
			self.logger.info('-All Table Statistics complete in explain plan for query '+query)
