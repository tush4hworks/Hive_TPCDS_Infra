import sys
import subprocess
import re

class statsPerQuery:
	application_id_regex=r'application_'
	appattempt_regex=r'appattempt_'
	num_containers_regex=r'Total number of containers'

	def __init__(self):
		pass

	def get_num_containers(self):
		try:
			application_id=None
			application_attempt=None
			num_containers=0
			output=subprocess.check_output('yarn application -list all',stderr=subprocess.STDOUT,shell=True)
			for line in output.split('\n'):
				if re.search(statsPerQuery.application_id_regex,line,re.I):
					application_id=line.split()[0]
					break
			if not application_id:
				raise Exception
			output=subprocess.check_output('yarn applicationattempt -list '+application_id,stderr=subprocess.STDOUT,shell=True)
			for line in output.split('\n'):
				if re.search(statsPerQuery.appattempt_regex,line,re.I):
					application_attempt=line.split()[0]
					break
			if not application_attempt:
				raise Exception
			output=subprocess.check_output('yarn container -list '+application_attempt,stderr=subprocess.STDOUT,shell=True)
			for line in output.split('\n'):
				if re.search(statsPerQuery.num_containers_regex,line,re.I):
					return int(line.split(':')[1])
			return num_containers
		except Exception:
			return 0

