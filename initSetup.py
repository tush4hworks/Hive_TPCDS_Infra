import sys
import re
import subprocess
import os

class doSetup:

	def __init__(self):
		if not os.path.isdir('hive-testbench'):
			try:
				subprocess.check_call('git clone https://github.com/hortonworks/hive-testbench.git',shell=True)
				subprocess.check_call('cd hive-testbench;chmod +x tpcds-build.sh;./tpcds-build.sh',shell=True)
			except Exception as e:
				print 'Failed because {}'.format(e.__str__())
				sys.exit(1)

	def build_tables(self,factor,storage='orc'):
		try:
			subprocess.check_call('cd hive-testbench;FORMAT={} ./tpcds-setup.sh {}'.format(storage,str(factor)),shell=True)
		except Exception as e:
			print 'Failed because {}'.format(e.__str__())
			sys.exit(1)


if __name__=='__main__':
	d=doSetup()
	if len(sys.argv)>2:
		d.build_tables(*sys.argv[1:])
