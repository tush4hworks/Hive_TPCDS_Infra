import sys
from collections import defaultdict
import commands

class analyze:

	def __init__(self,result_struct):
		self.results=result_struct
		self.f=open('analysis.csv','w+')
	
	def rank_average_execution_time(self):
		try:
			rank_dict=defaultdict(lambda:float(0))
			total_dict=defaultdict(lambda:float(0))
			exception_list=defaultdict(lambda:[])
			for db in self.results.keys():
				for ql in self.results[db].keys():
					for setting in self.results[db][ql].keys():
						if 'NA' in self.results[db][ql][setting]:
							exception_list[setting].append(ql)
						if set(self.results[db][ql][setting])==set(['NA']):
							continue
						rank_dict[setting]+=float(sum([item for item in self.results[db][ql][setting] if item!='NA'])/len([item for item in self.results[db][ql][setting] if item!='NA']))
						total_dict[setting]+=float(sum([item for item in self.results[db][ql][setting] if item!='NA']))
			self.f.write('SETTINGS RANKED ON AVERAGE EXECUTION TIMES\n')
			self.f.write('SETTING,AVERAGE EXECUTION TIME,TOTAL EXECUTION TIME\n')
			for key,val in sorted(rank_dict.items(),key=lambda x:x[1]):
				self.f.write(','.join([key,str(val),str(total_dict[key]),'Failed for Queries '+'-'.join(exception_list[key]) if exception_list[key] else 'Passed' ])+'\n')
		except Exception as e:
			print e.__str__()

	def rank_optimized_queries(self):
		try:
			optimal_dict=defaultdict(lambda:[None,sys.maxint])
			slist=[]
			for db in self.results.keys():
				for ql in self.results[db].keys():
					for setting in self.results[db][ql].keys():
						if set(self.results[db][ql][setting])==set(['NA']):
							continue
						time_taken=float(sum([item for item in self.results[db][ql][setting] if item!='NA'])/len([item for item in self.results[db][ql][setting] if item!='NA']))
						if time_taken<optimal_dict[ql][1]:
							optimal_dict[ql]=[setting,time_taken]
			self.f.write('BEST SETTING FOR EVERY QUERY\n')
			self.f.write('QUERY,SETTING,AVERAGE TIME\n')
			for key in sorted(optimal_dict.iterkeys()):
				self.f.write(','.join([key,optimal_dict[key][0],str(optimal_dict[key][1])])+'\n')
				slist.append(optimal_dict[key][0])
			self.f.write('SETTINGS RANKED ON MOST OPTIMIZED QUERIES\n')
			self.f.write('SETTING,BEST PERFORMING QUERIES\n')
			for setting in sorted(list(set(slist)),key=lambda x:slist.count(x),reverse=True):
				self.f.write(','.join([setting,str(slist.count(setting))])+'\n')
		except Exception as e:
			print e.__str__()

	def dumpToZeppelinInput(self):
		with open('times.csv','w+') as f:
			try:
				for db in self.results.keys():
					for ql in self.results[db].keys():
						for setting in self.results[db][ql].keys():
							for i in range(len(self.results[db][ql][setting])):
								f.write(','.join([ql,setting,str(i+1),self.results[db][ql][setting][i]]))
		status,out=commands.getstatusouput('hadoop fs -put times.csv /tmp')


	def closeFile(self):
		self.f.close()

