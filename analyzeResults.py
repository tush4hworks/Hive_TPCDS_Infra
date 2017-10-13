import sys
from collections import defaultdict

class analyze:

	def __init__(self,result_struct):
		self.results=result_struct
		self.f=open('analysis.csv','w+')
	
	def rank_average_execution_time(self):
		rank_dict=defaultdict(lambda:float(0))
		for db in self.results.keys():
			for ql in self.results[db].keys():
				for setting in self.results[db][ql].keys():
					rank_dict[setting]+=float(sum([item for item in self.results[db][ql][setting] if item!='NA'])/len([item for item in self.results[db][ql][setting] if item!='NA']))
		self.f.write('SETTINGS RANKED ON AVERAGE EXECUTION TIMES\n')
		self.f.write('SETTING,AVERAGE EXECUTION TIME\n')
		for key,val in sorted(rank_dict.items(),key=lambda x:x[1]):
			self.f.write(','.join([key,str(val)])+'\n')

	def rank_optimized_queries(self):
		optimal_dict=defaultdict(lambda:[None,sys.maxint])
		slist=[]
		for db in self.results.keys():
			for ql in self.results[db].keys():
				for setting in self.results[db][ql].keys():
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
		print slist
		for setting in sorted(list(set(slist)),key=lambda x:slist.count(x)):
			self.f.write(','.join([setting,str(slist.count(setting))])+'\n')

	def closeFile(self):
		self.f.close()

