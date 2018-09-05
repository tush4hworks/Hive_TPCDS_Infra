import sys
import requests
import json
import random
from string import ascii_lowercase

class zepInt:

	def __init__(self,host,zepUser,zepPass):
		self.url='http://'+host
		self.username=zepUser
		self.password=zepPass
		self.notebook=None
		self.zepLogin()

	def zepLogin(self):
		resp=requests.post(self.url+'/api/login',data={'userName':self.username,'password':self.password})
		self.cookie=resp.cookies

	def getAllNotebooks(self):
		resp=requests.get(self.url+'/api/notebook',cookies=self.cookie)
		return resp.json()

	def runParagraphs(self):
		if self.notebook:
			for note in self.getAllNotebooks()['body']:
				if note['name']==self.notebook:
					resp=requests.post(self.url+'/api/notebook/job/'+note['id'],cookies=self.cookie)
					break

	def createNote(self,namestr,settingnames):
		try:
			notejson=json.load(open('BaseNote.json'))
			notejson['name']='{}_{}'.format(namestr,''.join([random.choice(ascii_lowercase) for i in range(5)]))
			notejson['paragraphs'][2]['settings']['params']['settings']=','.join(settingnames)
			if len(settingnames)>=2:
				notejson['paragraphs'][3]['settings']['params']['settings']=','.join(settingnames[0:2])
			resp=requests.post(self.url+'/api/notebook',cookies=self.cookie,headers={'Content-Type':'application/json'},data=json.dumps(notejson))
			if resp.status_code==200:
				self.notebook=notejson['name']
		except Exception as e:
			print e.__str__()

	def checkIfExist(self,notebook):
		zepNotes=[note['name'] for note in self.getAllNotebooks()['body']]
		return (notebook in zepNotes)
