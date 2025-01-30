import requests
from bs4 import BeautifulSoup
import threading 
from queue import Queue 
import json
from flatten_json import flatten
from openpyxl import Workbook
import csv
import os
import time

class process_endpoints:

	def __init__(self,api_key,onedrive_path):


		'''
		Initialisation: Here we initialise the endpoints and base url, api_key, rows, columns and the onedrive page
		The variables are initialised so that they can be used across the class by different functions and reduce the need to repeatedly recall variables
		'''

		self.endpoints = ['/launches','/companies','/locations','/missions','/pads','/tags','/vehicles']
		self.base_url = 'https://fdo.rocketlaunch.live/json'
		self.api_key = api_key
		self.columns = []
		self.rows = []
		self.onedrive_path = onedrive_path

	def get_response(self,url):

		for q in range(10):
			try:
				r = requests.get(url,timeout=10) # Sending GET request to the endpoint/url with a timeout of 10 seconds, if the API does not respond in 10 seconds we retry, we retry for 10 times
				js_response = r.json() # JSON from the response which can be further parsed
				return js_response
			except:
				pass
				
	def convert_to_rows(self,flat_json):

		'''
		The API returns a nested JSON which can not be fit into a CSV file, thus we have to flatten the JSON so we can put it into the csv,
		this function basically takes a flattened JSON, then generates row for each item/result in the JSON

		How this function works:

			1. JSON looks like this post flattening: {result[0].name:X, result[0].spacecraft:Y, result[1].name:Z, result[1].spacecraft: M}
			2. For each result (0,1 etc) we develop a dictionary in the dct variable where all variables for a item (like 0,1) are written down, so dct for example looks like
			   this: 

				dct = { 0:{name:X, spacecraft:Y},
						1: {name:Z, spacecraft:M}} 

			3. Now once we have the dct, we go through each result item, and pull the info for each column and write/append into self.rows
		'''

		n = 0
		dct = {}
		for key in flat_json: # Reading the key from flattened JSON
			if 'result_' in key: # Checking if the key belongs to a result object (which we need to put into the csv)
				key_name = key.split('_',2)[-1] # Pull the key name from the key and remove the object number from the key, result[120].spacecrat.name becomes spacecraft.name
				if f'result_{n}' in key: # Checking if the key belongs to the result item we were process or if it has changed, since we flatten the JSON it becomes 1 dimensional, thus this method is needed
					if n not in dct: # Putting the data into the dictionary result item wise
						dct[n] = {key_name:flat_json[key]}
					else:
						dct[n][key_name] = flat_json[key]
				else:
					n+=1
					dct[n] = {key_name:flat_json[key]}

		for row in dct: # Going through each item in dct
			sub_row = []
			for col in self.columns: # Pulling info for each columns
				if col in dct[row]:
					sub_row.append(dct[row][col]) # Adding to row
				else:
					sub_row.append('NA') # If a column doesn't exist write NA
			self.rows.append(sub_row) # Append the row to self.rows for further processing

	def write_to_csv(self,csv_name):

		'''
		Writing the columns, rows into the csv
		'''

		fl = open(f'{csv_name}.csv','w',encoding='utf-8-sig',newline='')
		writer = csv.writer(fl)
		writer.writerow(self.columns)
		writer.writerows(self.rows)
		fl.close()

	def process(self,l):

		'''
		Here we send request to the endpoint, take the JSON and flatten it, convert it to rows using above function
		'''

		endpoint,pg = l
		print(f'>>> {endpoint}: Reading page {pg}')
		if endpoint == '/launches':
			js_response = self.get_response(self.base_url+endpoint+f'?modified_since=1930-01-01T19:02:00Z&after_date=1930-01-01&key={self.api_key}&page={pg}')
		else:
			js_response = self.get_response(self.base_url+endpoint+f'?key={self.api_key}&page={pg}')

		flat_json = flatten(js_response) # Using flatten function imported from flatten_json to convert the JSON into 1 dimension/flatten it
		self.convert_to_rows(flat_json) # Parsing the flat json and converting into rows

	def fetch_data_from_endpoints(self):

		'''
		This is the primary function handling the entire flow
		Flow:
			  1. Send GET request to first page of the endpoint, get the number of pages and data of first page
			  2. Put the first page data into self.rows and columns into self.columns
			  3. Pull data from all other pages, have used multithreading to allow faster processing of all pages
			  4. Once all pages are done we write them into csv
			  5. Once all endpoints are done write into master file
		'''

		for endpoint in self.endpoints:

			if endpoint=='/launches':
				js_response = self.get_response(self.base_url+endpoint+f'?modified_since=1930-01-01T19:02:00Z&after_date=1930-01-01&key={self.api_key}&page=1')
			else:
				js_response = self.get_response(self.base_url+endpoint+f'?key={self.api_key}')

			pages = js_response['last_page'] # Fetching the number of pages
			print(f'>>> {endpoint}: Pages detected: {pages}')
			flat_json = flatten(js_response) # Flatten first page data
			
			self.columns = [] # Reset the columns and rows
			self.rows = []
			dct = {}
			log = []
			n = 0
			for key in flat_json:
				'''
				While setting the self.columns the script tried to pull as many columns as it can by finding the biggest result, so it can write all data into output file
				'''
				if 'result_' in key:
					key_name = key.split('_',2)[-1]
					if f'result_{n}' in key:
						self.columns.append(key_name)
					else:
						n+=1
						if len(self.columns)>len(log):
							log = self.columns[::]
						self.columns = []

			self.columns = log
			self.convert_to_rows(flat_json) # Convert the data from first page into the rows

			def threader():
				while True: 
					worker = q.get()
					self.process(worker)
					q.task_done() 

			q = Queue()
			for x in range(5): # Using 5 threads which will run in parallel to process all pages
				t = threading.Thread(target=threader)
				t.daemon = True
				t.start()

			for pg in range(2,pages+1): # From page 2 onwards pull the data
				q.put([endpoint,pg]) 

			q.join() 

			csv_name = self.onedrive_path+'\\'+endpoint.split('/')[-1]
			self.write_to_csv(f'{csv_name}') # Write data into csv file
			print(f'>>> {endpoint}: All data written to {csv_name}.csv succesfully')

	def merge_files(self):

		'''
		This function when called reads all csv files in onedrive folder then merges them and writes into onedrive path
		'''

		files = os.listdir(self.onedrive_path)
		files = [self.onedrive_path+'\\'+x for x in files if '.csv' in x]
		print(f'>>> Files detected: {files}')
		print(f'>>> Merging all files')
		wb = Workbook()
		wb.remove(wb.active)

		for csv_file in files:
			sheet_name = os.path.splitext(os.path.basename(csv_file))[0][:31]
			ws = wb.create_sheet(title=sheet_name)

			with open(csv_file,'r', encoding='utf-8-sig',newline='') as f:
				reader = csv.reader(f)
				for row in reader:
					ws.append(row)

		td = time.strftime("%Y_%m_%d")
		path = self.onedrive_path+f'\\output_{td}.xlsx'

		wb.save(path)
		wb.close()
		print(f'>>> All files merged and saved in {path}')