from lib import process_endpoints
from dotenv import load_dotenv
import os

load_dotenv() # Loading the enviorement variables from .env file

def main():

	api_key = os.getenv('API_KEY') # Loading hte api key and onedrive path
	onedrive_path = os.getenv('ONEDRIVE_PATH')

	instance = process_endpoints(api_key,onedrive_path) # Calling the class from lib file
	instance.fetch_data_from_endpoints() # Fetching and processing all endpoints
	instance.merge_files() # Merging all files into a master file

main()