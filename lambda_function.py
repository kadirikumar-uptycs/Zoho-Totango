#!/home/bamboo/anaconda3/bin/python
# v.17 2024-04-30 Appended records for accounts with no tickets to the end of the priority pivot table.
# v.16 2023-04-04 add 'Collection' and 'Account' to outputted file names
# v.15 2023-04-03 version 0.15 changed date format
#      from: 28 Feb 2023 05:41 AM 
#      to: 2023-02-28T05:41:00z

import boto3


DEBUG = True
print ('\n')

# import argparse 
# import sys
import os
import datetime as dt
import csv
import io


customer_accounts = [
    {
        "Name": "The Co-operators",
        "Account id": "0015G00001xhSMDQA2"
    },
    {
        "Name": "Plaid",
        "Account id": "001f400001SKYFtAAP"
    },
    {
        "Name": "Nutanix",
        "Account id": "001f400000tt9cvAAA"
    },
    {
        "Name": "FINRA",
        "Account id": "001f400001PqnPWAAZ"
    },
    {
        "Name": "Chime Bank",
        "Account id": "0015G00001W7Bo7QAF"
    },
    {
        "Name": "Shein Group Ltd",
        "Account id": "0015G000021jao5QAA"
    },
    {
        "Name": "Lookout",
        "Account id": "001f4000004Ai5HAAS"
    },
    {
        "Name": "Highspot",
        "Account id": "0015G00001nrRwBQAU"
    },
    {
        "Name": "phoenixNAP",
        "Account id": "0015G00001e4LZyQAM"
    },
    {
        "Name": "Axonius",
        "Account id": "0015G00001bvEEbQAM"
    },
    {
        "Name": "Greenlight Financial Technology",
        "Account id": "001f400001KxwCrAAJ"
    },
    {
        "Name": "Cedar",
        "Account id": "0015G00001W7mMGQAZ"
    },
    {
        "Name": "Earnest",
        "Account id": "001f400000cmDEvAAM"
    },
    {
        "Name": "Victory Live",
        "Account id": "0015G00002OHldCQAT"
    },
    {
        "Name": "Brain Labs Digital Ltd",
        "Account id": "0015G00002HGhmOQAT"
    },
    {
        "Name": "Lumin Digital",
        "Account id": "001f400000rxt5GAAQ"
    },
    {
        "Name": "Serato",
        "Account id": "0015G00002QzvZnQAJ"
    },
    {
        "Name": "Handle Financial (PayNearMe)",
        "Account id": "001f400001C8o5RAAR"
    },
    {
        "Name": "CityBase",
        "Account id": "001f400000cmLPbAAM"
    },
    {
        "Name": "STX",
        "Account id": "0015G00002c99TYQAY"
    },
    {
        "Name": "ControlPlane",
        "Account id": "0015G00002HJgtgQAD"
    },
    {
        "Name": "AppCensus Inc",
        "Account id": "0015G00002hOIq4QAG"
    },
    {
        "Name": "VizyPay",
        "Account id": "0015G00002XGpiEQAT"
    },
    {
        "Name": "Crayon",
        "Account id": "0015G000024kAYqQAM"
    },
    {
        "Name": "Ontra",
        "Account id": "001Pa000007mozNIAQ"
    },
    {
        "Name": "SolCyber",
        "Account id": "0015G00002J0i5BQAR"
    },
    {
        "Name": "Biofourmis",
        "Account id": "0015G000020ROTQQA4"
    },
    {
        "Name": "IBM",
        "Account id": "001f400000tt9doAAA"
    },
    {
        "Name": "Comcast Corporation",
        "Account id": "001f4000004Ai3zAAC"
    },
    {
        "Name": "Urban One",
        "Account id": "0015G00002alnsIQAQ"
    },
    {
        "Name": "Paypal Inc",
        "Account id": "001f400000FdKuZAAV"
    },
    {
        "Name": "Netflix, Inc.",
        "Account id": "001f400000Fb4r5AAB"
    },
    {
        "Name": "MuleSoft",
        "Account id": "001f4000004Ai5MAAS"
    },
    {
        "Name": "Tredium",
        "Account id": "001f400001PokzyAAB"
    },
    {
        "Name": "Tesla Motors, Inc.",
        "Account id": "001f400001Go3cfAAB"
    },
    {
        "Name": "SpaceX",
        "Account id": "0015G00001fp24DQAQ"
    },
    {
        "Name": "Stripe",
        "Account id": "001f400000rxt8AAAQ"
    },
    {
        "Name": "SEI",
        "Account id": "001f400000cmLP7AAM"
    },
    {
        "Name": "Telenor",
        "Account id": "001f400000gIvgZAAS"
    },
    {
        "Name": "WIX",
        "Account id": "001f400001BSOeeAAH"
    },
    {
        "Name": "Copado",
        "Account id": "0015G00001i1isMQAQ"
    }
]


try:
	import pandas as pd
except ModuleNotFoundError:
	print ('\nPandas required. Please install pandas.')
	print ('pip install pandas\n\n')
	exit()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)


def lambda_handler(event, context):
	bucket_name = 'uptycs-zoho-totango-prod'
	# source_key = 'zapier/zoho/input/ExportReport_1692196503869.xls'
	print(event)
	bucket_name = event.get('Records')[0].get('s3').get('bucket').get('name')
	print('bucket_name = ', bucket_name)
	source_key = event.get('Records')[0].get('s3').get('object').get('key')
	
	print('source_key = ', source_key)
	 # Set your S3 bucket and file details
	
	source_file_tmp = '/tmp/file.xls'  # Destination path in Lambda execution environment

	# Initialize the S3 client
	s3 = boto3.client('s3')

	# Copy the file from S3 to Lambda's /tmp directory
	try:
		s3.download_file(bucket_name, source_key, source_file_tmp)
		print('File copied successfully')
		# return {
		#	 'statusCode': 200,
		#	 'body': 'File copied successfully.'
		# }
	except Exception as e:
		return {
			'statusCode': 500,
			'body': f'Error: {str(e)}'
		}
	
	
	
	
	
	
	
	
	
	# List of columns to pivot on
	# tables = [ 'Severity', 'Request Type', 'Ticket Owner', 'Status (Ticket)', 'Unresolved']
	tables = [ 'Priority (Ticket)', 'Request Type', 'Ticket Owner', 'Status (Ticket)', 'Unresolved']

	
	
	print ('\n\n\n')

		
	def read_file(file):
		pdf = pd.read_excel(file)
		pdf.columns = pdf.iloc[3]
		pdf = pdf[4:-1]		
		return pdf
	

		
	
	def normalize_data(pdf):
		columns = pdf.columns.values
		# for col in ['Severity', 'Request Type', 'Status (Ticket)']:
		# 	if col in columns:
		# 		pdf[col] = pdf[col].apply(lambda x: x.title())
		# pdf['Number of Tickets'] = 1
		# pdf.rename(columns = {"SF Account ID":"Account ID", "Ticket Owner":"Agent"},inplace=True)
		# return pdf
		for col in ['Priority (Ticket)', 'Request Type', 'Status (Ticket)']:
			if col in columns:
				pdf[col] = pdf[col].apply(lambda x: x.title())
		pdf['Number of Tickets'] = 1
		pdf.rename(columns = {"SF Account ID":"Account ID", "Ticket Owner":"Agent"},inplace=True)
		return pdf
		
	def transpose(pdf, primary, category, value='count'):
		new_pdf = pd.DataFrame()
		categories = list(set(pdf[category].tolist()))
		primaries = list(set(pdf[primary].tolist()))
		for prime in primaries:
			mini = pdf.loc[pdf[primary] == prime]
			index = len(new_pdf)
			new_pdf.loc[index,primary] = prime
			for __,row in mini.iterrows():
				new_pdf.loc[index,row[category]] = row[value]				
		return new_pdf
	
	# def create_pivot(pdf,primary, category, value='Number of Tickets'):
	# 	pdf = pdf.groupby([primary, category],as_index=False).agg({value:'sum'})
	# 	pdf = transpose(pdf,primary, category, value).fillna(0)
	# 	columns = pdf.columns.values
	# 	for col in columns[1:]:
	# 		pdf[col] = pdf[col].astype('int')
	# 	pdf = pdf.sort_values(by = [primary], ascending=True)
	# 	try:
	# 		del(pdf['-'])
	# 	except KeyError:
	# 		pass
	# 	return pdf
	def create_pivot(pdf,primary, category, value='Number of Tickets'):
		pdf = pdf.groupby([primary, category],as_index=False).agg({value:'sum'})
		pdf = transpose(pdf,primary, category, value).fillna(0)
		columns = pdf.columns.values
		for col in columns[1:]:
			pdf[col] = pdf[col].astype('int')
		pdf = pdf.sort_values(by = [primary], ascending=True)
		try:
			del(pdf['-'])
		except KeyError:
			pass
		return pdf

	# padding remaining account records at the end with all zeroes
	def pad_zero_records(df):
		new_rows = []
		all_accounts = set([account['Account id'] for account in customer_accounts])
		existing_accounts = set(df['Account ID'])
		missing_accounts = list(all_accounts - existing_accounts)
		for account in missing_accounts:
			new_rows.append({'Account ID': account, 'High': 0, 'Low': 0, 'Medium': 0, 'Blocker': 0, 'Grand Total': 0})
		new_df = pd.DataFrame(new_rows)
		df = pd.concat([df, new_df], ignore_index=True)
		return df
	
	def change_dates(x):
		# Change From: 28 Feb 2023 05:41 AM
		# To: 2023-03-15T12:31:00z
		try:
			t = dt.datetime.strptime(x, '%d %b %Y %H:%M %p')
		except ValueError:
			return ''
		H = int(t.strftime('%H'))
		if 'PM' in x and H < 12:
			t = t + dt.timedelta(hours=12)
		return t.strftime('%Y-%m-%dT%H:%M:00z')
	
	cwd = os.getcwd()
	pdf = read_file(source_file_tmp)

	pdf = normalize_data(pdf)
	the_time = dt.datetime.now().strftime('%Y%m%d%H%M')
	for table in tables:
		try:
			if DEBUG: print (f'\nWORKING ON {table}')
			t = table.replace(' ','_').lower().replace('(','').replace(')','')
		
		
			# if t in ['severity','request_type','status_ticket']:
			if t in ['priority_ticket','request_type','status_ticket']:
				file = f'zapier/zoho/output/Totango_Account_{t}.csv'
			else:
				file = f'zapier/zoho/output/Totango_Collection_{t}.csv'
		
			if DEBUG: print (f'Output will be stored at:\n{file}')
		
			if table =='Ticket Owner':
				results = pdf.groupby(['Account ID', 'Agent'],as_index=False).agg({'Number of Tickets':'sum'})
		
			# elif table == 'Severity':
			# 	results = create_pivot(pdf, 'Account ID',table)
			# 	cols = results.columns.values
			# 	for index, row in results.iterrows():
			# 		answer = 0
			# 		for col in cols:
			# 			try:
			# 				answer += int(row[col])
			# 			except ValueError:
			# 				pass
			# 		results.loc[index,'Grand Total'] = str(int(answer)).replace('.0','')
			elif table == 'Priority (Ticket)':
				results = create_pivot(pdf, 'Account ID',table)
				cols = results.columns.values
				for index, row in results.iterrows():
					answer = 0
					for col in cols:
						try:
							answer += int(row[col])
						except ValueError:
							pass
					results.loc[index,'Grand Total'] = str(int(answer)).replace('.0','')

				# padd rows for other accounts with no tickets
				results = pad_zero_records(results)
		
			elif table == 'Unresolved':
				results = read_file(source_file_tmp)
	# 			results = parse_arguments_and_read_file()
				try:
					del (results['Account Name'])
				except KeyError:
					print ('Original spreadsheet did not have Account Name as a field')
				try:
					del(results['Jira URL'])
				except KeyError:
					print('Original spreadsheet did not have Jiral URL as a field')
		
				# goal = ['Account ID', 'Ticket Id', 'Severity', 'Request Type', 'Status', 'Status Date', 'Ticket Age', 'Created Time', '1st Status DateNTime', 'Ticket Owner', 'Modified Time', 'Subject','Jira ID', 'Ticket URL']
				goal = ['Account ID', 'Ticket Id', 'Priority (Ticket)', 'Request Type', 'Status', 'Status Date', 'Ticket Age', 'Created Time', '1st Status DateNTime', 'Ticket Owner', 'Modified Time', 'Subject', 'Ticket URL']
				results.columns = goal
		
			else:
				results = pdf.groupby(['Account ID',table], as_index=False).agg({'Number of Tickets':'sum'})
				results = create_pivot(results, 'Account ID',table)
		
			results = results.sort_values(by=['Account ID'],ascending=True)
		
			
			# Modify dates so they match Totango Dates
			cols = results.columns.values
			dates_and_times = [x for x in cols if 'time' in x.lower() or 'date' in x.lower()]
			for col in dates_and_times:
				results[col] = results[col].apply(lambda x: change_dates(x))
				results = results.loc[results[col] != '']
			
	
			with io.StringIO() as csv_buffer:
				results.to_csv(csv_buffer,  index=False, quoting=csv.QUOTE_ALL)
				
				response = s3.put_object(
					Bucket=bucket_name, Key=file, Body=csv_buffer.getvalue()
				)
	# 		results.to_csv(file, index=False, quoting=csv.QUOTE_ALL)
		except:
			print(f'Error in {table}')
	
	print ('\n\n')

	return {
		'statusCode': 200,
		'body': 'Hello from Lambda!'
	}
