#!/home/bamboo/anaconda3/bin/python
# Take spreadsheet downloaded from Zoho and create a number of new
# spreadsheets that are pivot tables of the first. For example, 
# one of the output spreadsheets is called priority, and it is a spreadsheet
# with columns = possible priority values, and the rows showing the number
# of tickets of that priority for each customer

# v.17 2023-09-06 change column and table 'Severity' to 'Priority (Ticket)'
# v.16 2023-04-04 add 'Collection' and 'Account' to outputted file names
# v.15 2023-04-03 version 0.15 changed date format 
#      from: 28 Feb 2023 05:41 AM 
#      to: 2023-02-28T05:41:00z

DEBUG = True
print ('\n')


# Import necessary modules
import argparse 
import sys
import os
import datetime as dt
import csv

try:
	import pandas as pd
except ModuleNotFoundError:
	print ('\nPandas required. Please install pandas.')
	print ('pip install pandas\n\n')
	exit()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

try:
	import xlrd
except ModuleNotFoundError:
	print ('\nxlrd required. Please install xlrd.')
	print ('pip install xlrd\n\n')
	exit()	



# List of columns to pivot on
tables = [ 'Priority (Ticket)', 'Request Type', 'Ticket Owner', 'Status (Ticket)', 'Unresolved']


print ('\n\n\n')
def parse_arguments_and_read_file():
	desc = 'Create several pivoted csvs from an original excel file.'
	parser = argparse.ArgumentParser(prog=sys.argv[0],description=desc)
	parser.add_argument('path',help='/path/to/original_excel_file.xls')
	args = parser.parse_args()
	if len(args.path.split('/')) == 1:
		file = f'{os.getcwd()}/{args.path}'
	else:
		file = args.path

	try:
		handler = open(file)
		handler.close()
	except FileNotFoundError:
		print(f'file {file} is not valid\n\n')
		exit()

	pdf = pd.read_excel(file)
	pdf.columns = pdf.iloc[3]
	pdf = pdf[4:-1]
	return pdf

def normalize_data(pdf):
	# This section primarily exists to deal with historic tickets where
	# people manually typed things in, before input was restricted to
	# drop down menus
	columns = pdf.columns.values
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
pdf = parse_arguments_and_read_file()
print ('\nRead in zoho spreadsheet\n')

pdf = normalize_data(pdf)
print ('\nTweaked the spreadsheet to make data look the same\n')

the_time = dt.datetime.now().strftime('%Y%m%d%H%M')
print ('Time now:',the_time,'\n\n')

for table in tables:

	if DEBUG: print (f'\nWORKING ON {table}')
	t = table.replace(' ','_').lower().replace('(','').replace(')','')

	if t in ['priority_ticket','request_type','status_ticket']:
		file = f'{cwd}/Totango_Account_{t}_{the_time}.csv'
	else:
		file = f'{cwd}/Totango_Collection_{t}_{the_time}.csv'

	if DEBUG: print (f'Output will be stored at:\n{file}')

	if table =='Ticket Owner':
		results = pdf.groupby(['Account ID', 'Agent'],as_index=False).agg({'Number of Tickets':'sum'})

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

	elif table == 'Unresolved':
		results = parse_arguments_and_read_file()
		try:
			del (results['Account Name'])
		except KeyError:
			print ('Original spreadsheet did not have Account Name as a field')
		try:
			del(results['Jira URL'])
		except KeyError:
			print('Original spreadsheet did not have Jiral URL as a field')

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


	results.to_csv(file, index=False, quoting=csv.QUOTE_ALL)
	

print ('\n\n')

