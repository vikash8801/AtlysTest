import datetime

import requests
import pandas as pd
from sqlalchemy import create_engine
import sys


API_KEY = sys.argv[1]
companies = sys.argv[2]

def get_maket_capitalization(symbol):
	url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={API_KEY}'
	r = requests.get(url)
	data = r.json()
	return data['MarketCapitalization']

def fetch_data(symbol,date):
	# url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}'
	url=f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=demo'
	response = requests.get(url)
	data = response.json()
	df = pd.DataFrame(data['Time Series (Daily)'][date]).T
	df = df.reset_index()
	df= df.rename(columns={'index':'Date'}).rename(columns={'1. open':'Open','2. high':'High','3. low':'Low','4. close':'Close','5. volume':'Volume'})
	df['Company'] = symbol

	return df


fopen = open(companyFile, "r")
companies = fopen.read()
fopen.close()
companies_with_mk = [{'symbol':company,'MarketCapitalization':get_maket_capitalization(company)} for company in companies.split(',')]
companies_with_mk = sorted(companies_with_mk, key=lambda x: x['MarketCapitalization'], reverse=True)
print(companies_with_mk)
for company in companies_with_mk[:10]:
	yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
	data = fetch_data(company,yesterday)
	engine = create_engine('sqlite:///stock.db', echo=False)
	data.to_sql('Historical_Data', con=engine, if_exists='append', index=False)