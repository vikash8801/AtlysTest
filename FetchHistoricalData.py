import requests
import pandas as pd
from sqlalchemy import create_engine
import sys

API_KEY = sys.argv[1]
companyFile = sys.argv[2]
startDate = sys.argv[3]
endDate = sys.argv[4]


def get_maket_capitalization(symbol):
	url = f'https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={API_KEY}'
	r = requests.get(url)
	data = r.json()
	return data['MarketCapitalization']


def fetch_data(symbol, start_date, end_date):
	url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}&outputsize=full'
	# 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=RELIANCE.BSE&apikey=XHM2AS851HDMG&outputsize=full&startdate=2021-01-01&enddate=2021-01-31'
	# url=f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey=demo'
	response = requests.get(url)
	data = response.json()
	df = pd.DataFrame(data['Time Series (Daily)']).T
	df = df.reset_index()
	df = df.rename(columns={'index': 'date'}).rename(
		columns={'1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'})
	df['symbol'] = symbol
	filtered_data = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
	print(filtered_data)

	return pd.DataFrame(filtered_data)


fopen = open(companyFile, "r")
companies = fopen.read()
fopen.close()
companies_with_mk = [{'symbol':company,'MarketCapitalization':get_maket_capitalization(company)} for company in companies.split(',')]
companies_with_mk = sorted(companies_with_mk, key=lambda x: x['MarketCapitalization'], reverse=True)
print(companies_with_mk)
for company in companies_with_mk[:10]:
	data = fetch_data(company['symbol'], startDate, endDate)
	engine = create_engine('sqlite:///stock.db', echo=False)
	data.to_sql('Historical_Data', con=engine, if_exists='replace', index=False)
