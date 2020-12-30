#!/usr/bin/python
import os
import sys
import signal
import json
import yfinance as yf
import pandas as pd
import sqlite3
import argparse
import time
import datetime

def Load(filename):
	if os.path.isfile(filename) is True:
		file = open(filename, "r")
		data = file.read()
		file.close()
		return data
	return ""

def Save (filename, data):
	file = open(filename, "w")
	file.write(data)
	file.close()

def Append (filename, data):
	file = open(filename, "a")
	file.write(data)
	file.close()

def Get5D(ticker):
	'''
		Open            5.500600e+02
		High            5.740000e+02
		Low             5.453700e+02
		Close           5.740000e+02
		Volume          4.893020e+07
		Dividends       0.000000e+00
		Stock Splits    0.000000e+00
		Name: 2020-11-25 00:00:00, dtype: float64

		valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
	'''
	hist = ticker.history(period="5d")
	for idx, row in hist.iterrows():
		print("{0}\t{1:.5g}\t{2:.4g}\t{3:.4g}\t{4:.4g}\t{5:.4g}".format(idx,row['Open'],row['Close'],row['High'],row['Low'],row['Volume']))

def Get1MO(ticker):
	'''
		Open            5.500600e+02
		High            5.740000e+02
		Low             5.453700e+02
		Close           5.740000e+02
		Volume          4.893020e+07
		Dividends       0.000000e+00
		Stock Splits    0.000000e+00
		Name: 2020-11-25 00:00:00, dtype: float64

		valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
	'''
	return ticker.history(period="1mo")

def GetMAX(ticker):
	'''
		Open            5.500600e+02
		High            5.740000e+02
		Low             5.453700e+02
		Close           5.740000e+02
		Volume          4.893020e+07
		Dividends       0.000000e+00
		Stock Splits    0.000000e+00
		Name: 2020-11-25 00:00:00, dtype: float64

		valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
	'''
	return ticker.history(period="max")

g_exit = False
def signal_handler(signal, frame):
	global g_exit
	print("Accepted signal from other app")
	g_exit = True

def IsTickerExist(curs, ticker):
	query = "SELECT * FROM stocks_info WHERE `ticker` = '{0}'".format(ticker)
	curs.execute(query)
	rows = curs.fetchall()
	return len(rows) > 0

def GetTickersLocalDB(curs):
	tickers = []
	query = "SELECT ticker FROM stocks_info"
	curs.execute(query)
	rows = curs.fetchall()
	for row in rows:
		tickers.append(row[0])
	return tickers

def TickersFromFunder():
	ticker  = ""
	tickers = []
	path = os.path.join("resource","companies.csv")
	if os.path.isfile(path) is True:
		print("Appending tickers from Funder")
		dfCompanies = pd.read_csv(path)
		for index, row in dfCompanies.iterrows():
			splitted = row[1].split(" ")
			if len(splitted) > 1:
				ticker = splitted[0]
			else:
				ticker = row[1]
		
			tickers.append({
				"ticker": ticker,
				"name": "",
				"country": "",
				"sector": "",
				"industry": ""
			})
	return tickers

def TickersFromNasdaq():
	tickers = []
	path = os.path.join("resource","nasdaq.csv")
	if os.path.isfile(path) is True:
		print("Appending tickers from Nasdaq")
		dfCompanies = pd.read_csv(path)
		for index, row in dfCompanies.iterrows():
			country = ""
			if row[6] == row[6]: # Remove nan
				country = row[6]
			sector = ""
			if row[9] == row[9]: # Remove nan
				sector = row[9]
			industry = ""
			if row[10] == row[10]: # Remove nan
				industry = row[10]
			name = ""
			if row[1] == row[1]: # Remove nan
				name = row[1]
			tickers.append({
				"ticker": row[0],
				"name": name,
				"country": country,
				"sector": sector,
				"industry": industry
			})
	return tickers

def IsStockPriceExist(curs, date, ticker):
	query = "SELECT 1 FROM stocks_price WHERE date = '{0}' AND ticker = '{1}'".format(date,ticker) 
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		return True
	return False

def main():
	global g_exit
	signal.signal(signal.SIGINT, signal_handler)

	parser = argparse.ArgumentParser(description='Stock DB Creator')
	parser.add_argument('--query', action='store_true', help='Create queries from stocks resources')
	parser.add_argument('--create', action='store_true', help='Crerate DB from queries')
	parser.add_argument('--update', action='store_true', help='Update DB - Create only query file')
	parser.add_argument('--write', action='store_true', help='Update DB - Execute query file')
	parser.add_argument('--tickers', action='store', dest='tickers', help='Append tickers from tickers file')
	parser.add_argument('--nasdaq', action='store', dest='nasdaq', help='Append tickers from tickers file')
	args = parser.parse_args()

	curs = None
	db_exist = False
	path = os.path.join("output","stocks.db")
	if os.path.isfile(path) is True:
		db_exist = True

		path = os.path.join("output","stocks.db")
		conn = sqlite3.connect(path)
		curs = conn.cursor()

	tickers = []
	if args.tickers is not None:
		path = os.path.join("resource",args.tickers)
		data = Load(path)
		if data is not None:
			tickers = data.split("\n")
	else:
		if args.update is None:
			tickers = TickersFromNasdaq()
			#tk_funder = TickersFromFunder()
	
	query_counter 		= 1
	query_file_index 	= 1
	failed_tickers_list = []
	QUERIES_PER_FILE 	= 5000000
	
	if args.query is True:
		stock_price_path = os.path.join("output","query_stocks_price_{0}".format(query_file_index))
		for index, obj_ticker in enumerate(tickers[:]):
			if g_exit is True:
				return
			ticker = obj_ticker["ticker"]
			try:
				objtk 	= yf.Ticker(ticker)
				data 	= GetMAX(objtk)
			except Exception as e:
				print("Exception (YAHOO) {0}".format(e))
			
			print("({0}/{1}) Ticker Name: {2}, Size: {3}".format(index, len(tickers), ticker, data.shape[0]))
			if data.size > 0:
				count = 0
				# data.iloc[data.shape[0]-1]
				last = data.shape[0]-1
				try:
					for idx, row in data.iterrows():
						if g_exit is True:
							return
						stock_ts = time.mktime(datetime.datetime.strptime("{0}".format(idx), "%Y-%m-%d %H:%M:%S").timetuple())
						if count == 0:
							ipo = idx
						elif count == last:
							query_stocks_info = "INSERT OR REPLACE INTO stocks_info (`created_date`,`last_date`,`name`,`ticker`,`vol`,`open`,`close`,`low`,`high`,`sector`,`industry`) SELECT '{0}','{1}','{2}','{3}',{4},{5},{6},{7},{8},'{9}','{10}' WHERE NOT EXISTS (SELECT 1 FROM stocks_info WHERE created_date = '{0}' AND ticker = '{3}');".format(ipo,idx,obj_ticker["name"],ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'],obj_ticker["sector"],obj_ticker["industry"])
							path = os.path.join("output","query_stocks_info")
							Append(path,"{0}\n".format(query_stocks_info))
						count += 1
						query_stocks_price = "INSERT INTO stocks_price VALUES ({0},'{1}','{2}',{3},{4},{5},{6},{7});".format(stock_ts,idx,ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'])
						if query_counter % QUERIES_PER_FILE == 0:
							query_file_index += 1
							stock_price_path = os.path.join("output","query_stocks_price_{0}".format(query_file_index))
						Append(stock_price_path,"{0}\n".format(query_stocks_price))
						query_counter += 1
				except Exception as e:
					print("Exception {0}\n{1}\n{2}".format(e,query_stocks_info,query_stocks_price))
			else:
				failed_tickers_list.append(ticker)
		print("Failed Tickers List:\n{0}".format(failed_tickers_list))

	if args.create is True:
		path = os.path.join("output","query_stocks_info")
		query_stocks_info_list = Load(path).split("\n")
		if args.nasdaq is not None:
			stock_price_path = os.path.join("output","query_stocks_price_{0}".format(args.nasdaq))
			query_stocks_price_list = Load(stock_price_path).split("\n")

		path = os.path.join("output","stocks.db")
		conn = sqlite3.connect(path)
		curs = conn.cursor()

		# Create table
		curs.execute('''CREATE TABLE IF NOT EXISTS stocks_price (timestamp real, date text, ticker text, vol int, open real, close real, low real, high real)''')
		curs.execute('''CREATE TABLE IF NOT EXISTS stocks_info (
							created_date text, 
							last_date text,
							name text, 
							ticker text, 
							vol int, 
							open real, 
							close real, 
							low real, 
							high real,  
							sector text, 
							industry text)''')

		print("Create stock_info DB")
		# Create info DB
		for index, query in enumerate(query_stocks_info_list[:]):
			try:
				print("{0}/{1}".format(index,len(query_stocks_info_list)))
				curs.execute(query)
				if g_exit is True:
					return
			except Exception as e:
				print("Exception (SQLITE) {0}".format(e))
		# Save (commit) the changes
		try:
			print("Commit to DB")
			conn.commit()
		except Exception as e:
			print("Exception (SQLITE COMMIT) {0}".format(e))
		
		print("Create stocks_price DB")
		# Create stocks DB
		if args.nasdaq is not None:
			for index, query in enumerate(query_stocks_price_list[:]):
				try:
					print("{0}/{1}".format(index,len(query_stocks_price_list)))
					curs.execute(query)
					# Save (commit) the changes
					conn.commit()
					if g_exit is True:
						return
				except Exception as e:
					print("Exception (SQLITE) {0}".format(e))
		
		conn.close()
	
	if args.update is True:
		if curs is not None:
			tickers = GetTickersLocalDB(curs)
			stock_price_update_path = os.path.join("output","query_stocks_price_update")
			for index, ticker in enumerate(tickers[:]):
				if g_exit is True:
					return
				try:
					objtk 	= yf.Ticker(ticker)
					data 	= Get1MO(objtk)
				except Exception as e:
					print("Exception (YAHOO) {0}".format(e))
				
				print("({0}/{1}) Ticker Name: {2}, Size: {3}".format(index, len(tickers), ticker, data.shape[0]))
				if data.size > 0:
					count = 0
					last = data.shape[0]-1
					try:
						query_stocks_price = ""
						for idx in reversed(data.index):
							if g_exit is True:
								return
							stock_ts = time.mktime(datetime.datetime.strptime("{0}".format(idx), "%Y-%m-%d %H:%M:%S").timetuple())
							if IsStockPriceExist(curs, idx, ticker) is False:
								query_stocks_price = "INSERT INTO stocks_price VALUES ({0},'{1}','{2}',{3},{4},{5},{6},{7});".format(stock_ts,idx,ticker,data.loc[idx,'Volume'],data.loc[idx,'Open'],data.loc[idx,'Close'],data.loc[idx,'Low'],data.loc[idx,'High'])
								#query_stocks_price = "INSERT OR REPLACE INTO stocks_price (`timestamp`,`date`,`ticker`,`vol`,`open`,`close`,`low`,`high`) SELECT '{0}','{1}','{2}',{3},{4},{5},{6},{7} WHERE NOT EXISTS (SELECT 1 FROM stocks_price WHERE date = '{1}' AND ticker = '{2}');".format(stock_ts,idx,ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'])
								Append(stock_price_update_path,"{0}\n".format(query_stocks_price))
							else:
								print("Stock date {0} exist".format(idx))
								break
					except Exception as e:
						print("Exception {0}\n{1}".format(e,query_stocks_price))
				else:
					failed_tickers_list.append(ticker)
			
	if args.write is True:
		stock_price_update_path = os.path.join("output","query_stocks_price_update")
		query_stocks_price_list = Load(stock_price_update_path).split("\n")
		print("Update stock_info DB")
		# Create info DB
		for index, query in enumerate(query_stocks_price_list[:]):
			try:
				print("{0}/{1}".format(index,len(query_stocks_price_list)))
				curs.execute(query)
				if g_exit is True:
					return
			except Exception as e:
				print("Exception (SQLITE) {0}".format(e))
		# Save (commit) the changes
		try:
			print("Commit to DB")
			conn.commit()
		except Exception as e:
			print("Exception (SQLITE COMMIT) {0}".format(e))

if __name__ == "__main__":
	main()

'''
for index, ticker in enumerate(tickers[:]):
	# SELECT * FROM stocks WHERE strftime('%Y-%m-%d', date) BETWEEN "1983-11-20 00:00:00" AND "1983-11-25 00:00:00"
	print("({0}) Ticker Name: {1}".format(index, ticker))

	query_stocks_price = ""
	query_stocks_info = ""
	if start_harvesting is True:
		if data.size > 0:
			count = 0
			try:
				for idx, row in data.iterrows():
					if count == 0:
						first_row = data.iloc[0]
						last_row  = data.iloc[data.shape[0]-1]
						# query_stocks_info = "INSERT OR REPLACE INTO stocks_info (`created_date`,`name`,`ticker`,`vol`,`open`,`close`,`low`,`high`) SELECT '{0}','{1}','{2}',{3},{4},{5},{6},{7} WHERE NOT EXISTS (SELECT 1 FROM stocks_info WHERE created_date = '{0}' AND ticker = '{2}');".format(idx,'',ticker,last_row['Volume'],last_row['Open'],last_row['Close'],last_row['Low'],last_row['High'])
						query_stocks_info = "INSERT INTO stocks_info VALUES('{0}','{1}','{2}',{3},{4},{5},{6},{7});".format(idx,'',ticker,last_row['Volume'],last_row['Open'],last_row['Close'],last_row['Low'],last_row['High'])
						curs.execute(query_stocks_info)
					count += 1
					# Insert a row of data
					# query_stocks_price = "INSERT OR REPLACE INTO stocks_price (`date`,`ticker`,`vol`,`open`,`close`,`low`,`high`) SELECT '{0}','{1}',{2},{3},{4},{5},{6} WHERE NOT EXISTS (SELECT 1 FROM stocks_price WHERE date = '{0}' AND ticker = '{1}');".format(idx,ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'])
					query_stocks_price = "INSERT INTO stocks_price VALUES ('{0}','{1}',{2},{3},{4},{5},{6});".format(idx,ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'])
					curs.execute(query_stocks_price)
				# Save (commit) the changes
				conn.commit()
			except Exception as e:
				print("Exception {0}\n{1}\n{2}".format(e,query_stocks_info,query_stocks_price))
		else:
			failed_tickers_list.append(ticker)
'''
