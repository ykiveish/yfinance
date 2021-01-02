#!/usr/bin/python
import os
import sys
import signal
import json
import yfinance as yf
import pandas as pd
import sqlite3
import argparse
import datetime
import time

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

g_exit = False
def signal_handler(signal, frame):
	global g_exit
	print("Accepted signal from other app")
	g_exit = True

def GetStockInfo(curs, ticker):
	query = "SELECT * FROM stocks_info WHERE `ticker` = '{0}'".format(ticker)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		return rows[0]
	return None

def GetStocks(curs, price, year, month, day):
	if month < 10:
		month = '0{0}'.format(month)
	if day < 10:
		day = '0{0}'.format(day)
	buying_stocks 	= []
	tickers_list 	= []
	query = "select ticker, close from stocks_price where close < {close} and vol > 0 and date = '{year}-{month}-{day} 00:00:00'".format(close=price,year=year,month=month,day=day)
	print(query)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		for row in rows:
			buying_stocks.append({
				"ticker": row[0],
				"price": row[1]
			})
			tickers_list.append(row[0])
	return buying_stocks, tickers_list

def GetStocksByTS(curs, price, year, month, day):
	if month < 10:
		month = '0{0}'.format(month)
	if day < 10:
		day = '0{0}'.format(day)
	
	d = datetime.date(int(year),int(month),int(day))
	unixtime = time.mktime(d.timetuple())

	buying_stocks 	= []
	tickers_list 	= []
	query = "select stocks_price.* from (select timestamp from stocks_price where close > {low} and close < {high} and vol > 0 and timestamp > {ts} ORDER BY timestamp LIMIT 1) as tblTS inner join stocks_price on stocks_price.timestamp = tblTS.timestamp where stocks_price.close > {low} and stocks_price.close < {high} and stocks_price.vol > 0".format(low=price[0],high=price[1],ts=unixtime)
	# print(query)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		for row in rows:
			buying_stocks.append({
				"ts": row[0],
				"date": row[1],
				"ticker": row[2],
				"price": row[5]
			})
			tickers_list.append(row[0])
	return buying_stocks, tickers_list

def GetStockByTS(curs, ticker, year, month, day):
	if month < 10:
		month = '0{0}'.format(month)
	if day < 10:
		day = '0{0}'.format(day)

	d = datetime.date(int(year),int(month),int(day))
	unixtime = time.mktime(d.timetuple())

	query = "select * from stocks_price where ticker = '{ticker}' and timestamp > {ts} ORDER BY timestamp LIMIT 1".format(ticker=ticker,ts=unixtime)
	# print(query)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		return {
			"ts": rows[0][0],
			"date": rows[0][1],
			"ticker": rows[0][2],
			"price": rows[0][5]
		}
	return None

def GetClosestStocks(curs, price, year, month, day):
	re_day = day
	tries  = 0
	while(tries < 10):
		#stocks, tickers = GetStocks(curs,price,year,month,re_day)
		stocks, tickers = GetStocksByTS(curs,price,year,month,re_day)
		if stocks is not None:
			return stocks, tickers
		re_day += 1
		tries  += 1
	return None, None

def GetStock(curs, ticker, year, month, day):
	if month < 10:
		month = '0{0}'.format(month)
	if day < 10:
		day = '0{0}'.format(day)
	query = "select ticker, close from stocks_price where ticker = '{ticker}' and date = '{year}-{month}-{day} 00:00:00'".format(ticker=ticker,year=year,month=month,day=day)
	# print(query)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		return {
				"ticker": rows[0][0],
				"price": rows[0][1]
		}
	return None

def GetClosestStock(curs, ticker, year, month, day):
	re_day = day
	tries  = 0
	while(tries < 10):
		#stock = GetStock(curs,ticker,year,month,re_day)
		stock = GetStockByTS(curs,ticker,year,month,re_day)
		if stock is not None:
			return stock
		re_day += 1
		tries  += 1
	return None

def GetDay(year, month, day):
	query = "{year}-{month}-{day}".format(year=year,month=month,day=day)
	date_time_obj = datetime.datetime.strptime(query, '%Y-%m-%d')
	return (date_time_obj.weekday())

def GetWorkingDate(year, month, day):
	weekday = GetDay(year, month, day)
	if weekday == 5:
		day += 2
	elif weekday == 6:
		day += 1
	
	return year, month, day

def GetMAX(ticker):
	'''
		Open,High,Low,Close,Volume,Dividends,Stock Splits
	'''
	hist = []
	objtk = yf.Ticker(ticker)
	data = objtk.history(period="max")
	for idx, row in data.iterrows():
		hist.append({
			"date": "{0}".format(idx),
			"open": row['Open'],
			"close": row['Close'],
			"high": row['High'],
			"low": row['Low'],
			"vol": row['Volume']
		})
	return hist

def Run(start, stop, info, sell_interval):
	global g_exit
	# Calculate running months
	months = (stop["year"] - start["year"]) * 12 + (stop["month"] - start["month"])
	# Open local database
	path = os.path.join("..","stocks.db")
	conn = sqlite3.connect(path)
	curs = conn.cursor()

	csv = ""
	#row_1 = []
	#row_2 = []
	#row_3 = []
	#row_4 = []
	#prev_tickers = None
	year 		= start["year"]
	month 		= start["month"]
	day 		= start["day"]
	sell_year 	= 0
	sell_month 	= info["months"] / 12
	sum_investment = 0
	sum_earnings = 0
	sum_earnings_investment = 0
	for idx in range(months):
		if month > 12:
			month = 1
			year += 1
		
		investment 		= 0
		investment_date = ""
		earnings 		= 0
		earning_date 	= ""

		anomaly_tickers = ""
		below_dollar 	= []
		above_dollar 	= []

		print("({0}\{1}) {2:.4g}%".format(idx,months,float(idx/months)*100.0))
		print("({0}\{1}) Buy stocks for date {2}-{3}-{4}".format(idx,months,year,month,day))
		stocks, tickers = GetClosestStocks(curs,[0,1],year,month,day)
		print("({0}\{1}) Total ammount of stocks ({2})".format(idx,months,len(stocks)))
		investment_date = "{0}_{1}_{2}".format(year,month,day)

		sell_year = info["years"]
		if (12 - month < info["months"] % 12):
			sell_year  = 1 + int(info["months"] / 12)
			sell_month = (month + (info["months"] % 12)) % 12
		else:
			#sell_year = int(info["months"] / 12)
			sell_month = month + info["months"]

		earning_date = "{0}_{1}_{2}".format((year+sell_year),sell_month,day)
		print("({0}\{1}) Sell stocks for date {2}-{3}-{4}".format(idx,months,(year+sell_year),sell_month,day))

		#if prev_tickers is not None:
		#	for item in prev_tickers:
		#		if item not in tickers:
		#			above_dollar.append(item)
		#	for item in tickers:
		#		if item not in prev_tickers:
		#			below_dollar.append(item)
		#prev_tickers = tickers.copy()

		#y, m, d = GetWorkingDate(year+1,month,day)
		#earning_date = "{0}_{1}_{2}".format(y,m,d)

		invested_tickers = []
		for stock in stocks[:]:
			if g_exit is True:
				return
			
			if stock["ticker"] not in invested_tickers: # Remove duplication
				invested_tickers.append(stock["ticker"])
				investment += stock["price"]
				# Get stock from future
				stock_e = GetClosestStock(curs,stock["ticker"],year+sell_year,sell_month,day)
				if stock_e is not None: # Stock exist
					# Append to earnings
					earnings += stock_e["price"]
					if stock_e["price"] > float(10 * stock["price"]):
						print("[WARRNING] Ticker: {0}, Investment: {1} Earning: {2}".format(stock["ticker"], stock["price"], stock_e["price"]))
						anomaly_tickers += "{0}|".format(stock["ticker"])
				else: # STock does not exist (migght be bankropsy)
					print("-{0}",format(stock["price"]))
					earnings -= stock["price"]
		sum_investment += investment
		sum_earnings += earnings
		sum_earnings_investment += earnings-investment
		print("({0}\{1}) Invested ({2}) Vs. Earned ({3})".format(idx,months,investment,earnings))
		csv += "{0},{1},{2},{3},{4},{5},{6},{7}\n".format(month, investment, earnings, earnings-investment,len(stocks), investment_date, earning_date, anomaly_tickers)
		
		#row_1.append(investment)
		#row_2.append(earnings)
		#row_3.append(above_dollar)
		#row_4.append(below_dollar)

		month += 1
	conn.close()

	path = os.path.join("output","investment_vs_earnnings.csv")
	csv += "{0},{1},{2},{3},{4}\n".format("", sum_investment, sum_earnings, sum_earnings_investment, ((sum_earnings - sum_investment) / sum_investment)*100)
	Save(path,csv)

def main():
	global g_exit
	signal.signal(signal.SIGINT, signal_handler)

	parser = argparse.ArgumentParser(description='Stock DB Creator')
	parser.add_argument('--query', action='store_true', help='Create queries from stocks resources')
	parser.add_argument('--create', action='store_true', help='Crerate DB from queries')
	parser.add_argument('--tickers', action='store', dest='tickers', help='Append tickers from tickers file')
	parser.add_argument('--info', action='store', dest='name', help='Read ticker history')
	args = parser.parse_args()

	if args.name is not None:
		data = GetMAX(args.name)
		for stock in data:
			print("{0}\t{1:.2f} \t {2:.2f}".format(stock["date"],stock["open"],stock["close"]))
	else:
		Run({
			"year": 2015,
			"month": 1,
			"day": 1
		},{
			"year": 2019,
			"month": 12,
			"day": 1
		}, 
		{
			"years": 0,
			"months": 3,
			"prices_range": [0,1]
		}, 1)

if __name__ == "__main__":
	main()

'''
select stocks_info.ticker, stocks_price.close from stocks_info
join stocks_price on 
stocks_info.ticker = stocks_price.ticker
where stocks_info.close < 1 and stocks_info.vol > 0 and stocks_price.date = "2000-11-22 00:00:00"
'''
