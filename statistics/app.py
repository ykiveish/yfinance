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

def GetStock(curs, ticker, year, month, day):
	if month < 10:
		month = '0{0}'.format(month)
	if day < 10:
		day = '0{0}'.format(day)
	query = "select ticker, close from stocks_price where ticker = '{ticker}' and date = '{year}-{month}-{day} 00:00:00'".format(ticker=ticker,year=year,month=month,day=day)
	print(query)
	curs.execute(query)
	rows = curs.fetchall()
	if len(rows) > 0:
		return {
				"ticker": rows[0][0],
				"price": rows[0][1]
		}
	return None

def GetDay(year, month, day):
	query = "{year}-{month}-{day}".format(year=year,month=month,day=day)
	date_time_obj = datetime.datetime.strptime(query, '%Y-%m-%d')
	return (date_time_obj.weekday())

def GetWorkingDate(year, month, day):
	weekday = GetDay(year, month, day)
	#print(weekday)
	if weekday == 5:
		day += 2
	elif weekday == 6:
		day += 1
	
	return year, month, day

def Run(start, stop, sell_interval):
	global g_exit
	months = (stop["year"] - start["year"]) * 12 + (stop["month"] - start["month"])

	path = os.path.join("..","stocks.db")
	conn = sqlite3.connect(path)
	curs = conn.cursor()

	csv = ""
	#row_1 = []
	#row_2 = []
	#row_3 = []
	#row_4 = []
	#prev_tickers = None
	year 	= start["year"]
	month 	= start["month"]
	day 	= start["day"]
	for idx in range(months):
		if month > 12:
			month = 1
			year += 1
		
		investment 	= 0
		earnings 	= 0
		investment_date = ""
		earning_date = ""
		anomaly_tickers = ""
		below_dollar = []
		above_dollar = []

		print("({0}\{1}) {2:.4g}%".format(idx,months,float(idx/months)*100.0))

		y, m, d = GetWorkingDate(year,month,day)
		stocks, tickers = GetStocks(curs,1,y, m, d)
		investment_date = "{0}_{1}_{2}".format(y,m,d)

		#if prev_tickers is not None:
		#	for item in prev_tickers:
		#		if item not in tickers:
		#			above_dollar.append(item)
		#	for item in tickers:
		#		if item not in prev_tickers:
		#			below_dollar.append(item)
		#prev_tickers = tickers.copy()

		y, m, d = GetWorkingDate(year+1,month,day)
		earning_date = "{0}_{1}_{2}".format(y,m,d)
		invested_tickers = []
		for stock in stocks[:]:
			if g_exit is True:
				return
			
			if stock["ticker"] not in invested_tickers: # Remove duplication
				if stock["price"] > 0: # Stock cannot be negative
					invested_tickers.append(stock["ticker"])
					investment += stock["price"]
					# Get stock from future
					stock_e = GetStock(curs,stock["ticker"],y, m, d)
					if stock_e is not None: # Stock exist
						# Append to earnings
						print("+{0}",format(stock_e["price"]))
						earnings += stock_e["price"]
						if stock_e["price"] > float(10 * stock["price"]):
							print("[WARRNING] Ticker: {0}, Investment: {1} Earning: {2}".format(stock["ticker"], stock["price"], stock_e["price"]))
							anomaly_tickers += "{0}|".format(stock["ticker"])
					else: # STock does not exist (migght be bankropsy)
						print("-{0}",format(stock["price"]))
						earnings -= stock["price"]
				else:
					print("[ERROR] Stock price is NEGATIVE - Ticker: {0}, Price: {1}".format(stock["ticker"], stock["price"]))
			
				investment += stock["price"]
		csv += "{0},{1},{2},{3},{4},{5}\n".format(idx, investment, earnings, investment_date, earning_date, anomaly_tickers)
		
		#row_1.append(investment)
		#row_2.append(earnings)
		#row_3.append(above_dollar)
		#row_4.append(below_dollar)

		month += 1
	conn.close()

	path = os.path.join("output","investment_vs_earnnings.csv")
	Save(path,csv)

def main():
	global g_exit
	signal.signal(signal.SIGINT, signal_handler)

	parser = argparse.ArgumentParser(description='Stock DB Creator')
	parser.add_argument('--query', action='store_true', help='Create queries from stocks resources')
	parser.add_argument('--create', action='store_true', help='Crerate DB from queries')
	parser.add_argument('--tickers', action='store', dest='tickers', help='Append tickers from tickers file')
	args = parser.parse_args()

	Run({
		"year": 2011,
		"month": 1,
		"day": 1
	},{
		"year": 2011,
		"month": 3,
		"day": 1
	}, 1)

	'''
	path = os.path.join("..","stocks.db")
	conn = sqlite3.connect(path)
	curs = conn.cursor()

	year, month, day = GetWorkingDate(2008,5,13)
	stocks = GetStocks(curs,1,year, month, day)
		
	investment = 0
	earnings = 0
	year, month, day = GetWorkingDate(2008,9,13)
	for stock in stocks:
		investment += stock["price"]
		stock_e = GetStock(curs,stock["ticker"],year, month, day)
		if stock_e is not None:
				earnings += stock_e["price"]
		else:
			earnings -= stock["price"]
		print(investment,earnings)

	#query = "SELECT * FROM stocks_info WHERE `ticker` = '{0}'".format(ticker)
	#curs.execute(query)
	#rows = curs.fetchall()
	#if len(rows) > 0:
	#	pass

	conn.close()
	'''

if __name__ == "__main__":
	main()

'''
select stocks_info.ticker, stocks_price.close from stocks_info
join stocks_price on 
stocks_info.ticker = stocks_price.ticker
where stocks_info.close < 1 and stocks_info.vol > 0 and stocks_price.date = "2000-11-22 00:00:00"
'''
