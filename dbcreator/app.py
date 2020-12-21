#!/usr/bin/python
import os
import sys
import signal
import json
import yfinance as yf
import pandas as pd
import sqlite3
import argparse

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
	hist = ticker.history(period="1mo")
	for idx, row in hist.iterrows():
		print("{0}\t{1:.5g}\t{2:.4g}\t{3:.4g}\t{4:.4g}\t{5:.4g}".format(idx,row['Open'],row['Close'],row['High'],row['Low'],row['Volume']))

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

def main():
	global g_exit
	signal.signal(signal.SIGINT, signal_handler)

	parser = argparse.ArgumentParser(description='Stock DB Creator')
	parser.add_argument('--query', action='store_true', help='Create queries from stocks resources')
	parser.add_argument('--create', action='store_true', help='Crerate DB from queries')
	parser.add_argument('--tickers', action='store', dest='tickers', help='Append tickers from tickers file')
	args = parser.parse_args()

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
		path = os.path.join("resource","companies.csv")
		dfCompanies = pd.read_csv(path)
		for index, row in dfCompanies.iterrows():
			splitted = row[1].split(" ")
			if len(splitted) > 1:
				tickers.append(splitted[0])
			else:
				tickers.append(row[1])
	
	failed_tickers_list = []
	if args.query is True:
		for index, ticker in enumerate(tickers[:]):
			if g_exit is True:
				return
			# Check if companie exist
			query = "SELECT * FROM stocks_info WHERE `ticker` = '{0}'".format(ticker)
			curs.execute(query)
			rows = curs.fetchall()
			if len(rows) > 0:
				print("\tTicker {0} exist, will not be added to DB...".format(ticker))
			else:
				try:
					tk = yf.Ticker(ticker)
					data = GetMAX(tk)
				except Exception as e:
					print("Exception (YAHOO) {0}".format(e))
				
				print("({0}/{1}) Ticker Name: {2}, Size: {3}".format(index, len(tickers), ticker, data.shape[0]))
				if data.size > 0:
					count = 0
					try:
						for idx, row in data.iterrows():
							if g_exit is True:
								return
							if count == 0:
								first_row = data.iloc[0]
								last_row  = data.iloc[data.shape[0]-1]
								# query_stocks_info = "INSERT INTO stocks_info VALUES('{0}','{1}','{2}',{3},{4},{5},{6},{7});".format(idx,'',ticker,last_row['Volume'],last_row['Open'],last_row['Close'],last_row['Low'],last_row['High'])
								query_stocks_info = "INSERT OR REPLACE INTO stocks_info (`created_date`,`name`,`ticker`,`vol`,`open`,`close`,`low`,`high`) SELECT '{0}','{1}','{2}',{3},{4},{5},{6},{7} WHERE NOT EXISTS (SELECT 1 FROM stocks_info WHERE created_date = '{0}' AND ticker = '{2}');".format(idx,'',ticker,last_row['Volume'],last_row['Open'],last_row['Close'],last_row['Low'],last_row['High'])
								path = os.path.join("output","query_stocks_info")
								Append(path,"{0}\n".format(query_stocks_info))
							count += 1
							query_stocks_price = "INSERT INTO stocks_price VALUES ('{0}','{1}',{2},{3},{4},{5},{6});".format(idx,ticker,row['Volume'],row['Open'],row['Close'],row['Low'],row['High'])
							path = os.path.join("output","query_stocks_price")
							Append(path,"{0}\n".format(query_stocks_price))
					except Exception as e:
						print("Exception {0}\n{1}\n{2}".format(e,query_stocks_info,query_stocks_price))
				else:
					failed_tickers_list.append(ticker)
		print("Failed Tickers List:\n{0}".format(failed_tickers_list))

	if args.create is True:
		path = os.path.join("output","query_stocks_info")
		query_stocks_info_list = Load(path).split("\n")
		path = os.path.join("output","query_stocks_price")
		query_stocks_price_list = Load(path).split("\n")

		path = os.path.join("output","stocks.db")
		conn = sqlite3.connect(path)
		curs = conn.cursor()

		# Create table
		curs.execute('''CREATE TABLE IF NOT EXISTS stocks_price (date text, ticker text, vol int, open real, close real, low real, high real)''')
		curs.execute('''CREATE TABLE IF NOT EXISTS stocks_info (created_date text, name text, ticker text, vol int, open real, close real, low real, high real)''')

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
		conn.close()

if __name__ == "__main__":
	main()

'''
Failed Tickers List:
['MOWI', 'HNSA', '9437', '639', '241', '3481', '1882', '2502', 'APOPW', '2269', 'MTGB', 'EUCAR', 'PSON', 'PRIME', 'AXFO', 'KNEBV', 
'ALO', 'ABBN', '2587', '9437', '1211', '3141', 'HLMA', 'CSH-U', '3092', '797', 'HNR1', '981', 'WINE', '1337', '3938', 'ORSTED', '2208', 
'SBER', '2317', 'NDX1', '8150', 'BEIJB', '2181', 'MBTN', 'N', '728', '3800', '8804', '1896', '3988', '7269', '4751', '2318', 'SSE', 
'6013', 'VLTSA', '12', 'MOWI', 'NWH-U', '9202', 'KARN', '6981', 'SWMA', '1119', '9766', 'G1A', '2005', 'TOTS3', 'GYM', '3903', '9143', 
'2121', 'NOVOB', 'ATE', '9926', '2689', '696', 'FSC1V', '32', '2500', 'DHER', 'BVIC', '857', '991', '6758', '1810', '3769', 'AST', 'UTDI', 
'3798', '6370', '7532', 'BNZL', '2371', '5020', 'MCPHY', 'A2A', 'ENEL', 'BFSA', '7937', 'KNEBV', '2020', '3632', 'FAGR', '4689', '3328', 
'2342', 'NPI', 'SES', 'INDT', '3968', 'BKL', 'NXT', '3391', 'OGZD', 'PHTM', 'HARV', '291', '1288', '302', 'S92', '1833', 'GETIB', 'NIBEB', 
'888', '489', 'NESN', 'PHIA', '6501', 'AT1', '8252', 'STMN', '1478', '914', '9505', '6861', 'LOOMIS', 'MWE', 'CIEL3', '9830', 'SY1', '5108', 
'3932', 'ENOG', '9020', 'NETW', '7747', '914', '7649', 'LTOD', 'TEMN', 'HWDN', 'ANN', '7741', 'DHER', 'PHIA', 'TW/', '1910', 'BATS', '1066', 
'JMT', 'LYNN', '285', '2326', 'NEOEN', '6473', 'S30', '2357', '1398', '777', '874', '1347', '4755', '867', '1044', 'MMEN', '6807', 'NESN', 
'175', 'ERICB', 'ADYEN', '4704', '1COV', '941', '7844', '6869', '1605', 'SIX2', '4543', 'TTST', '3662', '6837', 'SN/', 'LONN', '939', '386', 
'MONC', 'SMU-U', 'EDPR', '1877', '8022', 'EOAN', 'IRE', 'PARD3', '6502', 'ENOG', 'THULE', 'TEMN', '3988', 'PBR/A', 'SAIS', 'SIX2', 'DB1', 
'B4B', '7974', 'TEMN', '8967', 'MOVE', '1928', '8570', 'SWMA', '6770', '6055', '6674', 'INDV', '3281', '6383', 'LSG', '1177', 'PNDORA', 
'2318', 'STORYB', '6185', '1', 'DWNI', 'TRMR', 'LOOMIS', 'COLR', 'NOBINA', '3690', '6865', '9433', 'SGRE', 'ORNBV', 'RIGD', '1336', 'SAABB', 
'WIZZ', '1211', '2979', '9551', '7974', 'RDSA', 'EMBRACB', 'BDEV', '1719', '257', 'PCELL', 'HHPD', '3659', '5110', '9955', 'ALPHA', '6856', 
'9201', '2319', '3765', '1793', 'HEN3', '4507', 'AV/', '4385', 'A2A', '576', 'LTOD', 'JCP', '7250', 'PAH3', 'GRT-U', 'FXPO', '6460', 'MHID', 
'G24', 'SIKA', '113', '7733', 'SRT3', '939', 'DIR-U', '7974', 'WEED', 'SBID', '1878', 'OGZD', 'GYM', '3911', 'TECN', '3382', '2432', '7267', 
'O2D', '6954', '9519', '2013', 'MONY', '6750', '8984', '3888', '1530', '4555', '3466', 'LLOY', 'ASRNL', 'KSP', '6103', '6856', '9697', 'SMDS', 
'HHPD', 'ENGI', 'HSBA', '6504', '998', 'ALERS', 'GBG', 'ABI', 'LKOD', '7832', '4204', 'SMWH', '1089', 'TENERGY', '2413', '6594', 'AZUL4', 'ELIS', 
'AGRA', 'MUV2', '3635', '6703', '241', '3471', 'AALB', 'LKOD', 'ATE', '6723', 'NOVOB', '8316', '3493', '2382', 'MTY', '1169', 'DWNI', 'ABBN', 
'5711', 'VRLA', '6902', 'NESN', '1896', 'PLAZ', 'ATA', '522', '4902', 'SUY1V', 'AGN', 'RRTL', '939', '586', 'SGRE', 'NESN', 'ATCOA', 'VOW3', 
'AVON', '6134', '3692', '6869', 'RIGD', '371', '2628', 'IMCD', 'SMTP', '992', 'TGOD', '853', '6503', 'MB', 'KHRN', 'BT/A', '4549', '6702', 'SBRY', 
'SEV', '2018', 'PODA', '3668', 'HEIA', '9684', 'TGYM', 'HOLMB', '9697', 'HNR1', '2020', 'VLNS', '303', 'CBDT', '9793', '968', 'DRW3', '1789', 
'HEN3', 'ROVIO', '883', 'DEMANT', '2331', '354', '799', '2186', 'JBH', 'NXT', 'SY1', 'OCDO', '7575', '1398', '2333', '6368', '8591', '1044', 
'268', 'BAS', '7164', 'PDX', '2670', '6952', 'MGGT', '6845', '3291', '9104', 'HPHT', 'FPE3', '3988', '7779', 'AF', 'JMAT', 'ALESK', '3249', 
'DWNI', 'NEXI', '763', 'BC8', 'RB-', '1377', '9989', '751', 'MNDI', '2319', 'VOW3', '6728', '1066', 'SIGHT', 'BILIA', 'ABBN', '9984', 'COH', 
'KOG', '9843', 'LGEN', '1299', '2175', '6701', '2331', '1169', '1317', '3656', 'TNE', 'SGRE', '6098', '5938', '3349', '2433', 'SMLS3', '2318', 
'YDUQ3', 'ENGI', 'CLIN', 'EXPN', 'UN01', 'KGN', '9735', 'NRTH', '1302', 'WAYL', 'HLAG', 'PHIA', '2269', 'PLTH', '1398', 'RDSA', '2768', '4565', 
'6370', '916', '836', '7201', '1800', '1339', 'SIKA', 'EKTAB', '1808', 'INF', 'DPLM', 'VAR1', 'PROTCT', 'LIFCOB', '4587', 'AMBUB', '3064', 'GWI', 
'COLOB', '6762', '1801', 'CIE']
'''