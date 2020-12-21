#!/usr/bin/python
import os
import sys
import signal
import json
import yfinance as yf
import pandas as pd

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

def signal_handler(signal, frame):
	print("Accepted signal from other app")

def main():
	signal.signal(signal.SIGINT, signal_handler)

	tk = yf.Ticker("TSLA")
	#print(msft.info)
	hist = tk.history(period="5d")

	Get5D(tk)
	
	print(tk.info["longBusinessSummary"])
	#print(tk.actions)
	#print(tk.dividends)

	#print(tk.quarterly_financials)
	#print(tk.quarterly_balance_sheet)
	#print(tk.quarterly_cashflow)
	#print(tk.calendar)

	'''
	tk.info
	tk.actions
	tk.dividends
	tk.splits
	tk.financials
	tk.quarterly_financials
	tk.major_holders
	tk.institutional_holders
	tk.balance_sheet
	tk.quarterly_balance_sheet
	tk.cashflow
	tk.quarterly_cashflow
	tk.earnings
	tk.quarterly_earnings
	tk.sustainability
	tk.recommendations
	tk.calendar
	tk.isin # ISIN = International Securities Identification Number
	tk.options
	opt = tk.option_chain('YYYY-MM-DD') # data available via: opt.calls, opt.puts
	
	'''

	#json_hist_raw = hist.to_json(orient="split")
	#json_hist = json.loads(json_hist_raw)
	#print(json_hist)
	

	#print(hist.index)
	#print(len(hist.index.values)) # .data is not a public property, and is fully deprecated
	
	#data = yf.download("TSLA", start="2020-12-01", end="2020-12-03", group_by="ticker")
	#print(data)
	#print(data['Close'])

	#print(type(hist))
	#print(hist)
	#print(hist["Close"])

if __name__ == "__main__":
	main()