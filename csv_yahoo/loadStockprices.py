import requests
import json
import time
import sqlite3
import pandas as p
from datetime import datetime

# Create a calendar



#this script handles initial loads of the database for missing data.
#for the script that loads the database each day, please see  truthfullsloadC
#for the script that loads data from the command line please truthfullsloadCL

#as above this 
polykey = "Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk"
apiurl = ""


def GetPrices(url, dates, ticker):
    con = sqlite3.connect("SP500.db")
    cur = con.cursor()
    sqlCreateTAble = ""
    sqlheader = "insert into temp_price ([Ticker],[Date],[Open], [Close], [AdjClose], [Low], [High], [Volume]) Values"

    try:
        response = requests.get(url)
    except requests.ReadTimeout:
        print("Read timeout error. Retrying in 10 seconds...")
        GetPrices(url=url,dates = dates,ticker=ticker)
        return
    except requests.ConnectionError:
        print("Connection error. Retrying in 10 seconds...")
        GetPrices(url=url,dates = dates,ticker=ticker)
        return

    data = response.json()

    try:
        results = data["results"]
    except KeyError:
        print(f"No result set. URL = {url}")
        exit

    try:
        nxt = data["next_url"]
    except KeyError:
        nxt = None
        print("no nxt. this is the last page")
        pass
    
    try:
        count = data["count"]
    except KeyError:
        #if ticker has a "-" in the name, try it with a dot
        print( f"No count data for {ticker}")
        exit()

  
    i = 0
    sql = sqlheader
    for element in results:

        open = element["o"]
        close = element["c"]
        adjclose = close
        high = element["h"]
        low = element["l"]    
        volume = element["v"]
        

        values = f"('{ticker}','{dates[i]}',{open},{0.0},{adjclose},{low},{high},{volume}),"
        sql = sql + values
        i = i + 1
        if i == count:
            #remove the comma from the string
            sql = sql[:-1]
            cur.execute(sql)  
            con.commit()
            time.sleep(12)
            print("retrieved ", count, f" data from polygon for ticker {ticker}")
            if  nxt is None:
                print(f"Finished retreiving data for {ticker}")
                return
            else:          
                con.close()
                print("Nxt has value loading the nxt set of records. Nxt: ", nxt)
                nxt_url = nxt + "&apiKey=" + polykey
                dates_new = dates[i:]
                return GetPrices(nxt_url, dates_new, ticker)
            
def GetOldestPrice(ticker):
    con = sqlite3.connect("SP500.db")
    cur = con.cursor()
    sql = f"select max(p.[date]) from price p where p.[ticker] = '{ticker}' "
    
    max = cur.execute(sql).fetchone()

    return max[0]

def getDateArray(ticker): 
    year = datetime.today().year
    today = datetime.today().strftime('%Y-%m-%d')    
    start_date = GetOldestPrice(ticker)
    end_date = today
    holidays_2023 = ['2023-01-02', '2022-01-16', '2023-02-20','2023-04-07','2023-05-29', '2023-06-19','2023-07-04','2023-09-04','2023-11-23','2023-12-25']
    holidays_2024 = ['2024-01-01','2023-01-15','2023-02-19','2023-03-29','2023-05-27','2023-06-19','2023-07-04','2023-09-02','2023-11-28','2023-12-25']

    if year == 2023:
        holidays = holidays_2023
    elif year == 2024:
        holidays = holidays_2024 
    else:
     raise ValueError("We don't have data for that year. Please input the holiday list for your year")

    business_dates = p.bdate_range(start = start_date, end =end_date, holidays=holidays, freq='C')
    return business_dates.format(formatter=lambda x : x.strftime('%Y-%m-%d'))

#Get tickers with value
def getTickers() :
    con = sqlite3.connect("SP500.db")
    cur = con.cursor()
    sql = "select distinct p.ticker from price p where p.[adjclose] > 0.0"
    
    cur.execute(sql)
    count = 0
    tickers = []
    for row in cur:
        tickers.append( row[0])
        count = count + 1

    return tickers


def CreateTempTable():
    con = sqlite3.connect("SP500.db")
    cur = con.cursor()
    sql = "CREATE TABLE if not exists temp_price AS SELECT * FROM price WHERE 0"
    cur.execute(sql)
    con.commit()
    cur.close()
    con.close()

def MergeTables(ticker:str):
    con = sqlite3.connect("SP500.db")
    cur = con.cursor()
    sql = f"""
            insert into price 
            select * from temp_price t
            except
            select p.* from price p
            where p.ticker = '{ticker}'
         
        """
    cur.execute(sql)
    con.commit()
    sql = "delete from temp_price"
    cur.execute(sql)
    con.commit()

    cur.close()
    con.close()

CreateTempTable()
tickers = getTickers()
i = tickers.index("XLB")
i = 0
for t in tickers[i:]:
    dates = getDateArray(t)
    lastpricedate = GetOldestPrice(t)
    today = datetime.today().strftime('%Y-%m-%d')  
    url = f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/{lastpricedate}/{today}?adjusted=true&sort=asc&limit=120&apiKey=Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk"
    GetPrices(url,dates, t)
    MergeTables(t)
    time.sleep(12)

