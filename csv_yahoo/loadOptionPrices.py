import requests
import sqlite3
import pandas as p
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import requests
import re
import os
import concurrent.futures

import math as m

polykey = "Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk"
def getWeeklyOptionExpirationDates():
    holidays_2015 = ['2015-01-01','2015-01-19','2015-04-03','2015-05-25','2015-07-03','2015-09-07','2015-10-12','2015-11-11','2015-11-26','2015-12-25']
    holidays_2016 = ['2016-01-01','2016-01-18','2016-03-25','2016-05-30','2016-07-04','2016-09-05','2016-10-10','2016-11-11','2016-11-24','2016-12-26']
    holidays_2017 = ['2017-01-02','2017-01-16','2017-02-20','2017-04-14','2017-05-29','2017-07-04','2017-09-04','2017-10-09','2017-11-23','2017-12-25']
    holidays_2018 = ['2018-01-01','2018-01-15','2018-02-19','2018-03-30','2018-05-28','2018-07-04','2018-09-03','2018-10-08','2018-11-12','2018-11-22','2018-12-25']
    holidays_2019 = ['2019-01-01','2019-01-21','2019-02-18','2019-04-19','2019-05-27','2019-07-04','2019-09-02','2019-10-14','2019-11-11','2019-11-28','2019-12-25']
    holidays_2020 = ['2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25']
    holidays_2021 = ['2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-12-24']
    holidays_2022 = ['2022-01-17','2022-02-21','2022-04-15','2022-05-30','2022-06-20','2022-07-04','2022-09-05','2022-11-24','2022-12-26']
    holidays_2023 = ['2023-01-02','2023-01-16','2023-02-20','2023-04-07','2023-05-29','2023-06-19','2023-07-04','2023-09-04','2023-11-23','2023-12-25']
    holidays_2024 = ['2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27','2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25']
    holidays_2025 = ['2025-01-01','2025-01-20','2025-02-17','2025-04-18','2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25']

    holidays = []
    holidays.extend(holidays_2015)
    holidays.extend(holidays_2016)
    holidays.extend(holidays_2017)
    holidays.extend(holidays_2018)
    holidays.extend(holidays_2019)
    holidays.extend(holidays_2020)
    holidays.extend(holidays_2021)
    holidays.extend(holidays_2022)
    holidays.extend(holidays_2023)
    holidays.extend(holidays_2024)
    holidays.extend(holidays_2025)

    today = datetime.today().strftime('%Y-%m-%d')    

    business_dates = p.bdate_range(start = '2020-01-01', end ='2025-12-31')
    MarketTradingDays = [x for x in business_dates if x._date_repr not in holidays]
    FridayHolidays = [x for x in business_dates if x.day_of_week == 4 and x._date_repr in holidays]
    thursdayexpirations = [business_dates[idx -1] for idx, x in enumerate(business_dates) if x in FridayHolidays]
    fridayExpirations = [x for x in MarketTradingDays if x.day_of_week == 4]

    TotalEOWExpirations = thursdayexpirations + fridayExpirations
    TotalEOWExpirations.sort()
    
    return TotalEOWExpirations

#current range of function is between 2020(start) to the end of 2025
def getMarketTradingDays(datebegin:str, dateend:str) ->list[p.Timestamp]:
    holidays_2015 = ['2015-01-01','2015-01-19','2015-04-03','2015-05-25','2015-07-03','2015-09-07','2015-10-12','2015-11-11','2015-11-26','2015-12-25']
    holidays_2016 = ['2016-01-01','2016-01-18','2016-03-25','2016-05-30','2016-07-04','2016-09-05','2016-10-10','2016-11-11','2016-11-24','2016-12-26']
    holidays_2017 = ['2017-01-02','2017-01-16','2017-02-20','2017-04-14','2017-05-29','2017-07-04','2017-09-04','2017-10-09','2017-11-23','2017-12-25']
    holidays_2018 = ['2018-01-01','2018-01-15','2018-02-19','2018-03-30','2018-05-28','2018-07-04','2018-09-03','2018-10-08','2018-11-12','2018-11-22','2018-12-25']
    holidays_2019 = ['2019-01-01','2019-01-21','2019-02-18','2019-04-19','2019-05-27','2019-07-04','2019-09-02','2019-10-14','2019-11-11','2019-11-28','2019-12-25']
    holidays_2020 = ['2020-01-01','2020-01-20','2020-02-17','2020-04-10','2020-05-25','2020-07-03','2020-09-07','2020-11-26','2020-12-25']
    holidays_2021 = ['2021-01-01','2021-01-18','2021-02-15','2021-04-02','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-12-24']
    holidays_2022 = ['2022-01-17','2022-02-21','2022-04-15','2022-05-30','2022-06-20','2022-07-04','2022-09-05','2022-11-24','2022-12-26']
    holidays_2023 = ['2023-01-02','2023-01-16','2023-02-20','2023-04-07','2023-05-29','2023-06-19','2023-07-04','2023-09-04','2023-11-23','2023-12-25']
    holidays_2024 = ['2024-01-01','2024-01-15','2024-02-19','2024-03-29','2024-05-27','2024-06-19','2024-07-04','2024-09-02','2024-11-28','2024-12-25']
    holidays_2025 = ['2025-01-01','2025-01-20','2025-02-17','2025-04-18','2025-05-26','2025-06-19','2025-07-04','2025-09-01','2025-11-27','2025-12-25']

    holidays = []
    holidays.extend(holidays_2015)
    holidays.extend(holidays_2016)
    holidays.extend(holidays_2017)
    holidays.extend(holidays_2018)
    holidays.extend(holidays_2019)
    holidays.extend(holidays_2020)
    holidays.extend(holidays_2021)
    holidays.extend(holidays_2022)
    holidays.extend(holidays_2023)
    holidays.extend(holidays_2024)
    holidays.extend(holidays_2025)

    business_dates = p.bdate_range(start = datebegin, end =dateend, holidays=holidays, freq='C')
    businessDatesList = [d for d in business_dates]
    return businessDatesList

def insertFinancialDates(datebegin:str, dateend:str):
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    dates = getMarketTradingDays(datebegin=datebegin, dateend=dateend)
    sqlstart= f"insert or ignore into financialDate([date]) values"
    for d in [d.strftime('%Y-%m-%d') for d in dates]:
        cur_option.execute(sqlstart + f"('{d}')");

    con_option.commit()
    cur_option.close()
    con_option.close()

def getFirstBusinessDayDates(datestart, dateend):
    dates = p.date_range(start = datestart, end = dateend)
    
    AllFirstDatesOfTheMonth = [d for d in dates if d.is_month_start]
    
    FirstDayOfTheMonthOnBusinessDay = [d for d in AllFirstDatesOfTheMonth if d.day_of_week in [0,1,2,3,4]]
    FirstDayOfBusinessMonthWhenSaturdayIsFirstDayOfTheMonth = [dates[idx + 2] for idx, d in enumerate(dates) if d in AllFirstDatesOfTheMonth and d.day_of_week == 5]
    FirstDayOfBusinessMonthWhenSundayIsFirstDayOfTheMonth = [dates[idx + 1] for idx, d in enumerate(dates) if d in AllFirstDatesOfTheMonth and d.day_of_week == 6]
    
    FirstBusinessdaysOfTheMonth = FirstDayOfTheMonthOnBusinessDay + FirstDayOfBusinessMonthWhenSaturdayIsFirstDayOfTheMonth + FirstDayOfBusinessMonthWhenSundayIsFirstDayOfTheMonth
    FirstBusinessdaysOfTheMonth.sort()
    return FirstBusinessdaysOfTheMonth

def getMonthlyOptionExpirationDates(datestart, dateend):
    
    date_start = datestart
    date_end = dateend
    dates = p.date_range(start = date_start, end = date_end)
    FirstbusinessDatesOfTheMonth = getFirstBusinessDayDates(date_start, date_end)

    MonthlyOptionExpirationDates = [getExpirationdayLambda(dates[idx + 14]) for idx, d in enumerate(dates) if d in FirstbusinessDatesOfTheMonth]

    return MonthlyOptionExpirationDates


def getExpirationdayLambda(timestamp: p.Timestamp) -> p.Timestamp:
    #this function should get the closest friday. If monday, that is 4 days away, if tuesday, 3 days... Friday 0 days
    #0 = monday, 1 = tuesday, 2 = wednesday, 3 = thursday, 4 = friday 
    
    if timestamp.day_of_week == 0:
        expirationDate = p.Timestamp(year = timestamp.year, month=timestamp.month, day =timestamp.day + 4)
    elif timestamp.day_of_week == 1:
        expirationDate = p.Timestamp(year = timestamp.year, month=timestamp.month, day =timestamp.day + 3)
    elif timestamp.day_of_week == 2:
        expirationDate = p.Timestamp(year = timestamp.year, month=timestamp.month, day =timestamp.day + 2)
    elif timestamp.day_of_week == 3:
        expirationDate = p.Timestamp(year = timestamp.year, month=timestamp.month, day =timestamp.day + 1)
    elif timestamp.day_of_week == 4:
        expirationDate = timestamp

    return expirationDate


def getATMStrike(price):
    if price > 5 and price <= 25:
        atmD = m.floor(price / 2.5) * 2.5
        atmU = m.ceil(price / 2.5) * 2.5

    if price > 25 and price <= 200:
        atmD = m.floor(price / 5.0) * 5
        atmU = m.ceil(price / 5.0) * 5

    if price > 200:
        atmD = m.floor(price / 10.0) * 10
        atmU = m.ceil(price / 10.0) * 10

    if (atmU - price) > (price - atmD):
        return atmD
    else:
        return atmU

def createPriceCode(price):
    pricestring = '{:.3f}'.format(round(price,3))
    if price /1.00 < 1.0:
        pricestring = f"00000" + pricestring
    elif price /10.00 < 1.0:
        pricestring = f"0000" + pricestring
    elif price /100.00 < 1.0:
        pricestring = f"000" + pricestring
    elif price / 1000.0 < 1.0:
        pricestring = f"00" + pricestring
    elif price / 10000.0 < 1.0:
        pricestring = f"0" + pricestring
    elif price / 100000.0 < 1.0:
        pricestring = pricestring
    else:
        raise "price not supported"
    return pricestring.replace(".","")
 
def getTickers() :
    con_stock = sqlite3.connect("SP500.db")
    cur_stock = con_stock.cursor()
    sql = "select distinct p.ticker from price p where p.[adjclose] > 0.0"
    
    cur_stock.execute(sql)
    count = 0
    tickers = []
    for row in cur_stock:
        tickers.append( row[0])
        count = count + 1

    cur_stock.close()
    con_stock.close()
    return tickers 

def getLastPrice(ticker:str, date:str):
    con_stock = sqlite3.connect("SP500.db")
    cur_stock = con_stock.cursor()
    sql = f"select adjclose from price where ticker = '{ticker}' and [date] = '{date}' order by date limit 1"
    cur_stock.execute(sql)
    rows = cur_stock.fetchone()

    cur_stock.close()
    con_stock.close()

    if rows is None:
        return None
    else:
        return rows[0]
    
def getPriceArray(ticker:str, datebegin:str, dateend:str) ->list:
    con_stock = sqlite3.connect("SP500.db")
    cur_stock = con_stock.cursor()
    sql = f"select date, adjclose from price where ticker = '{ticker}' and [date] >= '{datebegin}' and [date] <= '{dateend}' order by date"
    cur_stock.execute(sql)
    rows = cur_stock.fetchall()
    
    cur_stock.close()
    con_stock.close()
    if rows is None:
        return None
    else:
        return rows
    

#Inputs - Dateprices (price history of a stock.)
#       - ticker (symbol for the stock)
#       - datebegin, dateend (range of the stock prices)
#This function will get the optioncodes for atm strike of options and some other calculated strikes above 
def CreateOptionCodes(ticker:str, optiontype:str, datebegin:str, dateend:str, DatePrices:list)->set:
    
    MonthlyExpirationdatebegin = '2018-01-01'
    MonthlyExpirationdateend = '2024-12-31'
    MonthlyExpirations = getMonthlyOptionExpirationDates(MonthlyExpirationdatebegin, MonthlyExpirationdateend)

    allstrikes = getStrikes()
    OptionCodes:set = set()
    for dateprice in DatePrices:  
        #get the ATM strike for the stock price value for the trading day. 
        # Calculate the optioncode using this atm strike for the next 12 months
        #get the surrounding option codes within 10 strikes of the atm strike
        ATMstrike = getATMStrike(dateprice[1])
        upperstrikes = [strike for strike in allstrikes if strike > ATMstrike][0:10]
        lowerstrikes = [strike for strike in allstrikes if strike < ATMstrike][-10:]
        strikes = lowerstrikes + [ATMstrike] + upperstrikes

        expirations = [expiration for expiration in MonthlyExpirations if expiration > p.Timestamp(dateprice[0]) ][0:12]
        expirationCodes = [f"{e.year % 100}{e.month if e.month >= 10 else f'0{e.month}'}{e.day}" for e in expirations]

        for strike in strikes:
            optioncodes = [f"{ticker}{ec}{optiontype}{createPriceCode(strike)}" for ec in expirationCodes]
            OptionCodes.update(set(optioncodes))
        
    return OptionCodes
        

def getLastPriceAndDate(ticker:str):
    con_stock = sqlite3.connect("SP500.db")
    cur_stock = con_stock.cursor()
    sql = f"select date, adjclose, [date] from price where ticker = '{ticker}' order by date limit 1"
    cur_stock.execute(sql)
    rows = cur_stock.fetchone()

    cur_stock.close()
    con_stock.close()
 
    return rows
  

def getStrikes():

    band1 = range(0,20, 1)
    band2 = range(20, 100, 2)
    band3 = range(100, 500, 5)
    band4 = range(500, 1000, 10)
    band5 = range(1000, 5000, 50)

    bands = list(band1) + list(band2) + list(band3) + list(band4) + list(band5)

    return bands

#load option prices for between startdate and date of the maturity option specified in the
def loadoptionPrices(optionCode:str, url, maturity:str, duration:str):
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    try:
        response = requests.get(url)
    except requests.ConnectionError:
        cur_option.close()
        con_option.close()
        print("Connection error. Retrying in 10 seconds...")
        time.sleep(12)
        loadoptionPrices(optionCode=optionCode, url=url, maturity=maturity,duration = duration)
        return
    except requests.ReadTimeout:
        cur_option.close()
        con_option.close()
        print("Timeout error. Waiting 10 Seconds and retrying...")
        time.sleep(12)
        loadoptionPrices(optionCode=optionCode, url=url, maturity=maturity,duration= duration)
        return

    data = response.json()
    try:
        results = data["results"]
    except KeyError:
        #print(f"No data for {optionCode}")
        return None
    try:
        nxt = data['next_url']
    except KeyError:
        nxt = None   
        pass
    
    resultcount = len(results)
    if resultcount == 0:
        #print(f"no results for {url}")
        return None
    
    #if the date we run this application is past the maturity date and our result count is the same in the database set the sequenece to complete
    now_ts = p.Timestamp(datetime.now().strftime('%Y-%m-%d'))
    maturity_ts = p.Timestamp(maturity)

    if now_ts > maturity_ts:
        
        if resultcount == duration:
            cur_option.execute(f"update SequenceComplete set isComplete = 1 where Code = '{optionCode}'")
            con_option.commit()
            return False
    
    if resultcount == duration:
        print(f"{optionCode} is already fully updated")
        return None
    
    pricedata = results
    sql = "insert into temp_price(Code,MaturityDate,Duration,Open,AdjClose,High,Low,Volume, VWAP) Values"
    idx = duration
    for result in results:
        idx = idx + 1
        sql = sql + f"('{optionCode}','{maturity}',{idx},{result['o']},{result['c']},{result['h']},{result['l']},{result['v']},{result['vw']}),"

    sql = sql[:-1]
    #insert ticker symbol first 
    cur_option.execute(f"insert or ignore into Option(code) values('{optionCode}')")

    con_option.commit()

    try:
        cur_option.execute(sql)
    except sqlite3.IntegrityError as er:
        print(f"Integrity Error on inserting option prices into price table ")
        cur_option.close()
        con_option.close()
        return
        
    con_option.commit()
    cur_option.close()
    con_option.close()
    #insert 
    #insertDateData(optionCode, newlastestDate)
    
    print(f"loaded data for optioncode{optionCode} loaded {idx} values")
    if nxt is not None:
        print("Waiting 12 seconds because I got a free account :(")
        nxt_url = nxt + "&apiKey=" + polykey
        print(f"Loading nxt set of data. URL = ")
        loadoptionPrices(optionCode=optionCode, url = nxt_url, maturity = maturity,duration = duration)
    return True

def insertDateData(optioncode:str, lastdate:str):
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()

    sql = f"""with maxdur as (
            select max(p.duration) as maxdur from temp_price p
            where code = '{optioncode}'
            limit 1
            ),
            orderedDates as(
                select row_number() over (order by fd.date desc) as rowid, fd.[date] 
                from financialDate fd
                where fd.date <= '{lastdate}'
            )
            update temp_price 
            set date = data.tradingDate
            from(
                select od.date as tradingDate, od.rowid as rowid, (select md.maxdur from maxdur md) as maxd
                from temp_price p
                inner join orderedDates od
                on(od.rowid = maxd - p.Duration + 1 )
                where code = '{optioncode}'
            ) data
            where duration = data.maxd - data.rowid + 1
            and code = '{optioncode}' and date is null
            """
    try:
        cur_option.execute(sql)
    except:
        raise f"Error inserting date data into function for option - {optioncode}"
    finally:
        con_option.close()
        cur_option.close()
 

def getLastOptionPriceDate(code:str)  :
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    tickers = []
    sql = f"""
            select max(p.[date]) as lastdate
            from price p 
            where code = '{code}'
            """ 
    cur_option.execute(sql)
    rows = cur_option.fetchone()

    cur_option.close()
    con_option.close()

    if rows[0]  == None:
        return None
    else:
        return  rows[0]

def getLastOptionPriceDuration(code:str)  :
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    tickers = []
    sql = f"""
            select max(p.[duration]) as lastdate
            from price p 
            where code = '{code}'
            """ 
    cur_option.execute(sql)
    rows = cur_option.fetchone()

    cur_option.close()
    con_option.close()

    if rows[0]  == None:
        return None
    else:
        return  rows[0]
    
#option codes  that need to be updated
def getIncompleteOptionsContracts(ticker:str,optiontype:str) ->list :
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    sql = f"""
            select p2.code
            from SequenceComplete p2
            where p2.isComplete = 1 and p2.code glob '{ticker}[0-9]*{optiontype}*'
            """
    
    cur_option.execute(sql)
    rows = cur_option.fetchall()

    cur_option.close()
    con_option.close()

    if rows is None:
        return None
    return [r[0] for r in rows]

def getCompleteOptionContracts(ticker:str,optiontype:str) ->list:
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()

    sql = f"""
           select p2.code
            from SequenceComplete p2
            where p2.isComplete = 1 and p2.code glob '{ticker}[0-9]*{optiontype}*'
            """

    cur_option.execute(sql)
    rows = cur_option.fetchall()
    
    cur_option.close()
    con_option.close()
   
    if rows is None:
        return None
    return [r[0] for r in rows]
 
def stockpricerange(ticker:str,datebegin:str, dateend:str):
    con_stock = sqlite3.connect("SP500.db")
    cur_stock = con_stock.cursor()
    sql = f"""
            select 
                p.ticker
                ,min(p.adjclose) as smallestprice
                ,max(p.adjclose) as highestprice
            from price p
            WHERE p.[date] > '{datebegin}' and p.[date] < DATE('NOW') and p.[ticker] = '{ticker}'
            group by p.[ticker]
            having max(p.adjclose) > 2.0 * min(p.AdjClose) 
    """
    cur_stock.execute(sql)
    rows = cur_stock.fetchone()

    if rows is None:
        return None
    return rows


#to get option code form polygon io we have to reformat the code for the split price.
def GetSplitMultiplier(ticker:str, pricedate) -> float:

    if ticker == "AMZN":
        if pricedate < p.Timestamp("2022-06-06"):
            return 20.0
    if ticker == "AAPL":      
        if pricedate < p.Timestamp('1987-06-16'):
            return 224.0
        if pricedate < p.Timestamp('2000-06-21'):
            return 112.0
        if pricedate < p.Timestamp('2005-02-28'):
            return 56.0
        if pricedate < p.Timestamp('2014-06-09'):
            return 28.0
        if pricedate < p.Timestamp('2020-08-31'):
            return 4.0
    
    if ticker == "GOOG" or ticker == "GOOGL":
        if pricedate < p.Timestamp("2022-07-18"):
            return 20.0
    if ticker == "TSLA":
        if pricedate < p.Timestamp('2020-08-31'):
            return 15.0
        if pricedate < p.Timestamp('2022-08-25'):
            return 3.0
    
    return 1.0


def ExtractMaturityDate(optioncode:str):
    regex = re.findall('[A-Z]*[0-9]{6}',optioncode)
    regex = re.findall('[0-9]{6}',regex[0])

    return f"20{regex[0][0:2]}-{regex[0][2:4]}-{regex[0][4:6]}"

def CreateTempTable():
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()
    sql = "CREATE TABLE if not exists temp_price AS SELECT * FROM price WHERE 0"
    cur_option.execute(sql)
    con_option.commit()
    cur_option.execute("delete from temp_price")

    cur_option.close()
    con_option.close()

#insert into main table and avoid duplicates
def MergeTables(ticker:str, optiontype:str):
    con_option = sqlite3.connect("SP500O.db")
    cur_option = con_option.cursor()

    sql = f"""
            insert into price 
            select * from temp_price t
            except
            select p.* from price p
            where p.code glob '{ticker}[0-9]*{optiontype}*'
         
        """
    cur_option.execute(sql)
    con_option.commit()
    sql = "delete from temp_price"
    cur_option.execute(sql)
    con_option.commit()
    con_option.close
    

def LoadMonthlyOptionPrices(optionType:str):
 
    if optionType != 'C' and optionType != 'P':
        raise ValueError("Option Code should be either 'C' or 'P'")

    today = datetime(year = datetime.now().year, month = datetime.now().month, day = datetime.now().day  )
    lookback = datetime(year=today.year - 2, month=today.month, day=today.day)
    tradingdays = getMarketTradingDays(lookback.strftime('%Y-%m-%d'),today.strftime('%Y-%m-%d'))
    today = [d for d in tradingdays if d <= today][-1]
    tickers = getTickers()

    #remove tickers we don't have options for in polygon
    tickers.remove("^VIX")
    tickers.remove("^BCOM")

    start = "UPS"
    i = tickers.index(start)
    i = 0
    for t in tickers[i:]:

        script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
        relativepath = f"invalid/{t}.txt"
        invalidtickerfile = os.path.join(script_dir, relativepath)
        invalidtickers = []
        try:
        
            file = open(invalidtickerfile, 'r')
            invalidtickers = [symbol.rstrip() for symbol in file]
            file.close()
        except FileNotFoundError as error:
        
            #if file isn't found we will create it
            pass
        finally:

            file = open(invalidtickerfile, 'a')

        dateprices = getPriceArray(t, lookback.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"))
        optioncodes = CreateOptionCodes(t,optionType, lookback.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), dateprices)
        contractsToUpdate= getIncompleteOptionsContracts(t, optionType)
        completeContracts = getCompleteOptionContracts(t, optionType)

        optioncodes.update(set(contractsToUpdate))
        codestoquery = optioncodes - set(completeContracts)
        #codestoquery = codestoquery - set(invalidtickers)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = []
            for code in codestoquery: 
              
                lastduration = getLastOptionPriceDuration(code)
                if lastduration is None:startduration = 0 
                else:startduration = lastduration

                maturitydate = ExtractMaturityDate(code)
                url = f"https://api.polygon.io/v2/aggs/ticker/O:{code}/range/1/day/{lookback.strftime('%Y-%m-%d')}/{today.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&limit=1000&apiKey=Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk"
                #result = loadoptionPrices(optionCode = code, url = url, maturity=maturitydate, duration = startduration)
                future = executor.submit(loadoptionPrices, optionCode = code, url = url, maturity=maturitydate, duration = startduration)
                futures.append(future)
            print("Awaiting the codes to load")               

        #Addinvalid codes to invalid code file
        # for f in concurrent.futures.as_completed(futures) :
            
        MergeTables(t, optionType)
        print(f"Successfully Merged all data for {t}")

        #file.close()

       

#insertFinancialDates("2020-01-01", "2025-12-31")
CreateTempTable()
try:
    LoadMonthlyOptionPrices('C')
    LoadMonthlyOptionPrices('P')
except Exception as e:
    print(f"error loading option prices")
    
finally:
    print("finished loading procedure.")


#active options markets. Generally, 2 1/2 points when the strike price is between $5 and $25, 5 points when the strike price is between $25 and $200, 
# and 10 points when the strike price is over $200. However, these intervals can and will vary based on a number of factors.

#https://api.polygon.io/v2/aggs/ticker/O:SPY251219C00650000/range/1/day/2023-01-09/2023-01-09?adjusted=true&sort=asc&limit=120&apiKey=Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk