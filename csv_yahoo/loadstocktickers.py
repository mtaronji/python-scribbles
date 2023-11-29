import requests
import json
import time
import sqlite3

#this script handles initial loads of the database for missing data.
#for the script that loads the database each day, please see  truthfullsloadC
#for the script that loads data from the command line please truthfullsloadCL

#as above this 
polykey = "Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk"
apiurl = ""


#https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-09/2023-01-09?apiKey=Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk

# def CreateURL(ticker):
#     dirtyurl = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/2023-01-09/2023-01-09?apiKey={key}"
#     url = dirtyurl.format(ticker= ticker, key = polykey)

#gets the tickers from the API and adds them to the prod db
#insert tickers 50 at a time to make proces go faster
def GetTickers(url):
    con = sqlite3.connect('SP500.db')
    cur = con.cursor()
    sqlinsert = "insert or ignore into stock values"
    response = requests.get(url)
    data = response.json()
    results = data["results"]

    try:
        nxt = data["next_url"]
    except KeyError:
        nxt = None
        print("no nxt. this is the last page")
        pass
    count = data["count"]
    i = 0
    for element in results:
        ticker = element["ticker"].replace("'", "")
        name = element["name"].replace("'", "")
        locale = element["locale"].replace("'", "")
        
        if i > 0:
            sqlinsert += f",('{ticker}','{name}','{locale}')"
        else: 
            sqlinsert += f"('{ticker}','{name}','{locale}')"
    
        i = i + 1
        if i == count:
            cur.execute(sqlinsert)
            con.commit()
            time.sleep(12)
            print("Loaded ", count, " records into the db")
            if  nxt is None:
                print("Finished Loading")
                return
            else:          
                con.close()
                print("Nxt has value loading the nxt set of records. Nxt: ", nxt)
                nxt_url = nxt + "&apiKey=" + polykey
                return GetTickers(nxt_url)

GetTickers("https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&apiKey=Bnb4RtYGOp4LDSqpJGDoGiDOGW2gbYBk")
                

