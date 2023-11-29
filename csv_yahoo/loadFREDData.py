import requests
import time
import sqlite3

seriesids = ["T10Y2Y","MORTGAGE30US","DHHNGSP","DGS3MO","DGS1","DGS2","DGS5","DGS10","DGS30","EXPINF2YR","WM2NS","RRPONTSYD","EFFR","WTISPLC","UNRATE","UNRATE","T10Y3M","NGDPSAXDCUSQ"]
apikey = "53a0ae2865209ae1d30ede92a923909b"

baseurl = "https://api.stlouisfed.org"



def GetSeriesInfo(seriesid) -> dict:

    #section
    url= f"{baseurl}/fred/series?series_id={seriesid}&api_key={apikey}&file_type=json"
  
    try:
        response = requests.get(url)
    except requests.ConnectionError:
        print("Connection Error.Retrying in 10 seconds...")
        time.sleep(10000)
        GetSeriesInfo(seriesid)
    except requests.ReadTimeout:
        print("Read Timeout Error.Retrying in 10 seconds...")
        time.sleep(10000)
        GetSeriesInfo(seriesid)

    data = response.json()
    
    try:
        series = data["seriess"]

    except KeyError:
        print("No results for releases")
        return
    
    return series[0]


def LoadSeriesInfo(seriesid:str,seriesinfo:dict):
    con = sqlite3.connect("FRED.db")
    cur = con.cursor()

    if "notes" not in seriesinfo:
        seriesinfo["notes"] = ""
    else:
        seriesinfo["notes"] = seriesinfo["notes"].replace("'", '"')

    

    sql = f"""
            insert into Series(SeriesID,Title,Frequency,Units,SeasonalAdj,Popularity,Notes) 
            VALUES('{seriesid}','{seriesinfo["title"]}','{seriesinfo["frequency"]}','{seriesinfo["units"]}','{seriesinfo["seasonal_adjustment"]}','{seriesinfo["popularity"]}','{seriesinfo["notes"]}')
            """
    try:
        cur.execute(sql)
    except sqlite3.IntegrityError:
        print("Table is already inserted")
        
    finally:
        con.commit()
        cur.close()
        con.close()
        pass

    print(f"Loaded Series Info for {seriesid} Successfully")


def GetObservations(id:str):
     
    url = f"{baseurl}/fred/series/observations?series_id={id}&api_key={apikey}&file_type=json"

    try:
        response = requests.get(url)
    except requests.ConnectionError:
        print("Connection Error.Retrying in 10 seconds...")
        time.sleep(10000)
        GetObservations(id)
    except requests.ReadTimeout:
        print("Read Timeout Error.Retrying in 10 seconds...")
        time.sleep(10000)
        GetObservations(id)

    data = response.json()
    
    try:
        observations = data["observations"]

    except KeyError:
        print("No results for observations")
        return
    
    series = []
    for observation in observations:
        series.append(observation)
    
    return series


def LoadSeriesObservations(observations) :

    for key in observations:
        LoadObservations(observations[key], key)

def GetSeriesObservations(seriesids):
    AllSeriesObservations = {}
    for series in seriesids:
        AllSeriesObservations[series] = GetObservations(series)

    return AllSeriesObservations

def isFloat(value:str) -> bool:
    try:
        float(value)
        return True
    except ValueError:
        return False

#insert financial date if it isn't there
#then insert series id if it isn't there
#then insert the observations for the series id
#then insert the realtime period you got these observations
def LoadObservations(seriesid,observations):

    #remove observations that don't have numbers in value field
    updated_observations = [k for k in observations if isFloat(k['value'])]
    con = sqlite3.connect("FRED.db")
    cur = con.cursor()

    sqldel = f"delete from observations where SeriesID = '{seriesid}'"
    cur.execute(sqldel)
    con.commit()

    sql2 = "insert or ignore into financialdate(date) VALUES"
    val2 = ""

    sql3 = "insert into observations (SeriesID, Date, Observation) VALUES"
    val3 = ""

    idx = 0
    for observed in updated_observations:

        val3= val3 + f"('{seriesid}','{observed['date']}',{observed['value']}),"
        val2 = val2 + f"('{observed['date']}'),"
        idx = idx + 1
        if idx % 50 == 0:
            try:
                val3 = val3[:-1]
                val2 = val2[:-1]
                cur.execute(sql2 + val2)
                con.commit()
                cur.execute(sql3 + val3)
                con.commit()
            except sqlite3.IntegrityError:
                print("This series has already been loaded")
            finally:
                val2 = ""
                val3 = ""

    if idx % 50 != 0:
        try:
            val3 = val3[:-1]
            val2 = val2[:-1]
            cur.execute(sql2 + val2)
            con.commit()
            cur.execute(sql3 + val3)
            con.commit()
        except sqlite3.Error:
            print("error inserting financial dates or observations")
        finally:
             val2 = ""
             val3 = ""

    sql4 = f"insert or ignore into RealTimePeriod(SeriesID, realtime_from, realtime_to) VALUES('{seriesid}','{observations[0]['realtime_start']}','{observations[0]['realtime_end']}')"
    cur.execute(sql4)

    con.commit()
    cur.close()
    con.close()
    print(f"successfully loaded {seriesid}")

def main():
    for seriesid in seriesids:
        seriesinfo = GetSeriesInfo(seriesid)
        LoadSeriesInfo(seriesid,seriesinfo)
        SeriesObservations = GetObservations(seriesid)
        LoadObservations(seriesid,SeriesObservations)
   

main()
