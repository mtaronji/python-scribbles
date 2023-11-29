import sqlite3
import os
import csv
import sys

DataDir = "C:/Users/Taron/dataspace/csv_yahoo"
#TAKES IN A CSV FILE AND LOADS IT INTO THE DATABASE
#ticker = os.path.basename(sys.argv[1]).split(".")[0]  DEBUG


con = sqlite3.connect("SP500.db")
curser = con.cursor()

sqlPriceInsertStart = "insert or ignore into Price(Ticker, [Date], [Open], [High], [Low], [Close], [AdjClose], [Volume]) VALUES" 

for file in os.listdir(DataDir):
    if file.endswith(".csv"):
       ticker = os.path.splitext(file)[0]
       #polygon io doesn't accept dashed, it accepts dots
       ticker = ticker.replace('-', '.')

       sqlinsertticker = f"insert or ignore into Stock(Ticker) values('{ticker}')"
       curser.execute(sqlinsertticker)
       with open(os.path.join(DataDir, file), newline='') as csvfile:
            
            datareader = csv.reader(csvfile)
            next(datareader, None)  # skip the headers          

            sqlInsertPrice = ""
            count = 0
            for row in datareader:
                
                
                curser.execute(f"insert or ignore into FinancialDate([date]) values('{row[0]}')")
                temp = datareader.line_num
                count = count + 1

                if count % 50 == 1:
                    sqlInsertPrice = f" ('{ticker}','{row[0]}',{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]}) "
                elif count % 50 == 0:
                    sqlInsertPrice = sqlInsertPrice + f" ,('{ticker}','{row[0]}',{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]}) "
                    curser.execute(sqlPriceInsertStart + sqlInsertPrice)    
                    sqlInsertPrice = ""
                else:
                    sqlInsertPrice = sqlInsertPrice + f" ,('{ticker}','{row[0]}',{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]}) "


                #insert date data
                
            
            if sqlInsertPrice != "":
                curser.execute(sqlPriceInsertStart + sqlInsertPrice)
                sqlInsertPrice = ""
            
            sqlite3.Connection.commit(con)
            print("Insert Successful-" + ticker + "\n")

        

curser.close()
con.close()