import pyodbc 
import csv

cnxn = pyodbc.connect('Driver={SQL Server};Server=SPACE-STATION\\MSSQLSERVER01;Database=Market;Integrated Security=true;TrustServerCertificate=True"')
cnxn_cursor = cnxn.cursor()

#create temp table for price
sql_tempoptioncodetable = """
IF OBJECT_ID(N'dbo.tempoptioncode', N'U') IS NULL
select * into dbo.tempoptioncode
from stockoption.optioncodes
WHERE 1 = 0;
"""

sql_tempseqtable = """
IF OBJECT_ID(N'dbo.tempoptionseq', N'U') IS NULL
select * into dbo.tempoptionseq
from stockoption.SequenceComplete
WHERE 1 = 0;
"""

sql_tempoptionpricestable = """
IF OBJECT_ID(N'dbo.tempoptionprices', N'U') IS NULL
select * into dbo.tempoptionprices
from stockoption.prices
WHERE 1 = 0;
"""

sql_tempstockpricetable = """
IF OBJECT_ID(N'dbo.tempstockprice', N'U') IS NULL
select * into dbo.tempstockprice
from stock.price
WHERE 1 = 0;
"""

sql_tempdatestable = """
IF OBJECT_ID(N'dbo.tempdates', N'U') IS NULL
SELECT * INTO dbo.tempdates 
  FROM stock.financialdate 
  WHERE 1 = 0;
  
"""
cnxn_cursor.execute(sql_tempoptioncodetable)
cnxn_cursor.execute(sql_tempseqtable)
cnxn_cursor.execute(sql_tempoptionpricestable)
cnxn_cursor.execute(sql_tempstockpricetable)
cnxn_cursor.execute(sql_tempdatestable)
cnxn.commit()

#cnxn_cursor.execute("drop table dbo.tempoptioncode; drop table dbo.tempoptionseq; drop table dbo.tempoptionprices; drop table dbo.tempstockprice; drop table dbo.tempdates;")
#cnxn.commit()

INSERT_AMOUNT = 500
FILE_NAMES = ["C:/Users/taron/dataspace/csv_yahoo/EXPORT/financialDates.csv",
              "C:/Users/taron/dataspace/csv_yahoo/EXPORT/optioncodes.csv",
              "C:/Users/taron/dataspace/csv_yahoo/EXPORT/optionprices.csv",
              "C:/Users/taron/dataspace/csv_yahoo/EXPORT/optionsequences.csv"
              ]
#insert the csv data into temp tables

#Code,Duration,MaturityDate,Open,Close,AdjClose,High,Low,Volume,VWAP
#Code,IsComplete

cnxn_cursor.execute("delete from dbo.tempdates")
with open(FILE_NAMES[0]) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    next(reader, None)  # skip the headers
   
    sql_head = "insert into dbo.tempdates(Date) values"
    sql = sql_head
    for idx,row in enumerate(reader):
        sql = sql + f"('{row['Date']}'),"
        if idx % INSERT_AMOUNT == 0:
             cnxn_cursor.execute(sql[:-1])
             sql = sql_head

    if sql != sql_head:cnxn_cursor.execute(sql[:-1])
    cnxn.commit()

cnxn_cursor.execute("delete from dbo.tempoptioncode")
with open(FILE_NAMES[1]) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    next(reader, None)  # skip the headers
    sql_head = "insert into dbo.tempoptioncode(Code) values"
    sql = sql_head
    for idx,row in enumerate(reader):
        sql = sql + f"('{row['Code']}'),"
        if idx % INSERT_AMOUNT == 0:
            cnxn_cursor.execute(sql[:-1])
            sql = sql_head
    if sql != sql_head:cnxn_cursor.execute(sql[:-1])
    cnxn.commit()

cnxn_cursor.execute("delete from dbo.tempoptionprices")
with open(FILE_NAMES[2]) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    next(reader, None)  # skip the headers
    sql_head1 = "insert into dbo.tempoptionprices(Code,Duration,MaturityDate,[Open],[Close],AdjClose,High,Low,Volume,VWAP) values"
    sql_head2 = "insert into dbo.tempoptionprices(Code,Duration,MaturityDate,[Open],AdjClose,High,Low,Volume,VWAP) values"
    sql_head = sql_head2
    sql = sql_head
    for idx,row in enumerate(reader):

        if row['Close'] == '': 
             sql = sql + f"('{row['Code']}',{row['Duration']},'{row['MaturityDate']}',{row['Open']},{row['AdjClose']},{row['High']},{row['Low']},{row['Volume']},{row['VWAP']}),"
        else:
            sql = sql + f"('{row['Code']}',{row['Duration']},'{row['MaturityDate']}',{row['Open']},{row['Close']},{row['AdjClose']},{row['High']},{row['Low']},{row['Volume']},{row['VWAP']}),"

        if idx % INSERT_AMOUNT == 0:
            cnxn_cursor.execute(sql[:-1])
            sql = sql_head
    if sql != sql_head:cnxn_cursor.execute(sql[:-1])
    cnxn.commit()

cnxn_cursor.execute("delete from dbo.tempoptionseq")
with open(FILE_NAMES[3]) as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',')
    next(reader, None)  # skip the headers
    sql_head = "insert into dbo.tempoptionseq(Code,IsComplete) values"
    sql = sql_head
    for idx,row in enumerate(reader):
        sql = sql + f"('{row['Code']}',{row['IsComplete']}),"
        if idx % INSERT_AMOUNT == 0:
             cnxn_cursor.execute(sql[:-1])
             sql = sql_head
    if sql != sql_head:cnxn_cursor.execute(sql[:-1])
    cnxn.commit()

# with open(FILE_NAMES[4]) as csvfile:
#     reader = csv.reader(csvfile, delimiter=',')
#     next(reader, None)  # skip the headers
#     sql = "insert into dbo.tempdates(date) values"
#     for idx,row in enumerate(reader):
#         sql = sql + ""
#         if idx % 50 == 0:
#              cnxn_cursor.execute(sql[:-1])



#after inserts and merge drop all the tables
cnxn_cursor.execute("drop table dbo.tempoptioncode; drop table dbo.tempoptionseq; drop table dbo.tempoptionprices; drop table dbo.tempstockprice; drop table dbo.tempdates;")
cnxn.commit()