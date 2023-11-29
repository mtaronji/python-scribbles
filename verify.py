SP500 = ['AAPL','MSFT',
             'AMZN','NVDA','GOOGL','BRK-B','GOOG','XOM','META','UNH','TSLA','JNJ','JPM','V',
             'PG','MA','HD','LLY','CVX','MRK','ABBV','AVGO','PEP','KO','PFE',
             'COST','TMO','MCD','WMT','BAC','CRM','ABT','CSCO','DIS','LIN','ACN',
             'ADBE','DHR','TXN','CMCSA','WFC','NEE','VC','NKE','PM','BMY','RTX',
             'ORCL','NFLX','AMD','UPS','QCOM','HON','AMGN','T','LOW','INTU','INTC','COP','SBUX',
             'UNP','MS','SPGI','GS','BA','CAT','MDT','PLD','IBM','LMT','GE',
             'GILD','ELV','ISRG','DE','BKNG','SYK','BLK','AXP','MDLZ','NOW','AMAT',
             'AMT','C','ADI','CVS','TJX','ADP','MMC','TMUS','REGN','VRTX','PYPL','CB',
             'MO','ZTS','SCHW','PGR','SO','DUK', #1ST 100
             'CI','TGT','FISV','BCX','BSX','SLB','LRCX',
             'AON','EOG','NOC','CME','CSX','MU','ITW','EQIX','ETN','APD','CL','WM','HUM','ATVI',
             'ICE','HCA','EL','CDNS','MMM','SNPS','FCX','ORLY','MPC','SHW','CCI','FDX','EW','PXD',
             'GIS','KLAC','GD','AZO','CMG','PNC','MCK','SRE','MSI','EMR','DG','NSC','AEP','D',
             'KMB','DXCM','MCO','ROP','MAR','GM','USB','PSX','MRNA','F','VLO','APH','PSA','OXY',
             'ADM','AJG','NXPI','CTVA','MSCI','FTNT','EXC','BIIB','MCHP','ADSK','TFC','TRV','PH','IDXX',
             'ECL','A','TEL','TT','MNST','JCI','HES','CTAS','TDG','MET','HLT','O','YUM','NUE',
             'ANET','DOW','XEL','LHX','HSY','SYY','AIG','PCAR','CARR', #1ST 200            
             'NEM','IQV','AFL','ROST','COF',
             'STZ','CNC','WMB','SPG','ILMN','WELL','ED','PAYX','CHTR','DVN','MTD','OTIS','KMI','EA',
             'CPRT','VICI','AMP','RMD','FIS','DHI','PPG','CMI','BK','DD','PEG','ON','DLTR','AME',
             'ODFL','ROK','PRU','GEHC','FAST','KR','KHC','VRSK','ALL','CTSH','WEC','ENPH','HAL','GWW',
             'WBD','KDP','BKR','OKE','AWK','ZBH','GPN','APTV','CSGP','RSG','LEN','ANSS','DFS','EIX',
             'SBAC','ULTA','DLR','ES','TSCO','PCG','WST','ABC','KEYS','ACGL','WTW','FANG','URI','ALGN',
             'STT','GLW','HPQ','WBA','TROW','CEG','EFX','IFF','AVB','IT','LYB','PWR','FTV','GPC',
             'EBAY','AEE','WY','BAX','VMC','CBRE','IR','CHD','PODD','ETR','CDW', #1ST 300
             'HIG','DTE','MLM','DAL','FE','FSLR','MKC','MTB','PPL','CAH','HOLX','EQR','MPWR','DOV','LH',
             'LVS','CLX','ALB','EXR','VRSN','CTRA','TDY','TTWO','ARE','CNP','NDAQ','INVH','OMC','LUV','COO',
             'FITB','XYL','HPE','STE','DRI','RJF','WAT','STLD','WAB','FICO','VTR','CMS','SEDG','NVR','NTRS',
             'CAG','TSN','EXPD','MAA','K','RF','PFG','SWKS','TRGP','BR','PKI','CINF','AMCR','ATO','DGX',
             'HBAN','IEX','BALL','SJM','FDS','AES','EPAM','FLT','MOH','HVM','LW','IRM','TYL','FMC','TER',
             'MRO','GRMN','MOS','ZBRA','J','CBOE','JBHT','BBY','PAYC','CF','RE','AVY','IPG','UAL','EVRG',
             'PHM','BRO','LKQ','TXT','BG','MGM','CFG','SNA','EXPE','LNT', #1ST 400
             'INCY','RCL','NTAP','ESS','PTC','PKG','POOL','TECH','SYF','IP','AKAM','ETSY','UDR',
             'TFX','LDOS','MKTX','APA','WYNN','EQT','DPZ','TRMB','CPT','SWK','NDSN','VTRS','KIM',
             'WRB','BWA','HST','PEAK','NI','BF-B','HRL','HSIC','CHRW','JKHY','MAS','PARA','KMX','STX',
             'L','TAP','KEY','CPB','WDC','CE','FOXA','CRL','CDAY','GEN','TPR','JNPR','BIO','MTCH','EMN',
             'GL','QRVO','LYV','CCL','REG','CZR','ROL','ALLE','PNW','UHS','XRAY','AOS','PNR','AAL','HII',
             'CTLT','BBWI','NRG','RHI','FFIV','WRK','AAP','IVZ','WHR','BEN','VFC','BXP','FRT','SEE','NWSA',
             'HAS','GNRC','AIZ','OGN','CMA','DXC','ALK','NCLH','MHK','DVA','RL','NWL','ZION','FOX','LNC',
             'FRC','NWS','DISH','S&P500'] #503

import os 

for filename in os.listdir():
    if filename.split(".")[0] not in SP500:
        print(f"{filename} is not in s&p500 list")