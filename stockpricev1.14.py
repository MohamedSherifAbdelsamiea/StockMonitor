from datetime import date
import stockapiv2
import dboperation

stockdb = 'tickers'
today = date.today()
print("Today is: ", today)
Apikey= stockapiv2.getapikey(0)
print(len(Apikey))
totalnumberofstocks = dboperation.getnumberofstocks(stockdb)
pointer = 1
for index in range(totalnumberofstocks)     :
    try:
        dynmodbresponse=dboperation.getindexeddb(stockdb,'index',index,'index')
        symbol=dynmodbresponse['symbol']
        mystockdata = stockapiv2.stockdata(Apikey,symbol,pointer)
        mydict=dynmodbresponse
        dboperation.putdb(stockdb, mydict)
        pointer= pointer*-1
    except:
        print('Index Empty')



