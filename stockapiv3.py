import dboperation
import requests
from datetime import date
from datetime import timedelta

def getapikey(i):
    myindex = keyindex(i)
    myapikey= dboperation.getdb('tickersconfig','name',myindex)['key']
    return myapikey

def keyindex(i):
    switcher={
        0:'apikey',
        1:'apikey2',
        2:'apikey3',
        3:'apikey4'
    }
    return switcher.get(i,'Invalid entry')

def stockquery(symbol,date,key):
    response = requests.get("https://api.polygon.io/v1/open-close/{}/{}?adjusted=true&apiKey={}".format(symbol,date,key))
    return response.json()

def stockdata(key,symbol):
    mydata={}
    MAcount=0
    n=0
    open=[]
    high=[]
    low=[]
    close=[]
    volume=[]
    afterHours=[]
    preMarket=[]
    today = date.today()
    myindex=0
    while MAcount < 3 and n < 5:
        for mykey in range(len(key)):
            print('n :', n)
            print("Day Delta :", myindex)
            print("MAcount :",MAcount)
            print(key[mykey])
            mydate = today - timedelta(days = myindex)
            print("Stock Date: ", mydate)
            print("Stock Weekday: ", mydate.weekday())
            if (mydate.weekday() < 5 and MAcount < 3):
                response = stockquery(symbol,mydate,key[mykey])
                print(response)
                if (response['status'] == 'NOT_FOUND'):
                    myindex+=1
                    print('index increased due to data not found index now is : ',myindex )
                if (response['status'] == 'ERROR'):
                    if (response['error'] == 'today\'s Date not supported yet'):
                        myindex+=1
                        print('index increased due to day not supported index now is : ',myindex )
                if (response['status'] == 'OK'):
                        dboperation.putwarehousedb('tickerswarehouse',response)
                        #print("Moving Average: ",MA)
                        open.append(response['open'])
                        high.append(response['high'])
                        low.append(response['low'])
                        close.append(response['close'])
                        volume.append(response['volume'])
                        afterHours.append(response['afterHours'])
                        preMarket.append(response['preMarket'])
                        MAcount=MAcount+1
                        myindex+=1
                        print('index increased due to succ data index now is: ',myindex )
            else:
                myindex+=1            
        n+=1
    mydata['open']= open
    mydata['high']=high
    mydata['low']=low
    mydata['close'] = close
    mydata['volume'] = volume
    mydata['afterHours'] = afterHours
    mydata['preMarket'] = preMarket

    return mydata
