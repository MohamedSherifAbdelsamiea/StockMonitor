import boto3
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import And, Key
from datetime import date
from datetime import timedelta
import time

dynamodb = boto3.resource('dynamodb', region_name='me-south-1')

def updatecapacityprimary(mytable,read,write):
    client = boto3.client('dynamodb', 'me-south-1')
    try:
        response = client.update_table(
            TableName=mytable,
            ProvisionedThroughput={
                'ReadCapacityUnits': read,
                'WriteCapacityUnits': write
            }
        )
    except:
        print('No Action Required')

def updatecapacitysecondary(mytable,read,write):
    client = boto3.client('dynamodb', 'me-south-1')
    try:

        response = client.update_table(
            TableName=mytable,
            GlobalSecondaryIndexUpdates=[
            {
                'Update': {
                    'IndexName': mytable,
                    'ProvisionedThroughput': {
                        'ReadCapacityUnits': read,
                        'WriteCapacityUnits': write
                    }
                },
            }]
        )
    except:
        print('No Action Required')

def getnumberofstocks(mytable):
    client = boto3.client('dynamodb','me-south-1')
    response = client.describe_table(TableName='tickers')
    return response['Table']['ItemCount']

def getdb(mytable,key,index):
    table = dynamodb.Table(mytable)
    try:
        dynmodbresponse=table.query(
        KeyConditionExpression=Key(key).eq(index) 
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    dbresponse=dynmodbresponse['Items'][0]
    return dbresponse

def getdbsort(mytable,partitionkey,index,sortkey, sortindex):
    table = dynamodb.Table(mytable)
    try:
        dynmodbresponse=table.query(
        KeyConditionExpression=Key(partitionkey).eq(index) & Key(sortkey).eq(sortindex)
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    return dynmodbresponse

def getindexeddb(mytable,key,index,indexname):
    table = dynamodb.Table(mytable)
    try:
            dynmodbresponse=table.query(
            IndexName=indexname,
            KeyConditionExpression=Key(key).eq(index) 
            )
    except ClientError as e:
        print(e.response['Error']['Message'])
    try:
        dbresponse= dynmodbresponse['Items'][0]
    except:
        dbresponse=''
    return dbresponse

def putdb(mytable,dict):
    try:
        symbol = dict['symbol']
    except:
        symbol = ''
    try:
        index= dict['index']
    except:
        index = ''    
    try:
        last_updated_utc = dict['last_updated_utc']
    except:
        last_updated_utc = ''
    try:
        symbol_name = dict['symbol_name']
    except:
        symbol_name = ''
    try:
        symbol_market = dict['symbol_market']
    except:
        symbol_market = '' 
    try:
        symbol_local = dict['symbol_local']
    except:
        symbol_local = ''
    try:
        symbol_primary_exchange = dict['symbol_primary_exchange']
    except:
        symbol_primary_exchange = ''
    try:
        symbol_type = dict['symbol_type']
    except:
        symbol_type = ''
    try:
        symbol_status = dict['symbol_status']
    except:
        symbol_status = ''
    try:
        symbol_currency = dict['symbol_currency']
    except:
        symbol_currency = ''
    try:
        symbol_cik = dict['symbol_cik']
    except:
        symbol_cik = ''
    try:
        symbol_composite_figi = dict['symbol_composite_figi']
    except:
        symbol_composite_figi = ''
    try:
        symbol_share_class_figi = dict['symbol_share_class_figi']
    except:
        symbol_share_class_figi = ''
    try:
        symbol_outstanding_shares = dict['symbol_outstanding_shares']
    except:
        symbol_outstanding_shares = ''
    try:
        symbol_sic_description = dict['symbol_sic_description']
    except:
        symbol_sic_description = ''
    try:
        symbol_market_cap = dict['symbol_market_cap']
    except:
        symbol_market_cap = ''
    try:
        highlowspreadpercentage = dict['highlowspreadpercentage']
    except:
        highlowspreadpercentage = ''
    try:
        average_volume = dict['average_volume']
    except:
        average_volume = ''
    try:
        MA9greaterMA60count = dict['MA9greaterMA60count']
    except:
        MA9greaterMA60count = ''
    try:
        MA9greaterMA60spreadprecentage = dict['MA9greaterMA60spreadprecentage']
    except:
        MA9greaterMA60spreadprecentage = ''
    try:
        successiveMA9increase = dict['successiveMA9increase']
    except:
        successiveMA9increase = ''
    try:
        afterHours = dict['afterHours']
    except:
        afterHours = ''
    try:
        missingdata = dict['missingdata']
    except:
        missingdata = ''
    try:
        MA9successiveincreasepercentagecount=dict['MA9successiveincreasepercentagecount']
    except:
        MA9successiveincreasepercentagecount=''

    table = dynamodb.Table(mytable)
    table.put_item(
        Item={
        'symbol': symbol,
        'index': index,
        'last_updated_utc': last_updated_utc,
        'symbol_name': symbol_name,
        'symbol_market': symbol_market,
        'symbol_local': symbol_local,
        'symbol_primary_exchange': symbol_primary_exchange,
        'symbol_type': symbol_type,
        'symbol_status': symbol_status,
        'symbol_currency': symbol_currency,
        'symbol_cik': symbol_cik,
        'symbol_composite_figi': symbol_composite_figi,
        'symbol_share_class_figi': symbol_share_class_figi,
        'symbol_outstanding_shares': symbol_outstanding_shares,
        'symbol_sic_description' : symbol_sic_description,
        'symbol_market_cap' : symbol_market_cap,
        'highlowspreadpercentage': highlowspreadpercentage,
        'average_volume': average_volume,
        'MA9greaterMA60count': MA9greaterMA60count,
        'MA9greaterMA60spreadprecentage': MA9greaterMA60spreadprecentage,
        'successiveMA9increase': successiveMA9increase,
        'afterHours' : afterHours,
        'missingdata' : missingdata,
        'MA9successiveincreasepercentagecount': MA9successiveincreasepercentagecount
        }
        )
def putwarehousedb(mytable,data):
    table = dynamodb.Table(mytable)
    table.put_item(
        Item={
        'symbol': data['symbol'],
        'date': data['from'],
        'open' : str(data['open']),
        'high' : str(data['high']),
        'low' : str(data['low']),
        'close' : str(data['close']),
        'volume' : str(data['volume']),
        'afterHours' : str(data['afterHours']),
        'preMarket' : str(data['preMarket'])
        }
        )
def stockdata(symbol):
    mydata={}
    MAcount=0
    MA=[]
    open=[]
    high=[]
    low=[]
    close=[]
    volume=[]
    afterHours=[]
    preMarket=[]
    idate=[]
    highlowspreadprecentage=[]
    today = date.today()
    myindex=0
    Abortcondition=False
    missingdata=0
    while MAcount < 65 and Abortcondition==False and myindex < 120:
        #print("Day Delta :", myindex)
        #print("MAcount :",MAcount)
        mydate = today - timedelta(days = myindex)
        #print("Stock Date: ", mydate)
        #print("Stock Weekday: ", mydate.weekday())
        if (mydate.weekday() < 5 and MAcount < 65 and Abortcondition==False):
            response = getdbsort('tickerswarehouse','symbol',symbol,'date',str(mydate)) 
            #print(response)
            if (response['Count'] == 0):
                Abortcondition=False
                missingdata+=1    
                print('Missing Data')
            if (response['Count'] == 1):
                    MA.append(float(response['Items'][0]['close']))
                    #print("Moving Average: ",MA)
                    open.append(float(response['Items'][0]['open']))
                    high.append(float(response['Items'][0]['high']))
                    low.append(float(response['Items'][0]['low']))
                    close.append(float(response['Items'][0]['close']))
                    volume.append(float(response['Items'][0]['volume']))
                    afterHours.append(float(response['Items'][0]['afterHours']))
                    preMarket.append(float(response['Items'][0]['preMarket']))
                    idate.append(response['Items'][0]['date'])
                    highlowspreadprecentage.append(round(((float(response['Items'][0]['close'])-float(response['Items'][0]['open']))/float(response['Items'][0]['open']))*100,2))
                    MAcount=MAcount+1
        myindex+=1
    mydata['MA']= MA
    mydata['open']= open
    mydata['high']=high
    mydata['low']=low
    mydata['close'] = close
    mydata['volume'] = volume
    mydata['afterHours'] = afterHours
    mydata['preMarket'] = preMarket
    mydata['highlowspreadprecentage'] = highlowspreadprecentage
    mydata['missingdata']=missingdata
    mydata['last_updated_utc']=idate[0]

    return mydata
