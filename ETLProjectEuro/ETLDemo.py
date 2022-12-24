
import os
import pyodbc 
import sys
import petl
import configparser
import requests
import datetime
import json
import decimal



# get data from configuration file
config = configparser.ConfigParser()
try:
    config.read('ETLDemo.ini')
except Exception as e:
    print('could not read configuration file:' + str(e))
    sys.exit()


# read settings from configuration file
startDate = config['CONFIG']['startDate']
url = config['CONFIG']['url']
destServer = config['CONFIG']['server']
destDatabase = config['CONFIG']['database']

# request data from URL
try:
    BOCResponse = requests.get(url+startDate)
except Exception as e:
    print('could not make request:' + str(e))
    sys.exit()
#print (BOCResponse.text)

# initialize list of lists for data storage
BOCDates = []
BOCRates = []

# check response status and process BOC JSON object
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)

    # extract observation data into column arrays
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(decimal.Decimal(row['FXEURCAD']['v']))

    # create petl table from column arrays and rename the columns
    exchangeRates = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])

    print(exchangeRates)

    # load expense document
    try:
        expenses = petl.io.xlsx.fromxlsx('Expenses.xlsx',sheet='Github')
    except Exception as e:
        print('could not open expenses.xlsx:' + str(e))
        sys.exit()

    print(expenses)

    
    # join tables
    expenses = petl.outerjoin(exchangeRates,expenses,key='date')
    print(expenses)

    # fill down missing values
    expenses = petl.filldown(expenses,'rate')
    print(expenses)

    # remove dates with no expenses
    expenses = petl.select(expenses,lambda rec: rec.EUR != None)
    print(expenses)

    #add CDN column
    expenses = petl.addfield(expenses,'CAD', lambda rec: decimal.Decimal(rec.EUR) * rec.rate)
    print(expenses)


# intialize database connection
try:
        conn = pyodbc.connect('Driver={SQL Server};'

        'Server=HP-OMEN-LAPTOP;'
        'Database=ETLDemo;'
        'UID=NTID_OR_SVCACCT;'
        'Trusted_Connection=yes;')
        print(conn)
except Exception as e:
        print('could not connect to database:' + str(e))
        sys.exit()

    # populate Expenses database table
try:
        petl.io.todb (expenses,conn,'ExpensesEuro')
except Exception as e:
        print('could not write to database:' + str(e))
print(expenses)
