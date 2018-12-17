import re
import pymongo
from pymongo import MongoClient
import wget
import sys
import getopt


def usage():
    print 'usage test.py --year <year> --month <month>'
    print 'year in YYYY format'
    print 'month as "Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec'

def getEndDate(month,year):
    if month in ["Jan",'Mar','May','Jul','Aug','Oct','Dec']:
        return "31-"+month+"-"+year
    elif month in ['Apr','Jun','Sep'+'Nov']:
         return "30-"+month+"-"+year
    elif month in ['Feb']:
        return "28-"+month+"-"+year
    else:
        usage()
        sys.exit()

def parseOptions(argv):
   #print(str(argv))
   year = ''
   month = ''
   try:
      opts, args = getopt.getopt(argv,"h:y:m:",["help","year=","month="])
   except getopt.GetoptError:
        usage()
        sys.exit(2)
   #print str(opts)
   for opt, arg in opts:
      if opt in ['-h','--help']:
            usage()
            sys.exit()
      elif opt in ("-y", "--year"):
         year = arg
      elif opt in ("-m", "--month"):
         month = arg
   #print 'year is ', year
   #print 'month is ', month
   return  {
       "fromDate":"01-"+month+"-"+year,
       "endDate":getEndDate(month,year),
       "fileName":year+"-"+month+"-data"
   }

def insertToDB(db,collecionName,data):
    collection=db[collecionName]
    collection.insert_one(data)
    return
def checkDataExists(db,collectionName,data):
    collection=db[collectionName]
    return bool(collection.find_one(data))

def getDataFromAMFI(fromdate,todate,storein):
    #print fromdate,todate,storein
    url="http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt="+fromdate+"&todt="+todate
    print url
    wget.download(url,out=storein)
    return

def parser(fileName):
    # Main section starts here
    # Initialize compile patterns
    fundTypePattern=re.compile(r'(?P<type>^Interval|Open|Close).*\( (?P<subtype>[\S.* ]+)\)')
    #dataPattern=re.compile(r'(?P<schemeID>^\d+);(?P<schemeName>[\S+ ]+);(?P<nav>\S+);;;(?P<date>\S+)')
    dataPattern=re.compile(r'(?P<schemeID>^\d+);(?P<schemeName>[\S+ ]+);(?P<nav>\S+);(?P<rp>.*);(?P<sp>.*);(?P<date>\S+)')
    # Initialize variables
    fundHouse=""
    fundName=""
    fundType=""
    fundSubType=""
    fundID=""
    countScheme=0
    countData=0
    countDataperScheme=0
    #print(fileName)
    #Initialize DB Connection
    db = MongoClient('mongodb://localhost:27017/').mfv1
    if not db:
        print("not able to connect to DB")
        exit


    for line in open(fileName):
        #print line
        line = line.rstrip('\r\n')
        if((len(line) <= 0)):
        #    #print(len(line))
            continue
        D = dataPattern.match(line)
        if D:
            if(fundID != D.group('schemeID')):
                print("Inserted "+str(countDataperScheme)+" for "+fundHouse+" ID"+fundID)
                #insert new ID into DB
                fundID = D.group('schemeID')
                fundName = D.group('schemeName')
                data = {
                    "Org":fundHouse,
                    "Org ID": "ddd",
                    "SchemeID":fundID,
                    "SchemeName":fundName,
                    "TYPE":fundType,
                    "SubType":fundSubType
                }
                checkData = {
                    "Org":fundHouse,
                    "SchemeID":fundID
                }
                if not checkDataExists(db,"mf",checkData):
                    insertToDB(db,"mf",data)
                #print("Inserted "+countDataperScheme+" for "+fundHouse+" ID"+fundID)
                countDataperScheme=0
                countScheme = countScheme + 1
            nav = D.group('nav')
            date = D.group('date')
            #print(date+' '+fundID+' '+nav)
            data = {
                "date": date,
                "SchemeID": fundID,
                "NAV": nav,
                "SP": "",
                "RP": ""
            }
            insertToDB(db,"navdata",data)
            countData=countData + 1
            countDataperScheme = countDataperScheme + 1
            continue
        T = fundTypePattern.match(line)
        if T:
            fundType=T.group('type')
            fundSubType=T.group('subtype')
            continue
        if(fundHouse != line):
            fundHouse=line  
            #print(fundHouse)
        if(line.find("Scheme Code") !=  -1 ):
            continue
    print "Total Data inserted " + str(countData)

if __name__ == "__main__":
    print ("Starting MF NAV parser")
    #fileName="2018-Oct-data"
    #fullPath=filePath+fileName
    data=parseOptions(sys.argv[1:])
    filePath="./"
    fileName=filePath+data.get('fileName')
    getDataFromAMFI( data.get('fromDate'),data.get('endDate'),fileName)
    parser(fileName)
