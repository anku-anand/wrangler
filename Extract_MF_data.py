import re
import pymongo
from pymongo import MongoClient
import wget
import sys
import getopt
import os
import os.path
import time


months = ["Jan",'Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

def usage():
    print ('To get data for current month use: python Extract_MF_data.py ')
    print ('To get data for a given month and year use: python Extract_MF_data.py --year <year> --month <month>')
    print ('To get data for a given range of year and mont use: \n\t python Extract_MF_data.py --fromyear <year> --toyear <year> [--frommonth <month> --tomonth <month]')
    print ('year in YYYY format')
    print ('month as "Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec')
    sys.exit()

def getEndDate(month,year):
    if month in ["Jan",'Mar','May','Jul','Aug','Oct','Dec']:
        return "31-"+month+"-"+year
    elif month in ['Apr','Jun','Sep','Nov']:
         return "30-"+month+"-"+year
    elif month in ['Feb']:
        return "29-"+month+"-"+year
    else:
        usage()

def parseOptions(argv):
   #print(str(argv))
   year = ''
   month = ''
   frommonth = tomonth = fromyear = toyear = None
   year, month, day = time.strftime("%Y,%b,%d").split(',')
   #frommonth = tomonth = month
   try:
      opts, args = getopt.getopt(argv,"h:y:m:",["help","year=","month=","toyear=","fromyear=","frommonth=","tomonth="])
   except getopt.GetoptError:
        usage()
   #print (str(opts))
   for opt, arg in opts:
      if opt in ['-h','--help']:
            usage()
      elif opt in ("-y", "--year"):
         year = arg
      elif opt in ("-m", "--month"):
         month = arg
         if month not in months:
             print('Months in wrong format')
             usage
      elif opt in ("-fy", "--fromyear"):
         fromyear = arg
      elif opt in ("-ty", "--toyear"):
         toyear = arg
      elif opt in ("-fm", "--frommonth"):
         frommonth = arg
         if frommonth not in months:
             print('frommonth in wrong format')
             usage
      elif opt in ("-tm", "--tomonth"):
         tomonth = arg
         if tomonth not in months:
             print('tomonth in wrong format')
             usage
   if fromyear is not None:
      if toyear is None:
          toyear = year
   if toyear is not None:
       if fromyear is None:
           print('--fromyear is missing')
           usage()
   if frommonth is not None:
      if tomonth is None:
          print('--tomonth is missing')
          usage()
   if tomonth is not None:
       if frommonth is None:
           print('--frommonth is missing')
           usage()
   if frommonth is not None :
       if fromyear is None:
           print('Month range cannot be specified without Year range')
           usage()
   if fromyear is not None and toyear is not None:
     #print (fromyear)  
     if int(fromyear) > int(toyear):
         print('from year cannot be greater than to year')
         usage()
     if int(fromyear) == int(toyear):
        if months.index(tomonth) < months.index(frommonth):
            print('From month cannot be greater than to month')
            usage()
     if frommonth is None:
         frommonth = month
     if tomonth is None:
         tomonth = month
   #print ('year is ', year)
   #print ('month is ', month)
   options = {}
   if fromyear is not None:
       options['range'] = 'True'
       options['fromyear'] = int(fromyear)
       options['toyear'] = int(toyear)
       options['frommonth'] = frommonth
       options['tomonth'] = tomonth
       return getDateRange(options)
   else:
       print('No options specified')
       options['range'] = 'False'
       options['fromDate'] = "01-"+month+"-"+year
       options['endDate'] = getEndDate(month,year)
       options['fileName'] = "./"+year+"-"+month+"-data"
       return [options]


def insertToDB(db,collecionName,data):
    collection=db[collecionName]
    collection.insert_one(data)
    return
def checkDataExists(db,collectionName,data):
    collection=db[collectionName]
    return bool(collection.find_one(data))

def getDataFromAMFI(query):
    print (query['fromDate'],query['endDate'],query['fileName'])
    url="http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt="+query['fromDate']+"&todt="+query['endDate']
    print (url)
    if os.path.isfile(query['fileName']):
            os.remove(query['fileName'])
    wget.download(url,out=query['fileName'])
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
        sys.exit()


    for line in open(fileName):
        #print (line)
        line = line.rstrip('\r\n')
        if((len(line) <= 0)):
        #    #print(len(line))
            continue
        D = dataPattern.match(line)
        if D:
            if(fundID != D.group('schemeID')):
                #print("Inserted "+str(countDataperScheme)+" for "+fundHouse+" ID"+fundID)
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
            data = {
                "date": D.group('date'),
                "SchemeID": fundID,
                "NAV": D.group('nav'),
                "SP": D.group('sp'),
                "RP":D.group('rp') 
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
    print ("Total Data inserted " + str(countData))

def getDateRange(options):
    """ Gets options and returns a list of dict
    each dict has fromdate, enddate, filename """
    #print(options)
    queries = []
    fromyear = options['fromyear']
    toyear = options['toyear']
    """ if frommonth is Jan and tomonth is Dec, a single loop is sufficient else first and last years has to be dealt seperately """
    if options['frommonth'] is not "Jan":
        #print(options['frommonth'])
        index = months.index(options['frommonth'])
        for m in range(index,12):
            month = months[m]
            #print(month+':'+str(fromyear))
            query = {
                    'fromDate':'01-'+month+'-'+str(fromyear),
                    'endDate':getEndDate(month,str(fromyear)),
                    'fileName':'./'+str(fromyear)+'-'+month+'-Data'
                    }
            queries.append(query)
        fromyear = fromyear+1
    diff = toyear - fromyear 
    #print ('fromyear: '+str(fromyear)+' toyear: '+str(toyear)+" diff is :"+str(diff))
    if diff > 0 :
       for year in range(fromyear,toyear):
        #print(year)
        for month in months:
            query = {
                    'fromDate':'01-'+month+'-'+str(year),
                    'endDate':getEndDate(month,str(year)),
                    'fileName':'./'+str(year)+'-'+month+'-Data'
                    }
            queries.append(query)
            #print(month+':'+str(year))
    if options['tomonth'] is not "Dec":
        #print('tomonth: '+options['tomonth'])
        index = months.index(options['tomonth'])
        #print(index)
        for m in range(0,index+1):
            month = months[m]
            #print(month+':'+str(toyear))
            query = {
                    'fromDate':'01-'+month+'-'+str(toyear),
                    'endDate':getEndDate(month,str(toyear)),
                    'fileName':'./'+str(toyear)+'-'+month+'-Data'
                    }
            queries.append(query)
    return queries       


def getDataAndParse(query):
        getDataFromAMFI(query)
        parser(query['fileName'])
        print(query)
        pass

if __name__ == "__main__":
    print ("Starting MF NAV parser")
    options=parseOptions(sys.argv[1:])
    #print(options)
    if options is None:
        sys.exist()
    for option in options:
        getDataAndParse(option)
    #parser('./2019-Jan-data')
