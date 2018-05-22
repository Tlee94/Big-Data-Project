import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StringType
from pyspark.sql.functions import countDistinct
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.types import FloatType
from operator import add
import pyspark as ps

def taxisid(pId,lines):
    import csv
    if pId==0:
        lines.next()
    reader = csv.reader(lines)
    for row in reader:
        if (row[0] !='' and row[0] !='taxi_id'):
            (taxi_id) = ((row[0]))
            yield (taxi_id)

def taxi_trip(pId, lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in reader:
        if (row[13] != '' and row[13] != 'trip_total'):
            (trip_total) = (float(row[13]))
            yield (trip_total)

def chicagocashcredit(pId,lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
	if (row[14] == 'Cash' or row[14] == 'Credit Card'):
	    yield(row[14])

def chi_cash(pId,lines):
    import csv
    if pId == 0:
       lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[14] == 'Cash'):
            yield(row[14])
    
def chi_credit(pId,lines):
    import csv
    if pId == 0:
       lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[14] == 'Credit Card'):
            yield(row[14])

def nyccash(pId,lines):
    import csv
    if pId == 0:
       lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[19] == '2'):
           yield(row[19])

def nyccredit(pId,lines):
    import csv
    if pId == 0:
       lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[19] == '1'):
           yield(row[19])

def nyc_taxi_trip(pId, lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in reader:
        (yield(float(row[18].replace(',',''))))

#def get_chi_taxi_trip_sec(pId, lines):
#    import csv
#    if pId == 0:
#        lines.next()
#    for row in csv.reader(lines):
#        if row[3] != "":
#            yield row[3]

def nyc_filter_timePM(pId,lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[18] != '' and
           row[1][20] == 'P'):
            yield(row[1][11:13])

def nyc_filter_timePM_fares(pId,lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[18] != '' and
           row[1][20] == 'P'):
            yield(row[1][11:13],float(row[18].replace(',','')))

def nyc_filter_timeAM(pId,lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[18] != '' and
           row[1][20] == 'A'):
            yield(row[1][11:13])

def nyc_filter_timeAM_fares(pId,lines):
    import csv
    if pId == 0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[18] != '' and
           row[1][20] == 'A'):
            yield(row[1][11:13],float(row[18].replace(',','')))

def chi_filter_time_fare(pId,lines):
    import csv
    if pId==0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[9] != ''):
            yield (row[1][10:12],float(row[9]))

def chi_filter_time(pId,lines):
    import csv
    if pId==0:
        lines.next()
    reader = csv.reader(lines)
    for row in csv.reader(lines):
        if (row[9] != ''):
            yield (row[1][10:12])

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')
    sc = SparkContext()

    spark = SparkSession(sc)

    taxi01 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_01.csv')
    taxi02 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_02.csv')
    taxi03 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_03.csv')
    taxi04 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_04.csv')
    taxi05 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_05.csv')
    taxi06 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_06.csv')
    taxi07 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_07.csv')
    taxi08 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_08.csv')
    taxi09 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_09.csv')
    taxi10 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_10.csv')
    taxi11 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_11.csv')
    taxi12 = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_12.csv')
    alltaxi = sc.union([taxi01, taxi02, taxi03, taxi04, taxi05,
                        taxi06, taxi07, taxi08, taxi09, taxi10,
                        taxi11, taxi12])
    
    nyc = sc.textFile('/user/tlee000/2016_Green_Taxi_Trip_Data.csv')
    
    nyctaxifare = nyc.mapPartitionsWithIndex(nyc_taxi_trip).cache()
    totalnycfare = nyctaxifare.reduce(lambda accum, n: accum + n)
    countnycfare = nyctaxifare.count()
    avgnycfareperride = totalnycfare/countnycfare
    taxifare = alltaxi.mapPartitionsWithIndex(taxi_trip).cache()
    totalfare = taxifare.reduce(lambda accum, n: accum + n)
    countfare = taxifare.count()
    avgfareperride = totalfare/countfare
    print ("Average fare per ride for Chicago:  %s")  % avgfareperride
    print("Average fare per ride for NYC: %s") % avgnycfareperride
    
    taxi_id = alltaxi.mapPartitionsWithIndex(taxisid).cache()
    taxi_id = taxi_id.distinct()
    print("Number of Vehicles in Chicago: %s") % taxi_id.count() 
    print('\n')

    chicagocashorcredit = alltaxi.mapPartitionsWithIndex(chicagocashcredit).cache()
    chicagocashorcreditcount = chicagocashorcredit.count()
    print(chicagocashorcreditcount)
    c = chicagocashorcredit.take(5)
    print(c)    
    
    chicago_cash = alltaxi.mapPartitionsWithIndex(chi_cash).cache()
    chicash = chicago_cash.count()
    print(chicash)

    chicago_credit = alltaxi.mapPartitionsWithIndex(chi_credit).cache()
    chicredit = chicago_credit.count()
    print(chicredit)    
    percentchicash = ((float(chicash)/float(chicagocashorcreditcount)) * 100)
    percentchicredit = ((float(chicredit)/float(chicagocashorcreditcount)) * 100)

    print("Percentage of Cash Payment in Chicago: %s") % percentchicash
    print("Percent of Credit Payment in Chicago: %s") % percentchicredit
    print("Number of Cash Payments in Chicago: %s") % chicash
    print("Number of Credit Payments Chicago: %s") % chicredit
    
    nyc_credit = nyc.mapPartitionsWithIndex(nyccredit).cache()
    ny_credit = nyc_credit.count()
    nyc_cash = nyc.mapPartitionsWithIndex(nyccash).cache()
    ny_cash = nyc_cash.count()
    percentnycash = ((float(ny_cash)/((float(ny_cash)) + ((float(ny_credit))))) * 100)
    percentnycredit = ((float(ny_credit)/((float(ny_cash)) + ((float(ny_credit))))) * 100)

    print("Percentage of Cash Payment in NYC: %s") % percentnycash
    print("Percentage of Credit Payment in NYC: %s") % percentnycredit
    print("Number of Cash Payments in NYC: %s") % ny_cash
    print("Number of Credit Payments in NYC: %s") % ny_credit

    chicago_payments = chicagocashorcredit.map(lambda row: (row,1)).reduceByKey(add)
    ch_pcount = chicago_payments.take(5)
    print("Credit Card and Cash Payments in Chicago: %s") % ch_pcount

    PMtimes = nyc.mapPartitionsWithIndex(nyc_filter_timePM)
    nyc_pm_count = PMtimes.map(lambda row: (row,1)).reduceByKey(add)
    nycpmcount = nyc_pm_count.take(12)
    print("PM counts for nyc: %s") % nycpmcount

    AMtimes = nyc.mapPartitionsWithIndex(nyc_filter_timeAM)
    nyc_am_count = AMtimes.map(lambda row: (row,1)).reduceByKey(add)
    nycamcount = nyc_am_count.take(12)
    print("AM counts for nyc: %s") % nycamcount

    
    PMtimefare = nyc.mapPartitionsWithIndex(nyc_filter_timePM_fares)
    nycPMtimefare = PMtimefare.reduceByKey(add)
    print("NYC PM fares hour: ")
    print(nycPMtimefare.take(12))

    AMtimefare = nyc.mapPartitionsWithIndex(nyc_filter_timeAM_fares)
    nycAMtimefare = AMtimefare.reduceByKey(add)
    print("NYC AM fares hour: ")
    print(nycAMtimefare.take(12))
    

    print("Chicago fares per hour each month: ")
    chitimefare1 = taxi01.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare2 = taxi02.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare3 = taxi03.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare4 = taxi04.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare5 = taxi05.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare6 = taxi06.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare7 = taxi07.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare8 = taxi08.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare9 = taxi09.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare10 = taxi10.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare11 = taxi11.mapPartitionsWithIndex(chi_filter_time_fare)
    chitimefare12 = taxi12.mapPartitionsWithIndex(chi_filter_time_fare)

    chitime_fare1 = chitimefare1.reduceByKey(add)
    chitime_fare2 = chitimefare2.reduceByKey(add)
    chitime_fare3 = chitimefare3.reduceByKey(add)
    chitime_fare4 = chitimefare4.reduceByKey(add)
    chitime_fare5 = chitimefare5.reduceByKey(add)
    chitime_fare6 = chitimefare6.reduceByKey(add)
    chitime_fare7 = chitimefare7.reduceByKey(add)
    chitime_fare8 = chitimefare8.reduceByKey(add)
    chitime_fare9 = chitimefare9.reduceByKey(add)
    chitime_fare10 = chitimefare10.reduceByKey(add)
    chitime_fare11 = chitimefare11.reduceByKey(add)
    chitime_fare12 = chitimefare12.reduceByKey(add)
    
    print("Jan")
    print(chitime_fare1.take(48))
    print("Feb")
    print(chitime_fare2.take(48))
    print("Mar")
    print(chitime_fare3.take(48))
    print("Apr")
    print(chitime_fare4.take(48))
    print("may")
    print(chitime_fare5.take(48))
    print("june")
    print(chitime_fare6.take(48))
    print("july")
    print(chitime_fare7.take(48))
    print("aug")
    print(chitime_fare8.take(48))
    print("sept")
    print(chitime_fare9.take(48))
    print("oct")
    print(chitime_fare10.take(48))
    print("nov")
    print(chitime_fare11.take(48))
    print("dec")
    print(chitime_fare12.take(48))
    
    chi_times = alltaxi.mapPartitionsWithIndex(chi_filter_time)
    chicago_time_count = chi_times.map(lambda row: (row,1)).reduceByKey(add)
    print('/n')
    print("Chicago trip counts per hour: ")
    print(chicago_time_count.take(48))

