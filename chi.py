import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession

from functools import reduce
from operator import add

def get_chi_taxi_trip_sec(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[3] != "":
            yield row[3]


def is_number(string):
    try:
        float(string)
        return True
    except ValueError:
        return False

#get all the trip miles
def get_chi_taxi_trip_miles(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[4] != "":
            yield row[4]


def get_chi_taxi_trip_avg_sec():
    l = chi_taxi_sec.take(chi_taxi.count())
    return reduce(lambda x, y: float(x) + float(y), l) / len(l)

def get_chi_taxi_avg_mile():
    l = chi_taxi_tripmiles.take(chi_taxi.count())
    return reduce(lambda x, y: float(x) + float(y), l) / len(l)


def get_avg_mile_per_hour_chi():
    avg_trip_sec = get_chi_taxi_trip_avg_sec()
    avg_trip_mile = get_chi_taxi_avg_mile()

    return avg_trip_mile * (3600 / avg_trip_sec)



def time_bracket_helper(sec):
    sec = int(sec)
    if sec > 0 and sec < 60:  # 1 min
        sec = 1
    elif sec >= 60 and sec < 300:  # 5min
        sec = 5
    elif sec >= 300 and sec < 600:  # 10min
        sec = 10
    elif sec >= 600 and sec < 1200:  # 20min
        sec = 20
    elif sec >= 1200 and sec < 1800:  # 30min
        sec = 30
    elif sec >= 1800 and sec < 3600:  # 1hr
        sec = 60
    elif sec >= 3600:  # more than 1 hour
        sec = 100

    return sec



def get_time_traveled_bracket_chi():
    chi_time_bracket = chi_taxi_sec.map(lambda row: (time_bracket_helper(row), 1)).reduceByKey(add)
    chi_time_bracket = chi_time_bracket.collect()
    chi_time_bracket = sorted(chi_time_bracket, key=lambda x: int(x[0]))
    return chi_time_bracket



def dist_traveled_bracket(m):
    miles = float(m)
    if miles < 1:  # 1 min
        miles = 1
    elif miles >= 1 and miles < 2:  # 5min
        miles = 2
    elif miles >= 2 and miles < 3:  # 10min
        miles = 3
    elif miles >= 4 and miles < 5:  # 10min
        miles = 4
    elif miles >= 5 and miles < 6:  # 10min
        miles = 5
    elif miles >= 6 and miles < 7:  # 5min
        miles = 6
    elif miles >= 7 and miles < 8:  # 10min
        miles = 7
    elif miles >= 8 and miles < 9:  # 10min
        miles = 8
    elif miles >= 9 and miles < 10:  # 10min
        miles = 9
    elif miles >= 10 and miles < 35:
        miles = 10
    else:
        miles = 9999

    return int(miles)


def get_dist_traveled_bracket_chi():
    chi_dist_traveled = chi_taxi_tripmiles.map(lambda row: (dist_traveled_bracket(row), 1)).reduceByKey(add)
    chi_dist_traveled = chi_dist_traveled.collect()
    chi_dist_traveled = sorted(chi_dist_traveled, key=lambda x: int(x[0]))
    return chi_dist_traveled

def get_distinct_companies_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[15] != "":
            yield row[15]


def get_company_business_record_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[15] != "":
            yield row[15]

def get_company_business_bracket():
    comp_bracket = companies_business.map(lambda row: (row, 1)).reduceByKey(add)
    comp_bracket = comp_bracket.collect()
    comp_bracket = filter(lambda x: x[0].isdigit(),comp_bracket)
    comp_bracket = sorted(comp_bracket, key=lambda x: int(x[0]))
    return comp_bracket

    #
    # company_business_bracket = {}
    # for d in companies_business.collect():
    #     if d not in company_business_bracket:
    #         company_business_bracket[d] = 0
    #     company_business_bracket[d] += 1
    #
    # return company_business_bracket


def get_trip_start_time_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            yield row[1][-8:-6]


def get_taxi_time_start_bracket_chi():
    # time_dict = {'1':0, '2':0, '3':0, '4':0, '5':0, '6':0, '7':0, '8':0, '9':0, '10':0, '11':0, '12':0, '13':0,
    #              '14:'0, '15':0, '16':0, '17':0, '18':0, '19':0, '20':0, '21':0, '22':0, '23':0, '24':0}

    st = start_time.map(lambda row: (row, 1)).reduceByKey(add)
    st = st.collect()
    st = filter(lambda x: x[0].isdigit(), st)
    st = sorted(st, key=lambda x: int(x[0]))
    return st
    #
    # time_dict = {}
    #
    # for time in start_time.collect():
    #     if time not in time_dict:
    #         time_dict[time] = 0
    #     time_dict[time] += 1
    #
    #
    # l = []
    # for key, value in time_dict.items():
    #     if key != 'im':
    #         l.append((key, value))
    #
    # l = sorted(l, key=lambda x: int(x[0]))
    # return l


def get_trip_per_month_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        try:
            if row[1] != "":
                yield row[1].split()[0].split('-')[1]
        except:
            pass


def get_trip_per_month_bracket_chi():
    trip_month = trip_per_month.map(lambda row: (row, 1)).reduceByKey(add)
    trip_month = trip_month.collect()
    st = filter(lambda x: x[0].isdigit(), trip_month)
    st = sorted(st, key=lambda x: int(x[0]))
    return st


    # d = {}
    #
    # for trip in trip_per_month.collect():
    #     if trip not in d:
    #         d[trip] = 0
    #     d[trip] += 1
    #
    # # print time_dict
    #
    # l = []
    # for key, value in d.items():
    #     l.append((key, value))
    #
    # l = sorted(l, key=lambda x: int(x[0]))
    # return l


#-------
def get_trip_date_chi(pId, lines):
    import csv
    if pId == 0:
        next(lines)
    for row in csv.reader(lines):
        if row[1] != "":
            try:
                yield row[1].split()[0].split('-')[1] + '-' + row[1].split()[0].split('-')[2]
            except:
                pass


def get_highest_trip_day_chi(num_of_days):
    dates = trip_date.map(lambda row: (row, 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[1]), reverse=True)
    return dates

    # trip_dates = {}
    # for trip in trip_date.collect():
    #     if trip not in trip_dates:
    #         trip_dates[trip] = 0
    #     trip_dates[trip] += 1
    #
    # l = []
    # for key, value in trip_dates.items():
    #     l.append((key, value))
    #
    # # l = sorted(l,key=itemgetter(1),reverse=True)
    # l = sorted(l, key=lambda x: int(x[1]), reverse=True)
    # return l[:num_of_days]


def get_lowest_trip_day_chi(num_of_days):
    dates = trip_date.map(lambda row: (row, 1)).reduceByKey(add)
    dates = dates.collect()
    dates = filter(lambda x: x[0].isdigit(), dates)
    dates = sorted(dates, key=lambda x: int(x[1]))
    return dates

    # trip_dates = {}
    # for trip in trip_date.collect():
    #     if trip not in trip_dates:
    #         trip_dates[trip] = 0
    #     trip_dates[trip] += 1
    #
    # l = []
    # for key, value in trip_dates.items():
    #     l.append((key, value))
    #
    # l = sorted(l, key=lambda x: int(x[1]))
    # return l[:num_of_days]




if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    sc = SparkContext()

    spark = SparkSession(sc)

    #chi_taxi = sc.textFile('chicago_taxi_trips_2016_01.csv')
    # #chi_taxi = sc.textFile('/user/tlee000/chicago_taxi_trips_2016_01.csv')

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
    chi_taxi = sc.union([taxi01, taxi02, taxi03, taxi04, taxi05,
                        taxi06, taxi07, taxi08, taxi09, taxi10,
                        taxi11, taxi12])


    chi_taxi_sec = chi_taxi.mapPartitionsWithIndex(get_chi_taxi_trip_sec).cache()
    chi_taxi_sec = chi_taxi_sec.filter(lambda x: x.isdigit())
    chi_taxi_sec.take(10)

    chi_taxi_tripmiles = chi_taxi.mapPartitionsWithIndex(get_chi_taxi_trip_miles).cache()
    chi_taxi_tripmiles = chi_taxi_tripmiles.filter(lambda x: is_number(x))
    chi_taxi_tripmiles.take(10)

    # print(get_chi_taxi_trip_avg_sec())
    # print(get_chi_taxi_avg_mile())
    # print(get_avg_mile_per_hour_chi())
    print("time traveled:", get_time_traveled_bracket_chi())
    print("distance traveled: ",get_dist_traveled_bracket_chi())

    companies = chi_taxi.mapPartitionsWithIndex(get_distinct_companies_chi).cache()
    companies = companies.distinct()
    #print(companies.count())
    companies.take(10)

    companies_business = chi_taxi.mapPartitionsWithIndex(get_company_business_record_chi).cache()
    #print(companies_business.count())
    companies_business.take(10)

    print("company business bracket: ", get_company_business_bracket())

    start_time = chi_taxi.mapPartitionsWithIndex(get_trip_start_time_chi).cache()
    #print("start time count: ", start_time.count())
    #print(type(start_time.take(10)[0]))
    start_time.take(10)

    print("taxi time start:", get_taxi_time_start_bracket_chi())

    trip_per_month = chi_taxi.mapPartitionsWithIndex(get_trip_per_month_chi).cache()
    trip_per_month.take(10)

    print("get_trip_per_month_bracket_chi: ", get_trip_per_month_bracket_chi())

    average_trip_a_day = chi_taxi.count() / 366
    print("average_trip_a_day: ", average_trip_a_day)

    trip_date = chi_taxi.mapPartitionsWithIndex(get_trip_date_chi).cache()
    trip_date.take(10)

    print("get_highest_trip_day_chi 5: ", get_highest_trip_day_chi(5))
    print("get_lowest_trip_day_chi 5: ", get_lowest_trip_day_chi(5))

    print("success")


