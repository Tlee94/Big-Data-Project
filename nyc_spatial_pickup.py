import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext

import fiona
import fiona.crs
import shapely
import rtree
import pandas as pd
import geopandas as gpd

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex(shapefile)

    if pid==0:
        records.next()
    reader = csv.reader(records)
    counts = {}

    for row in reader:
        p = geom.Point(proj(float(row[5]), float(row[6])))
        zone = findZone(p, index, zones)
        if zone:
            counts[zone] = counts.get(zone, 0) + 1
    return counts.items()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf8')

    sc = SparkContext()
    spark = SparkSession(sc)

    # Read Shapefile
    shapefile = '/user/tlee000/geo_export_f3f1c046-b582-4f97-980f-48e4f9bcfb7f.shp'
    boros = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))

    rdd = sc.textFile('/user/tlee000/2016_Green_Taxi_Trip_Data.csv')
    counts = rdd.mapPartitionsWithIndex(processTrips)
    counts = counts.reduceByKey(lambda x,y: x+y)
    counts = counts.collect()
    print(counts)
