import math
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


def get_haversine_distance(lat1, long1, lat2, long2):
    earth_radius = 6371
    lat1, long1, lat2, long2 = map(math.radians, [lat1, long1, lat2, long2])
    delta_lat = lat2 - lat1
    delta_long = long2 - long1
    area = math.sin(delta_lat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(delta_long/2)**2
    circumference = 2 * math.asin(math.sqrt(area))
    return earth_radius * circumference

haversine_distance = udf(get_haversine_distance, DoubleType())