from csvtable import CsvTable
from csvtable import cache_dir
from urllib import request
from urllib.error import HTTPError, URLError
from datetime import datetime
import numpy as np
import os
import gzip
import time
from shapely.geometry.point import Point

class Station:
    time_format = "%Y%m%d"  # YYYYMMDD
    
    def __init__(self, d):
        self.name = d["stationname"]
        self.usaf = d["usaf"]           # Air Force station ID (this is a string)
        self.wban = d["wban"]           # NCDC WBAN number (also a string)
        self.icao = d["icao"]           # ICAO ID
        self.country = d["ctry"]        # Country
        self.state = d["state"]         # State for US stations
        self.latitude = d["lat"]        # Latitude in thousandths of decimal degrees
        self.longitude = d["lon"]       # Longitude in thousandths of decimal degrees
        self.elevation = d["elevm"]     # Elevation in meters
        
        # Start period of record (YYYYMMDD)
        self.record_start = datetime.strptime(d["begin"], Station.time_format).date()
        # End period of record (YYYYMMDD)
        self.record_end = datetime.strptime(d["end"], Station.time_format).date()      

    # Courtesy of https://andrew.hedges.name/experiments/haversine/
    @staticmethod
    def distance(lat1, long1, lat2, long2):
        dlat = lat2 - lat1
        dlon = long2 - long1
        a = np.square(np.sin(np.deg2rad(dlat / 2))) + \
            np.cos(np.deg2rad(lat2)) * \
            np.cos(np.deg2rad(lat1)) * \
            np.square(np.sin(np.deg2rad(dlon / 2)))
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        return c * 6373  # Radius of the earth in kilometers
    
    def distance_from(self, latitude, longitude):
        return Station.distance(self.latitude, self.longitude, latitude, longitude)

    def retrieve_obs(self, year):
        # Ensure the year is within this station's recording range
        if year < self.record_start.year or year > self.record_end.year:
            print("Station " + self.usaf + " has no observations for %d." % year)
            return None

        # Find the url and filename
        filename = self.usaf + "-" + self.wban + "-" + str(year) + ".op.gz"
        filepath = os.path.join(cache_dir, filename)
        if os.path.exists(filepath):
            age = int(time.time() - os.path.getmtime(filepath))
            current_year = int(time.strftime("%Y"))
            if year == current_year and age > 24 * 3600:
                # Re-download observation for the current year if they are over 24 hours old
                print("Cached file " + filename + " is outdated.")
            else:
                print("File " + filename + " found in cache.")
                return Station.parse_gsod_data(filepath)

        # Retrieve the .op file
        url = "https://www1.ncdc.noaa.gov/pub/data/gsod/" + str(year) + "/" + filename
        print("Downloading " + filename + "...", end=' ')
        try:
            request.urlretrieve(url, filepath)
        except HTTPError as err:
            if err.code == 404:
                print("Failed: does not exist")
            else:
                print("Failed with HTTP code %d" % err.code)
            return None
        except URLError:
            print("Name could not be resolved, server is likely down (again)")
            raise Exception("Gotta wait a bit")
        print("Succeeded.")

        return None if filepath is None else Station.parse_gsod_data(filepath)

    # See ftp://ftp.ncdc.noaa.gov/pub/data/gsod/GSOD_DESC.txt
    @staticmethod
    def parse_gsod_data(op_filepath):
        # Read the archive
        gz_reader = gzip.GzipFile(op_filepath, 'rb')
        contents = gz_reader.read().decode("utf-8")
        gz_reader.close()
    
        # Parse the data (we have to use the indices here because .op files are formatted by 
        # character alignment and not with separators like .csv files)
        dates = []
        data = []
        for line in contents.split("\n")[1:]:
            if line == "": 
                continue
            
            dates.append(line[14:22])
            datum = {
                "temp": float(line[24:30]),
                "dewp": float(line[35:41]),
                "slp": float(line[46:52]),
                "stp": float(line[57:63]),
                "visib": float(line[68:73]),
                "wdsp": float(line[78:83]),
                "mxspd": float(line[88:93]),
                "gust": float(line[95:100]),
                "max": float(line[102:108]),
                "min": float(line[110:116]),
                "prcp": float(line[118:123]),
                "sndp": float(line[125:130]),
                "fog": bool(int(line[132])),
                "rain": bool(int(line[133])),
                "snow": bool(int(line[134])),
                "hail": bool(int(line[135])),
                "thunder": bool(int(line[136])),
                "tornado": bool(int(line[137])),
            }
            
            # Deal with missing values
            for attribute in ["temp", "dewp", "slp", "stp", "max", "min"]:
                if datum[attribute] == 9999.9:
                    datum[attribute] = None
            for attribute in ["visib", "wdsp", "mxspd", "gust", "sndp"]:
                if datum[attribute] == 999.9:
                    datum[attribute] = None
                    
            # Special flag for precipitations
            if line[123] == 'I' or datum["prcp"] == 99.99:
                datum["prcp"] = None
            if not datum["rain"] and datum["prcp"] is None:
                datum["prcp"] = 0.0

            # Special flag for the snow
            if not datum["snow"] and datum["sndp"] is None:
                datum["sndp"] = 0.0

            data.append(datum)
            
        return dict((date, datum) for (date, datum) in zip(dates, data))
        
    def get_key(self):
        return Station.as_key(self.usaf, self.wban)

    @staticmethod
    def as_key(usaf, wban):
        return usaf + str(wban)

    def is_valid(self):
        return self.usaf and self.wban and self.longitude and self.latitude

    @staticmethod
    def get_stations(start_date=None, end_date=None):
        # Filter stations that have no observation within the time range
        stations = list(station_table.values())
            
        if start_date is not None:
            stations = [station for station in stations if station.record_end > start_date]
        if end_date is not None:
            stations = [station for station in stations if station.record_start < end_date]
            
        return stations
            
    @staticmethod
    def find_closest_stations(latitude, longitude, max_dist=None, start_date=None, end_date=None):
        stations = Station.get_stations(start_date, end_date)
            
        # Evaluate the distance with all stations
        distances = np.array([station.distance_from(latitude, longitude) for 
                              station in stations])

        # Sort stations based on their distance
        closest = list(zip(stations, distances))
        closest.sort(key=lambda x: x[1])

        # Remove stations that are too far away
        if max_dist is not None:
            closest = closest[:np.sum(distances <= max_dist)]

        # Return the sorted stations and distances
        return closest
    
    @staticmethod
    def find_stations_in_geometry(shape, contour_dist=0, start_date=None, end_date=None):
        stations = Station.get_stations(start_date, end_date)
        
        # For performance purposes, find a cutoff distance beyond which stations are ignored
        center = shape.centroid
        hull_points = [Point(x, y) for x, y in zip(*shape.convex_hull.exterior.xy)]
        furthest_point = max(hull_points, key=lambda x: center.distance(x))
        max_dist = Station.distance(center.y, center.x, furthest_point.y, furthest_point.x)
        max_dist += contour_dist

        # Evaluate the distance with all stations
        shapes = shape if shape.geom_type == 'MultiPolygon' else [shape]
        distances = []
        for station in stations:
            # First get an approximate distance from the centroid
            distance_approx = station.distance_from(center.y, center.x)
            if distance_approx > max_dist:
                distances.append(None)
                continue
            
            # Points inside the borders have a distance of 0
            station_point = Point(station.longitude, station.latitude)
            if any(sub_shape.contains(station_point) for sub_shape in shapes):
                distances.append(0)
                continue
            
            # Otherwise, evaluate the real distance from the region borders
            distance = 99999
            for sub_shape in shapes:
                exterior = sub_shape.exterior
                projection = exterior.interpolate(exterior.project(station_point))
                distance = min(distance, station.distance_from(projection.y, projection.x))
            distances.append(distance if distance < contour_dist else None)
        distances = np.array(distances)

        # Sort stations based on their distance
        closest = [(station, distance) for station, distance in zip(stations, distances) if
                   distance is not None]
        closest.sort(key=lambda x: x[1])

        # Return the sorted stations and distances
        return closest


# See ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt
station_table = CsvTable("ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv",
                format=[str] * 6 + [float] * 3 + [str] * 2,
                entry_type=Station,
                key=Station.get_key)

