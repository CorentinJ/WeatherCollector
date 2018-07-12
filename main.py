import sys
from record import Record
import geocoding


if __name__ == '__main__':
    print("Enter the name of the location to gather data from: ", end="")
    location_name = input().lower()
    shape = geocoding.query(location_name, interactive=True)

    print("Enter the start date (YYYYMMDD, e.g. 20160330): ", end="")
    start_date = input()
    print("Enter the end date (YYYYMMDD) (optional): ", end="")
    end_date = input()
    end_date = None if end_date == "" else end_date
    print("Enter the maximal search distance (default: 25) (optional): ", end="")
    contour_dist = input()
    contour_dist = 25 if contour_dist == "" else int(contour_dist)
    print("Enter the maximal number of stations (default: 12) (optional): ", end="")
    max_stations = input()
    max_stations = 12 if max_stations == "" else int(max_stations)

    record = Record(location_name, shape, start_date, end_date, contour_dist, max_stations,
                    interactive=True)
    record.export_as_csv("weather_data_" + location_name + ".csv")