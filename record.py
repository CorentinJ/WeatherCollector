from station import Station
from datetime import date, timedelta, datetime
import matplotlib.pyplot as plt
import pathos.pools as pools
import numpy as np
import warnings
from os import path
from google.cloud import bigquery

# Gathers weather data for a given location and time range
class Record:
    # IMPORTANT: multithreading on windows requires code to be inside of
    #   if __name__ == '__main__':
    #       ...
    # otherwise it will recursively run the entire code
    multithreaded = True    
    date_format = "%Y%m%d"  # YYYYMMDD
    attributes = ['min',        # Min temperature F°
                  'temp',       # Mean temperature F°
                  'max',        # Max temperature F°
                  'wdsp',       # Mean wind speed
                  'mxspd',      # Max wind speed (sustained)
                  #'gust',       # Max wind speed (instant) - IGNORED
                  'prcp',       # Total precipitation
                  'dewp',       # Mean dew point F°
                  'sndp',       # Snow depth
                  'stp',        # Mean station pressure
                  'slp',        # Mean sea level pressure
                  'visib',      # Mean visibility
                  'fog',        # Presence of fog
                  'rain',       # Presence of rain
                  'snow',       # Presence of snow
                  'hail',       # Presence of hail
                  'thunder',    # Presence of thunder
                  'tornado']    # Presence of a tornado
    max_station_distance = 50.0 # Distance in km after which a station has no weight
    
    def __init__(self, name, shape, start_date, end_date=None, contour_dist=25, max_stations=12,
                 interactive=False, save_plot=False):
        """
        :param name: The name of the location
        :param shape: The (multi)polygon describing the shape of the location
        :param start_date: Beginning of the record period (YYYYMMDD)
        :param end_date: End of the record period. If None, the day before the current day is used.
        :param contour_dist: Range within which weather station data is to be trusted.
        :param max_stations: Maximum number of stations used to gather data
        :param interactive: Whether or not to display a plot of the stations
        :param save_plot: Whether or not to save this plot to disk
        """
        self.name = name
        self.shape = shape
        self.contour_dist = contour_dist
        if contour_dist > Record.max_station_distance:
            warnings.warn('contour distance of %f is greater than maximum allowed distance of %f' %
                          (contour_dist, Record.max_station_distance))
        self.max_stations = max_stations
        self.start_date = datetime.strptime(start_date, Record.date_format).date()
        if end_date is None:
            self.end_date = date.today() - timedelta(days=1)
        else:
            self.end_date = datetime.strptime(end_date, Record.date_format).date()
        self.interactive = interactive
        self.save_plot = save_plot
        
        # Gather the data and build the record
        self.collection_info = ""
        self.__build()
    
    def __log(self, message):
        self.collection_info += "# " + message.replace("\n", "\n# ") + "\n"
    
    def __plot_stations(self, closest_stations):
        shapes = self.shape if self.shape.geom_type == 'MultiPolygon' else [self.shape]
        for sub_shape in shapes:
            x, y = sub_shape.exterior.xy
            plt.plot(x, y, c='blue')
    
        for station, distance in closest_stations:
            plt.plot(station.longitude, station.latitude, 'o', c='red')
            plt.text(station.longitude, station.latitude, station.name + ("\n%.1fkm" % distance))
    
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        plt.title('Weather stations in ' + self.name)
        
        if self.save_plot:
            plt.savefig(self.name + '.png')
        if self.interactive:
            plt.show()
    
    def __build(self):
        self.__log(self.name.upper())
        
        print("Gathering data from " + self.name +
              " from " + self.start_date.strftime("%d/%m/%Y") +
              " to " + self.end_date.strftime("%d/%m/%Y"))
        self.__log("From " + self.start_date.strftime("%d/%m/%Y") +
              " to " + self.end_date.strftime("%d/%m/%Y") + "\n")
        
        # Retrieve stations within <contour_dist> that are overlapping with the time range
        closest_stations = Station.find_stations_in_geometry(
            self.shape,
            self.contour_dist,
            self.start_date,
            self.end_date
        )
    
        if len(closest_stations) == 0:
            print("Found no stations close enough! Try increasing the search radius.")
            return
        self.max_stations = min(self.max_stations, len(closest_stations))
        
        # Prune unnecessary stations
        print("Using the first %d of the %d stations found within %dkm:" %
              (self.max_stations, len(closest_stations), self.contour_dist))
        closest_stations = closest_stations[:self.max_stations]
        stations, distances = zip(*closest_stations)
        
        # Display them
        self.__log("STATIONS:")
        base_weights = [Record.distance_weight(distance) for distance in distances]
        base_weights = np.array(base_weights)
        for (station, distance), weight in zip(closest_stations, base_weights):
            message = station.name[:20].ljust(20) + (" at %.2fkm" % distance).ljust(14) + \
                      "(trust: %.2f%%)" % (weight * 100)
            print(message)
            self.__log(message)
        if self.interactive or self.save_plot:
            self.__plot_stations(closest_stations)
            
        # Gather data
        self.data = []
        if Record.multithreaded:
            thread_pool = pools.ProcessPool(4)
        date = self.start_date
        year = -1
        while date <= self.end_date:
            if year != date.year:
                # Retrieve data from the year <year> for each station
                year = date.year
                print("\nCollecting data for the year %d" % year)
                if Record.multithreaded:
                    job = lambda station: station.retrieve_obs(year)
                    all_yearly_data = thread_pool.map(job, stations)
                else:
                    all_yearly_data = [station.retrieve_obs(year) for station in stations]
            
            # Gather daily data
            date_key = date.strftime(Record.date_format)
            daily_data, daily_trusts = [], []
            for yearly_data, trust in zip(all_yearly_data, base_weights):
                if yearly_data is None:
                    continue
                if date_key in yearly_data:
                    daily_data.append(yearly_data[date_key])
                    daily_trusts.append(trust)
                    
            # Case of missing data for the entire day
            if len(daily_data) == 0:
                print("Got no data for " + str(date))
                
            # Populate attributes
            datum = {}
            for attribute in Record.attributes:
                # Perform a weighted average
                value = 0
                total_weight = 0
                for daily_datum, daily_trust in zip(daily_data, daily_trusts):
                    if daily_datum[attribute] is None:
                        continue
                    value += daily_datum[attribute] * daily_trust
                    total_weight += daily_trust
                    
                if total_weight == 0:
                    datum[attribute] = None
                    continue

                value = value / total_weight
                datum[attribute] = value
            
            # Add datum to record
            self.data.append((date, datum))
            
            date += timedelta(days=1)

    @staticmethod
    def distance_weight(distance):
        """
        Computes a coefficient of trust based on the distance between the observations and the 
        target location.
        :param distance: [float] The distance in kilometers
        :return: [float] The coefficient of trust (within [0 - 1])
        """
        # A plot of the function can be found here: https://i.imgur.com/QjzAT18.png
        if distance >= Record.max_station_distance:
            return 0
        return 1 - (distance / Record.max_station_distance) ** 2
    
    def export_as_csv(self, filepath):
        just_size = 11
        csvfile = open(filepath, 'w')
        
        # Write the header
        csvfile.write(self.collection_info + "\n")
        csvfile.write("DATE,".ljust(just_size))
        for attribute in Record.attributes:
            attribute = attribute.upper() + ("," if attribute != Record.attributes[-1] else "") 
            csvfile.write(attribute.ljust(just_size))
        csvfile.write("\n")
        
        # Write each row
        for date, datum in self.data:
            csvfile.write((date.strftime(Record.date_format)+ ", ").ljust(just_size))
            
            for attribute in Record.attributes:
                if not attribute in datum:
                    raise Exception('Corrupted record data')
                
                if datum[attribute] is None:
                    text = "NA"
                else:
                    text = "%.3f" % datum[attribute]
                    
                if attribute != Record.attributes[-1]:
                    text += ", "
                    
                csvfile.write(text.ljust(just_size))
            csvfile.write("\n")
            
        print("\nSuccesfully written " + filepath + "!")
        csvfile.close()
        
    def export_in_bigquerry(self):
        access_key = path.realpath("../../access_key.json")
        bigquery_client = bigquery.Client.from_service_account_json(access_key)
        dataset_ref = bigquery_client.dataset("weather")
        table_ref = bigquery_client.get_table(dataset_ref.table("noaa"))

        data = []
        for date, datum in self.data:
            entry = {
                "city": self.name,
                "date": date.strftime("%Y-%m-%d")
            }

            for attribute in Record.attributes:
                if not attribute in datum:
                    raise Exception("Corrupted record data")

                if datum[attribute] is None:
                    entry[attribute] = None 
                else:
                    entry[attribute] = "%.3f" % datum[attribute]
            
            data.append(entry)

        result = bigquery_client.create_rows(table_ref, data)
        if  result != []:
            print("Error storing weather")
            print(result)
        else:
            print("Storage successful")


    @staticmethod
    def read_from_csv(filepath):
        csvfile = open(filepath, 'r')
        
        # Skip header
        for line in csvfile:
            line = line.rstrip()
            if line != "" and line[0] != '#':
                break
                
        # Match row names
        csv_rownames = line.lower().replace(" ", "").split(',')
        true_rownames = ['date'] + Record.attributes
        if csv_rownames != true_rownames:
            raise Exception('\nHeader mismatch:\n' + str(csv_rownames) + 
                            '\n\nAgainst:\n' + str(true_rownames))
        
        # Read contents
        data = []
        for line in csvfile:
            text_datum = line.rstrip().replace(" ", "").split(',')
            date = datetime.strptime(text_datum[0], Record.date_format).date()
            values = [None if value == 'NA' else float(value) for value in text_datum[1:]]
            datum = {attribute: value for attribute, value in zip(Record.attributes, values)}
            data.append((date, datum))
            
        csvfile.close()
        return data
