import os
import csv
from urllib import request
from collections.abc import Mapping
import _pickle as cPickle

cache_dir = 'cache'

"""
A read-only dictionary built after a given csv file. Implements all methods a read-only
dictionary exposes, so use it as such.
"""
class CsvTable(Mapping):
    def __init__(self, source, format, entry_type=None, headers=None, 
                 key=None):
        """
        Parameters
        ----------
        source : str
            Source path of the text file containing the data. Can be a URL.
        format: tuple of types
            Specifies the data format. e.g.: (string, float, int, int).
        entry_type: class
            Type of the stored entries. If this is set, a new object of that type will be created
            with a dictionary of the data as only argument. Otherwise, the dictionary itself is
            the entry. Each dictionary has for keys the headers and for values the row data cast
            to the types specified in <format>. If the class implements a function is_valid(self)
            that returns a boolean, it will be used to filter invalid entries.
        headers: [string]
            If provided, overrides the headers found in the csv file.
        key: lambda entry: key
            The key by which objects will be indexed in the table. If set to None, the entries
            will be referenced by their index.
        """
        self.source = source
        self.filename = os.path.basename(source)
        self.filepath = os.path.join(cache_dir, self.filename)
        self.cached_filename = self.filename[:self.filename.rfind('.')] + \
            '.obj'
        self.cached_filepath = os.path.join(cache_dir, self.cached_filename)
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)
        self.format = format
        self.entry_type = entry_type
        self.headers = headers
        self.key = key
        self.__table = dict()

    def __make_local_copy(self):
        """
        Retrieves the file from the location specified at <source> and makes
        a local copy.
        """
        # Verifiy the file does not already exist
        if os.path.exists(self.filepath):
            last_updated_time = os.path.getctime(self.filepath)
            return

        # Retrieve and save the file
        print("Retrieving file from " + self.source + "...")
        try:
            # The source is a URL
            request.urlretrieve(self.source, self.filepath)
            print("Downloaded and saved " + self.filename)
        except ValueError:
            # The source is a file path
            os.copy(self.source, self.filepath)

    @staticmethod
    def __format_name(colname):
        colname = colname.replace(' ', '_').lower()
        return "".join([char for char in colname if char.isalpha()])

    @staticmethod
    def __datum_as_type(dtype, datum):
        return None if datum == "" else dtype(datum)

    def build(self):
        """
        Effectively builds the table from the csv file if there is no cached
        copy of it, otherwise the cached version is loaded.
        """
        # Check if the table is not in cache first
        if os.path.exists(self.cached_filepath):
            print("Loading cached table" + self.cached_filename + "...")
            with open(self.cached_filepath, "rb") as cached_table:
                self.__table = cPickle.load(cached_table)
            return

        # Make a local copy of the file
        print("Found no cached copy of the table, creating it now.")
        self.__make_local_copy()

        # Build the table
        print("Parsing data from " + self.filename + "...")
        with open(self.filepath, 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='\"')

            # Parse the headers
            if self.headers is None:
                self.headers = [CsvTable.__format_name(colname) for colname in next(reader)]
            else:
                next(reader)

            # Create entries from the rows
            for i, row in enumerate(reader):
                data = [CsvTable.__datum_as_type(dtype, datum) for dtype, datum
                        in zip(self.format, row)]
                named_data = dict(zip(self.headers, data))

                if self.entry_type is not None:
                    entry = self.entry_type(named_data)
                else:
                    entry = named_data

                if hasattr(entry, 'is_valid'):
                    if not entry.is_valid():
                        continue

                if self.key is None:
                    self.__table[i] = entry
                else:
                    self.__table[self.key(entry)] = entry

        # Save the table in the disk cache
        with open(self.cached_filepath, "wb") as cached_table:
            cPickle.dump(self.__table, cached_table)

    def __getitem__(self, key):
        if len(self.__table) == 0:
            self.build()
        return self.__table[key]

    def __len__(self):
        return len(self.__table)

    def __iter__(self):
        if len(self.__table) == 0:
            self.build()
        return self.__table.__iter__()
