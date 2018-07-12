from urllib import request
import json
import console
import webbrowser
import shapely.geometry

# Documentation of the API can be found at:
# http://wiki.openstreetmap.org/wiki/Nominatim
def query(search_string, interactive=False, result_count=5):
    """
    Attempts to convert a string query into geographic coordinates.

    Parameters
    ----------
    search_string : str
        The string query. Supports fairly standard requests in alphanumeric format (spaces
        allowed). Examples of requests:
            "london"
            "67 washington street"
            "la baguette magique sart tilman"
    interactive: bool
        If true, lets the user preview locations and validate them. Otherwise, the most likely
        location is always selected.
    result_count: int
        The limit in the number of locations considered. Only used in interactive mode.
    """

    # Retrieve guesses of location
    search_string = search_string.replace(' ', '+')
    url = "http://nominatim.openstreetmap.org/search?q=" + search_string + "&format=json&limit=" + \
          str(result_count) + "&polygon_geojson=1"
    page_data = request.urlopen(url).read().decode('ascii', 'ignore')
    if page_data == "[]":
        if interactive:
            print('Could not find anything for \"' + search_string + "\"")
        return None
    cities = json.loads(page_data)
    
    # Filter results that aren't areas
    cities = [city for city in cities if city["geojson"]["type"] != 'Point']
    if len(cities) == 0:
        print('Found no matching area')
        return None

    # Verify the answer by the user if interactive is enabled. Otherwise take the first result.
    if interactive:
        for i, city in enumerate(cities):
            print('Location name: ' + city["display_name"][:90])
            correct = console.query_yes_no("\nDoes that seem like the right place? (say no if "
                                           "you're not 100% sure)")
            if correct:
                break

            open_in_browser = console.query_yes_no("Should I open the details of the location in "
                                                   "your browser?", default='no')
            if open_in_browser:
                url = "http://nominatim.openstreetmap.org/details.php?place_id=" + city["place_id"]
                webbrowser.open_new_tab(url)
                correct = console.query_yes_no("Is that correct?")
                if correct:
                    break

            if i + 1 == len(cities):
                print("No more locations available.")
                return None
            print("\nMoving on to the next location:")
    else:
        city = cities[0]
        print('Assuming ' + city["display_name"][:90])

    return shapely.geometry.shape(city["geojson"])