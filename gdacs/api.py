from os.path import join
from cachetools import cached, TTLCache
from gdacs.schemas import GeoJSON
from gdacs.utils import *
from pygeoif.factories import from_wkt


CACHE_TTL = 300  # secs
EVENT_TYPES = [None, 'TC', 'EQ', 'FL', 'VO', 'DR', 'WF']
DATA_FORMATS = [None, 'xml', 'geojson', 'shp']
ALERT_LEVELS = [None, 'green', 'orange', 'red']
BASE_URL = "https://www.gdacs.org/datareport/resources"
API_BASE_URL = "https://www.gdacs.org/gdacsapi"
LATEST_EVENTS_URL = f"{API_BASE_URL}/api/events/geteventlist/latest"
LATEST_EVENTS_URL_4APP = f"{API_BASE_URL}/api/events/geteventlist/EVENTS4APP"
EVENTS_BY_AREA_URL = f"{API_BASE_URL}/api/events/geteventlist/eventsbyarea"
EVENTS_DATA_URL = f"{API_BASE_URL}/api/events/geteventdata"



class GDACSAPIReader:
    def __init__(self):
        pass

    def __repr__(self) -> str:
        return "GDACS API Client."

    @cached(cache=TTLCache(maxsize=500, ttl=CACHE_TTL))
    def latest_events_4app(self,
                      event_type: str = None,
                      limit: int = None
                      ):
        """ Get latest events from GDACS RSS feed. """
        if event_type not in EVENT_TYPES:
            raise GDACSAPIError("API Error: Used an invalid `event_type` parameter in request.")

        res = requests.get(LATEST_EVENTS_URL_4APP)
        if res.status_code != 200:
            raise GDACSAPIError("API Error: GDACS RSS feed can not be reached.")

        events = [
            event for event in res.json()['features']
            if event_type in [None, event['properties']['eventtype']]
        ]
        features = json.loads(json.dumps(events[:limit]))
        return GeoJSON(features=features)

    @cached(cache=TTLCache(maxsize=500, ttl=CACHE_TTL))
    def latest_events(self,
                      event_list: str = None,
                      alert_level: str = None,
                      date_modified: str = None,
                      country: str = None,
                      severity: int = None,
                      page_size: int = 100,
                      page_number: int = 1,
                      ):
        """
        Fetches the latest events based on provided filters and caching enabled for performance. This function is decorated
        with a caching mechanism that stores the results with a time-to-live (TTL) duration and a maximum capacity limit
        on the cache.

        Args:
            event_list (str, optional): A comma-separated list specifying event types to filter.
            alert_level (str, optional): The level of alert to filter events by.
            date_modified (str, optional): The last modification date of events to filter in ISO-8601 format.
            country (str, optional): The specific country for which to retrieve events.
            severity (float, optional): Numerical severity level to filter events by.
            page_size (int, optional): The number of records to fetch per page. Defaults to 100.
            page_number (int, optional): The specific page of records to fetch. Defaults to 1.

        Returns:
            GeoJSON: The fetched list of events along with associated metadata, filtered as per provided inputs.

        Raises:
            ValueError: Raised if the input parameters are invalid or do not conform to
            required formats.
            GDACSAPIError: If the API request fails or returns an error.
        """

        if event_list:
            event_types = [et.strip() for et in event_list.split(',')]
            for et in event_types:
                if et not in EVENT_TYPES:
                    raise ValueError(f"API Error: Invalid event type '{et}' in event_list")

        if alert_level and alert_level not in ALERT_LEVELS:
            raise ValueError(f"API Error: Invalid alert level '{alert_level}' in alert_level")

        # Build query parameters
        params = {
            'eventlist': event_list,
            'alertlevel': alert_level,
            'datemodified': date_modified,
            'country': country,
            'severity': severity,
            'pagesize': page_size,
            'pagenumber': page_number
        }

        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        # Make API request
        res = requests.get(LATEST_EVENTS_URL, params=params)
        if res.status_code == 204:
            return GeoJSON(features=[])
        if res.status_code != 200:
            raise GDACSAPIError("API Error: GDACS API can not be reached.")

        return GeoJSON(features=res.json()['features'])


    @cached(cache=TTLCache(maxsize=500, ttl=CACHE_TTL))
    def get_events_by_area(self,
                           geometry_area: str,
                           days: int = None
                           ):
        """
        Retrieve environmental events for a specified geographic area over a given number
        of days. This function employs caching to enhance performance and reduce redundant
        computations or data fetches.

        Args:
        geometry_area: A string representation of the geographic area for which events
            are to be retrieved in WKT format.
        days: Optional integer value for the number of past days to include in the query.
            If not provided, a default behavior is applied according to implementation.

        Returns:
        A list of events related to the specified geographic area within the specified
        time range.

        Raises:
        ValueError: Raised if the input parameters are invalid or do not conform to
            required formats.
        GDACSAPIError: If the API request fails or returns an error.
        """
        # validate that geometry_area is a valid WKT string
        try:
            from_wkt(geometry_area)
        except (AttributeError, TypeError) as e:
            raise ValueError(f"API Error: Invalid geometry_area '{geometry_area}'") from e
        except Exception as e:
            raise GDACSAPIError(f"API Error: {e}") from e

        params = {
            'geometryArea': geometry_area,
            'days': days
        }

        res = requests.get(EVENTS_BY_AREA_URL, params=params)
        if res.status_code == 204:
            return GeoJSON(features=[])
        if res.status_code != 200:
            raise GDACSAPIError("API Error: GDACS API can not be reached.")
        return GeoJSON(features=res.json()['features'])


    @cached(cache=TTLCache(maxsize=500, ttl=CACHE_TTL))
    def get_events_data(self,
                        event_id: int,
                        event_type: str,
                        source: str = None,
                        ):
        """ Get data of a single event from GDACS API. """
        if event_type not in EVENT_TYPES:
            raise GDACSAPIError("API Error: Used an invalid `event_type` parameter in request.")

        params = {
            'eventid': event_id,
            'eventtype': event_type,
            'source': source,
        }
        # Remove None values
        params = {k: v for k, v in params.items() if v is not None}

        res = requests.get(EVENTS_DATA_URL, params=params)
        if res.status_code == 404:
            raise GDACSAPIError("API Error: Event ID not found.")
        if res.status_code != 200:
            raise GDACSAPIError("API Error: GDACS API can not be reached.")
        return GeoJSON(features=res.json()['features'])


    @cached(cache=TTLCache(maxsize=500, ttl=CACHE_TTL))
    def get_event(self,
                  event_id: str,
                  event_type: str = None,
                  episode_id: str = None,
                  source_format: str = None,
                  cap_file: bool = False
                  ):
        """ Get record of a single event from GDACS API. """
        if event_type not in EVENT_TYPES:
            raise GDACSAPIError("API Error: Used an invalid `event_type` parameter in request.")

        if source_format not in DATA_FORMATS:
            raise GDACSAPIError("API Error: Used an invalid `data_format` parameter in request.")

        if source_format == 'geojson':
            return self.__get_geojson_event(event_type, event_id, episode_id)
        elif source_format == 'shp':
            return self.__get_shp_event(event_type, event_id, episode_id)
        else:
            return self.__get_xml_event(event_type, event_id, episode_id, cap_file)

    def __get_geojson_event(self, event_type: str, event_id: str, episode_id: str = None):
        file_name = f"geojson_{event_id}_{episode_id}.geojson"
        geojson_path = join(BASE_URL, event_type, event_id, file_name).replace("\\", "/")
        return handle_geojson(geojson_path)        

    def __get_shp_event(self, event_type: str, event_id: str, episode_id: str = None):
        file_name = f"Shape_{event_id}_{episode_id}.zip"
        shp_path = join(BASE_URL, event_type, event_id, file_name).replace("\\", "/")
        return download_shp(shp_path)

    def __get_xml_event(self, event_type: str, event_id: str, episode_id: str = None, cap_file: bool = False):
        if cap_file:
            file_name = f"cap_{event_id}.xml"
        elif not episode_id:
            file_name = f"rss_{event_id}.xml"
        else:
            file_name = f"rss_{event_id}_{episode_id}.xml"

        xml_path = join(BASE_URL, event_type, event_id, file_name).replace("\\", "/")
        return handle_xml(xml_path)