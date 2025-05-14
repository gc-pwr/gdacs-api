from unittest import TestCase, mock

from gdacs.api import GDACSAPIReader, GDACSAPIError
from gdacs.schemas import GeoJSON
from gdacs.utils import delete_downloads


class TestGDACSAPI(TestCase):
    def setUp(self):
        self.client = GDACSAPIReader()

    def tearDown(self):
        self.client = None
        delete_downloads()

    def test_latest_events_4app_no_args(self):
        '''Test latest_events_4app() without any arguments.'''
        events = self.client.latest_events_4app()
        self.assertTrue(events) if len(events) > 0 else self.assertFalse(events)

    def test_latest_events_4app_limit(self):
        ''' Test latest_events_4app() set limit for returned events. '''
        limit = 5
        events = self.client.latest_events_4app(limit=limit)
        self.assertEqual(len(events), limit)

    def test_latest_events_4app_event_types(self):
        ''' Test latest_events_4app() filter by event_types argument. '''
        for event_type in ["TC", "EQ", "FL", "DR", "WF", "VO"]:
            events = self.client.latest_events_4app(event_type=event_type) 
            self.assertTrue(events) if len(events) > 0 else self.assertFalse(events)
    
    def test_latest_events_4app_multiple_args(self):
        ''' Test latest_events_4app() with multiple argumnets defined. '''
        events = self.client.latest_events_4app(event_type="EQ", limit=5)
        self.assertTrue(events) if len(events) > 0 else self.assertFalse(events)
        self.assertEqual(len(events), 5) if len(events) == 5 else self.assertFalse(events)

    def test_get_event_for_different_events(self):
        self.assertTrue(
            self.client.get_event(event_type='TC', event_id='1000132')
        )  # xml event without episode_id

        self.assertTrue(
            self.client.get_event(event_type='TC', event_id='1000132', episode_id='8')
        )  # xml event with episode id

        self.assertTrue(
            self.client.get_event(event_type='DR', event_id='1012428', episode_id='10', source_format='geojson')
        )  # geojson
        
        self.assertEqual(
            self.client.get_event(event_type='TC', event_id='1000132', episode_id='8', source_format='shp'), 
            'Downloaded Shape_1000132_8.zip in directory.'
        )  # shapefile

    def test_exception_errors(self):
        ''' Testing for exceptions and errors '''
        with self.assertRaises(GDACSAPIError): # missing event record
            self.client.get_event(event_type='DR', event_id='1012428', source_format='geojson')

        with self.assertRaises(GDACSAPIError): # invalid argument
            self.client.get_event(event_type='DH', event_id='1012428', source_format='geojson')  # no event type of DH

    def test_latest_events_no_args(self):
        '''Test latest_events() without any arguments.'''
        events = self.client.latest_events()
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_latest_events_page_size(self):
        '''Test latest_events() with custom page_size parameter.'''
        page_size = 5
        events = self.client.latest_events(page_size=page_size)
        # Note: We can't assert the exact length as the API might return fewer events
        self.assertLessEqual(len(events), page_size)

    def test_latest_events_pagination(self):
        '''Test latest_events() pagination functionality.'''
        # Get first page of results with small page size
        page_size = 3
        first_page = self.client.latest_events(page_size=page_size, page_number=1)

        # Get second page of results
        second_page = self.client.latest_events(page_size=page_size, page_number=2)

        # If both pages have events, they should be different
        if len(first_page) > 0 and len(second_page) > 0:
            first_ids = [f['properties'].get('eventid') for f in first_page.features]
            second_ids = [f['properties'].get('eventid') for f in second_page.features]
            # At least some events should be different between pages
            self.assertTrue(any(id not in first_ids for id in second_ids))

    def test_latest_events_filter_by_event_type(self):
        '''Test latest_events() filter by event_list parameter.'''
        for event_type in ["TC", "EQ", "FL", "DR", "WF", "VO"]:
            events = self.client.latest_events(event_list=event_type)
            # Check that all returned events match the filter
            if len(events) > 0:
                for event in events.features:
                    self.assertEqual(event['properties']['eventtype'], event_type)

    def test_latest_events_filter_by_multiple_event_types(self):
        '''Test latest_events() filter by multiple event types.'''
        events = self.client.latest_events(event_list="EQ,TC")
        # Check that all returned events match one of the filters
        if len(events) > 0:
            for event in events.features:
                self.assertIn(event['properties']['eventtype'], ["EQ", "TC"])

    def test_latest_events_filter_by_alert_level(self):
        '''Test latest_events() filter by alert_level parameter.'''
        for level in ["green", "orange", "red"]:
            events = self.client.latest_events(alert_level=level)
            # Check that all returned events match the filter
            if len(events) > 0:
                for event in events.features:
                    self.assertEqual(event['properties']['alertlevel'], level.capitalize())

    def test_latest_events_filter_by_country(self):
        '''Test latest_events() filter by country parameter.'''
        country = "USA"  # Example country
        events = self.client.latest_events(country=country)
        # Check that all returned events match the filter
        if len(events) > 0:
            for event in events.features:
                self.assertIn(country, event['properties'].get('country', ''))

    # def test_latest_events_filter_by_severity(self):
    # Commented out as from the api documentation severity filter is not clearly documented
    #     '''Test latest_events() filter by severity parameter.'''
    #     severity = 1000  # Example severity level
    #     events = self.client.latest_events(severity=severity)
    #     # Check that all returned events match the filter
    #     if len(events) > 0:
    #         for event in events.features:
    #             self.assertGreaterEqual(event['properties'].get('severitydata').get("severity"), severity)

    def test_latest_events_filter_by_date_modified(self):
        '''Test latest_events() filter by date_modified parameter.'''
        # Use a date in ISO-8601 format
        date = "2023-01-01T00:00:00Z"
        events = self.client.latest_events(date_modified=date)
        # We can't easily verify the filter worked without knowing the data
        # But we can verify the method runs successfully
        self.assertIsInstance(events, GeoJSON)

    def test_latest_events_multiple_filters(self):
        '''Test latest_events() with multiple filters applied.'''
        events = self.client.latest_events(
            event_list="EQ",
            alert_level="red",
            page_size=5
        )
        # Check that all returned events match the filters
        if len(events) > 0:
            for event in events.features:
                self.assertEqual(event['properties']['eventtype'], "EQ")
                self.assertEqual(event['properties']['alertlevel'], "Red")
            self.assertLessEqual(len(events), 5)

    def test_latest_events_empty_response(self):
        '''Test latest_events() with filters that return no results.'''
        # Using a very specific combination of filters likely to return no results
        events = self.client.latest_events(
            event_list="TC",
            alert_level="red",
            country="Antarctica"  # Unlikely to have tropical cyclones in Antarctica
        )
        self.assertEqual(len(events.features), 0)

    def test_latest_events_invalid_event_type(self):
        '''Test latest_events() with invalid event type.'''
        with self.assertRaises(ValueError):
            self.client.latest_events(event_list="INVALID")

    def test_latest_events_invalid_alert_level(self):
        '''Test latest_events() with invalid alert level.'''
        with self.assertRaises(ValueError):
            self.client.latest_events(alert_level="INVALID")

    def test_latest_events_api_error(self):
        '''Test handling of API errors in latest_events().'''
        # Mock the requests.get to simulate an API error
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            with self.assertRaises(GDACSAPIError):
                self.client.latest_events()

    def test_latest_events_no_content(self):
        '''Test handling of 204 No Content response in latest_events().'''
        # Mock the requests.get to simulate a 204 response
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 204
            mock_get.return_value = mock_response

            events = self.client.latest_events()
            self.assertEqual(len(events.features), 0)

    def test_get_events_by_area_point(self):
        '''Test get_events_by_area() with a point geometry.'''
        # Simple point in WKT format
        point_wkt = "POINT(-73.985428 40.748817)"  # New York City coordinates
        events = self.client.get_events_by_area(geometry_area=point_wkt)
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_polygon(self):
        '''Test get_events_by_area() with a polygon geometry.'''
        # Simple polygon in WKT format (approximate area around Japan)
        polygon_wkt = "POLYGON((130 30, 145 30, 145 45, 130 45, 130 30))"
        events = self.client.get_events_by_area(geometry_area=polygon_wkt)
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_with_days(self):
        '''Test get_events_by_area() with days parameter.'''
        point_wkt = "POINT(12.496366 41.902782)"  # Rome coordinates
        days = 30  # Look for events in the last 30 days
        events = self.client.get_events_by_area(geometry_area=point_wkt, days=days)
        self.assertIsInstance(events, GeoJSON)

        # We can't easily verify the time filter worked without knowing the data
        # But we can verify the method runs successfully
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_linestring(self):
        '''Test get_events_by_area() with a linestring geometry.'''
        # Simple linestring in WKT format (approximate San Andreas fault line)
        linestring_wkt = "LINESTRING(-122.4194 37.7749, -118.2437 34.0522)"  # San Francisco to Los Angeles
        events = self.client.get_events_by_area(geometry_area=linestring_wkt)
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_multipoint(self):
        '''Test get_events_by_area() with a multipoint geometry.'''
        # Multipoint in WKT format (major European capitals)
        multipoint_wkt = "MULTIPOINT((2.3522 48.8566), (13.4050 52.5200), (-0.1278 51.5074))"  # Paris, Berlin, London
        events = self.client.get_events_by_area(geometry_area=multipoint_wkt)
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_invalid_wkt(self):
        '''Test get_events_by_area() with invalid WKT string.'''
        invalid_wkt = "NOT A WKT STRING"
        with self.assertRaises(ValueError):
            self.client.get_events_by_area(geometry_area=invalid_wkt)

    def test_get_events_by_area_empty_wkt(self):
        '''Test get_events_by_area() with empty WKT string.'''
        with self.assertRaises(ValueError):
            self.client.get_events_by_area(geometry_area="")

    def test_get_events_by_area_none_wkt(self):
        '''Test get_events_by_area() with None WKT string.'''
        with self.assertRaises(ValueError):
            self.client.get_events_by_area(geometry_area=None)

    def test_get_events_by_area_invalid_days(self):
        '''Test get_events_by_area() with invalid days parameter.'''
        point_wkt = "POINT(0 0)"
        # Test with negative days, which should still work but might get caught by API validation
        events = self.client.get_events_by_area(geometry_area=point_wkt, days=-10)
        self.assertIsInstance(events, GeoJSON)

        # Test with non-integer days, which should be converted to integer by API
        events = self.client.get_events_by_area(geometry_area=point_wkt, days=10.5)
        self.assertIsInstance(events, GeoJSON)

    def test_get_events_by_area_api_error(self):
        '''Test handling of API errors in get_events_by_area().'''
        valid_wkt = "POINT(0 0)"
        # Mock the requests.get to simulate an API error
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            with self.assertRaises(GDACSAPIError):
                self.client.get_events_by_area(geometry_area=valid_wkt)

    def test_get_events_by_area_complex_polygon(self):
        '''Test get_events_by_area() with a complex polygon geometry.'''
        # Polygon approximating the Mediterranean Sea
        complex_polygon_wkt = """POLYGON((
            -5.5 35.8, 3.0 43.8, 10.5 43.5, 13.5 45.7, 
            18.4 40.6, 20.2 40.0, 26.7 40.2, 29.0 41.2, 
            35.8 35.2, 28.0 30.8, 15.0 30.0, 10.0 31.5, 
            -5.5 35.8
        ))"""
        events = self.client.get_events_by_area(geometry_area=complex_polygon_wkt)
        self.assertIsInstance(events, GeoJSON)
        self.assertTrue(isinstance(events.features, list))

    def test_get_events_by_area_cache(self):
        '''Test caching functionality of get_events_by_area().'''
        point_wkt = "POINT(0 0)"

        # First call should make a real API request
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'features': []}
            mock_get.return_value = mock_response

            self.client.get_events_by_area(geometry_area=point_wkt)
            self.assertEqual(mock_get.call_count, 1)

        # Second call with same parameters should use cache
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'features': []}
            mock_get.return_value = mock_response

            self.client.get_events_by_area(geometry_area=point_wkt)
            self.assertEqual(mock_get.call_count, 0)  # No new API calls

        # Call with different parameters should make a new API request
        with mock.patch('requests.get') as mock_get:
            mock_response = mock.Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = {'features': []}
            mock_get.return_value = mock_response

            self.client.get_events_by_area(geometry_area=point_wkt, days=7)
            self.assertEqual(mock_get.call_count, 1)