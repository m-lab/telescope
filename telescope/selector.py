#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2014 Measurement Lab
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import json
import logging
import re
import itertools

import iptranslation
import utils


class Error(Exception):
    pass


class SelectorParseError(Error):

    def __init__(self, message):
        super(SelectorParseError, self).__init__(
            'Failed to parse selector file: %s' % message)


class Selector(object):
    """Represents the data required to select a dataset from the M-Lab data.

     Attributes:
         start_time: (datetime) Time at which selection window begins.
         duration: (int) Duration of time window in seconds.
         metric: (str) Specifies the metric for which to retrieve data.
         ip_translation_spec: (iptranslation.IPTranslationStrategySpec)
             Specifies how to translate the IP address information.
         client_provider: (str) Name of provider for which to retrieve data.
         site_name: (str) Name of M-Lab site for which to retrieve data.
    """

    def __init__(self):
        self.start_time = None
        self.duration = None
        self.metric = None
        self.ip_translation_spec = None
        self.client_provider = None
        self.client_country = None
        self.site = None

    def __repr__(self):
        return (
            '<Selector Object (site: %s, client_provider: %s, client_country:'
            '%s, metric: %s, start_time: %s, duration: %s)>' %
            (self.site, self.client_provider, self.client_country, self.metric,
             self.start_time.strftime("%Y-%m-%d"), self.duration))


class MultiSelector(object):
    """Represents a set of Selector objects.

     Attributes:
         start_times: (list) A list of datetimes indicating the start times of
             the child Selectors.
         duration: (int) Duration of time window in seconds for child Selectors.
             Note that duation is a scalar, as a list would result in Selectors
             that overlap.
         metrics: (list) A list of metrics contained in child Selectors.
         ip_translation_spec: (iptranslation.IPTranslationStrategySpec)
             Specifies how to translate the IP address information.
         client_providers: (list) List of string names of providers in the child
             Selectors.
         sites: (list) List of M-Lab sites in the child Selectors..
    """

    def __init__(self):
        self.start_times = None
        self.duration = None
        self.ip_translation_spec = None

        # We use itertools to enumerate a combination of individual selectors from
        # lists of multiple values. Itertools will not iterate when passed a None
        # value, so instead we default to a list with a single value of None.

        self.metrics = [None]
        self.client_providers = [None]
        self.client_countries = [None]
        self.sites = [None]

    def split(self):
        """Splits a MultiSelector into an equivalent list of Selectors."""
        selectors = []
        selector_product = itertools.product(
            self.start_times, self.client_providers, self.client_countries,
            self.sites, self.metrics)
        for (start_time, client_provider, client_country, site,
             metric) in selector_product:
            selector = Selector()
            selector.ip_translation_spec = self.ip_translation_spec
            selector.duration = self.duration
            selector.start_time = start_time
            selector.client_provider = client_provider
            selector.client_country = client_country
            selector.site = site
            selector.metric = metric
            selectors.append(selector)
        return selectors


class SelectorFileParser(object):
    """Parser for Telescope selector files.

    Parses selector files, the primary mechanism for specification of
    measurement targets.
    """

    def __init__(self):
        self.logger = logging.getLogger('telescope')

    def parse(self, selector_filepath):
        """Parses a selector file into one or more Selector objects.

        Each Selector object corresponds to one discrete dataset to retrieve
        from BigQuery. For fields in the selector file that contain lists of
        values (e.g. metrics, sites), the parser will create a separate
        Selector object for each combination of those values (e.g. if the file
        specifies metrics [A, B] and sites: [X, Y, Z] the parser will create
        Selectors for (A, X), (A, Y), (A, Z), (B, X), (B, Y), (B,Z)).

        Args:
            selector_filepath: (str) Path to selector file to parse.

        Returns:
            list: A list of parsed Selector objects.
        """
        with open(selector_filepath, 'r') as selector_fileinput:
            return self._parse_file_contents(selector_fileinput.read())

    def _parse_file_contents(self, selector_file_contents):
        try:
            selector_input_json = json.loads(selector_file_contents)
        except ValueError:
            raise SelectorParseError('MalformedJSON')

        self._validate_selector_input(selector_input_json)

        selectors = self._parse_input_for_selectors(selector_input_json)

        return selectors

    def _parse_input_for_selectors(self, selector_json):
        """Parse the selector JSON dictionary and return a list of Selectors,
        one for each combination specified.

        Args:
            selector_json (dict): Unprocessed Selector JSON file represented as a
                dict.

        Returns:
            list: A list of parsed Selector objects.
        """
        multi_selector = MultiSelector()
        multi_selector.duration = self._parse_duration(
            selector_json['duration'])
        multi_selector.ip_translation_spec = self._parse_ip_translation(
            selector_json['ip_translation'])

        multi_selector.start_times = self._parse_start_times(
            selector_json['start_times'])
        multi_selector.metrics = selector_json['metrics']

        if ('client_providers' in selector_json and
                selector_json['client_providers']):
            multi_selector.client_providers = _normalize_string_values(
                selector_json['client_providers'])
        if ('client_countries' in selector_json and
                selector_json['client_countries']):
            multi_selector.client_countries = _normalize_string_values(
                selector_json['client_countries'])
        if 'sites' in selector_json and selector_json['sites']:
            multi_selector.sites = _normalize_string_values(
                selector_json['sites'])

        return multi_selector.split()

    def _parse_start_times(self, start_times_raw):
        start_times = []
        for start_time_string in start_times_raw:
            start_times.append(self._parse_start_time(start_time_string))
        return start_times

    def _parse_start_time(self, start_time_string):
        """Parse the time window start time.

        Parse the start time from the expected timestamp format to Python
        datetime format. Must be in UTC time.

        Args:
            start_time_string: (str) Timestamp in format YYYY-MM-DDTHH-mm-SS.

        Returns:
            datetime: Python datetime for set timestamp string.
        """
        try:
            timestamp = (
                datetime.datetime.strptime(start_time_string,
                                           '%Y-%m-%dT%H:%M:%SZ'))
            return utils.make_datetime_utc_aware(timestamp)
        except ValueError:
            raise SelectorParseError('UnsupportedSubsetDateFormat')

    def _parse_duration(self, duration_string):
        """Parse the time window duration.

        Parse the time window duration from the expected timespan format to
        integer number of seconds.

        Args:
            duration_string: (str) length in human-readable format, must follow
                number + time type format. (d=days, h=hours, m=minutes,
                s=seconds), e.g. 30d.

        Returns:
            int: Number of seconds in specified time period.
        """
        duration_seconds_to_return = 0
        duration_string_segments = re.findall('[0-9]+[a-zA-Z]+',
                                              duration_string)

        if duration_string_segments:
            for segment in duration_string_segments:
                numerical_amount = int(re.search('[0-9]+', segment).group(0))
                duration_type = re.search('[a-zA-Z]+', segment).group(0)

                if duration_type == 'd':
                    duration_seconds_to_return += (
                        datetime.timedelta(
                            days=numerical_amount).total_seconds())
                elif duration_type == 'h':
                    duration_seconds_to_return += (
                        datetime.timedelta(
                            hours=numerical_amount).total_seconds())
                elif duration_type == 'm':
                    duration_seconds_to_return += (
                        datetime.timedelta(
                            minutes=numerical_amount).total_seconds())
                elif duration_type == 's':
                    duration_seconds_to_return += numerical_amount
                else:
                    raise SelectorParseError('UnsupportedSelectorDurationType')
        else:
            raise SelectorParseError('UnsupportedSelectorDuration')

        return duration_seconds_to_return

    def _parse_ip_translation(self, ip_translation_dict):
        """Parse the ip_translation field into an IPTranslationStrategySpec object.

        Args:
            ip_translation_dict: (dict) An unprocessed dictionary of
                ip_translation data from the input selector file.

        Returns:
            IPTranslationStrategySpec: An IPTranslationStrategySpec, which
            specifies properties of the IP translation strategy according to
            the selector file.
        """
        try:
            ip_translation_spec = iptranslation.IPTranslationStrategySpec
            ip_translation_spec.strategy_name = ip_translation_dict['strategy']
            ip_translation_spec.params = ip_translation_dict['params']
            return ip_translation_spec
        except KeyError as e:
            raise SelectorParseError(
                ('Missing expected field in ip_translation '
                 'dict: %s') % e.args[0])

    def _validate_selector_input(self, selector_dict):
        if 'file_format_version' not in selector_dict:
            raise SelectorParseError('NoSelectorVersionSpecified')
        elif selector_dict['file_format_version'] == 1.0:
            raise SelectorParseError('DeprecatedSelectorVersion')
        elif selector_dict['file_format_version'] == 1.1:
            parser_validator = SelectorFileValidator1_1()
        else:
            raise SelectorParseError('UnsupportedSelectorVersion')

        parser_validator.validate(selector_dict)


class SelectorFileValidator(object):

    def validate(self, selector_dict):
        raise NotImplementedError('Subclasses must implement this function.')

    def validate_common(self, selector_dict):
        if 'duration' not in selector_dict:
            raise SelectorParseError('UnsupportedDuration')

        if (('metrics' not in selector_dict) or
            (type(selector_dict['metrics']) != list)):
            raise SelectorParseError('MetricsRequiresList')

        if not selector_dict['start_times']:
            raise SelectorParseError('List of start times must be non-empty.')

        if not selector_dict['metrics']:
            raise SelectorParseError('List of metrics must be non-empty.')

        if 'client_countries' in selector_dict:
            for client_country in selector_dict['client_countries']:
                if not re.match('^[a-zA-Z]{2}$', client_country):
                    raise SelectorParseError(
                        'Requires ISO alpha-2 country code.')

        supported_metrics = ('upload_throughput', 'download_throughput',
                             'average_rtt', 'minimum_rtt',
                             'packet_retransmit_rate')
        for metric in selector_dict['metrics']:
            if metric not in supported_metrics:
                raise SelectorParseError('UnsupportedMetric')


class SelectorFileValidator1_1(SelectorFileValidator):

    def validate(self, selector_dict):
        self.validate_common(selector_dict)
        if 'subsets' in selector_dict:
            raise SelectorParseError('SubsetsNoLongerSupported')


class MultiSelectorJsonEncoder(json.JSONEncoder):
    """Encode Telescope multi-selector into JSON."""

    def default(self, obj):
        if isinstance(obj, MultiSelector):
            return self._encode_multi_selector(obj)
        return json.JSONEncoder.default(self, obj)

    def _encode_multi_selector(self, selector):
        base_selector = {
            'file_format_version': 1.1,
            'duration': self._encode_duration(selector.duration),
            'metrics': selector.metrics,
            'ip_translation': self._encode_ip_translation(
                selector.ip_translation_spec),
            'start_times': self._encode_start_times(selector.start_times)
        }

        if selector.sites != [None]:
            base_selector['sites'] = selector.sites
        if selector.client_countries != [None]:
            base_selector['client_countries'] = selector.client_countries
        if selector.client_providers != [None]:
            base_selector['client_providers'] = selector.client_providers

        return base_selector

    def _encode_duration(self, duration):
        return str(duration) + 'd'

    def _encode_ip_translation(self, ip_translation):
        return {
            'strategy': ip_translation.strategy_name,
            'params': ip_translation.params,
        }

    def _encode_start_times(self, start_times):
        encoded_start_times = []
        for start_time in start_times:
            encoded_start_times.append(datetime.datetime.strftime(
                start_time, '%Y-%m-%dT%H:%M:%SZ'))
        return encoded_start_times


def _normalize_string_values(field_values):
    """Normalize string values for passed parameters.

    Normalizes parameters to ensure that they are consistent across queries where
    factors such as case are should not change the output, and therefore not
    require additional Telescope queries.

    Args:
        field_values: (list) A list of string parameters.

    Returns:
        list: A list of normalized parameters for building selectors from.
    """
    return [field_value.lower() for field_value in field_values]
