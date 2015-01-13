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
import os
import re
import itertools

import iptranslation
import utils

class Selector(object):
  """ Represents the data required to select a single dataset from
      the M-Lab data.

  """
  def __init__(self):
    self.start_time = None
    self.duration = None
    self.metric = None
    self.ip_translation_spec = None
    self.client_provider = None
    self.site = None
    self.mlab_project = None

  def __repr__(self):
    """Return a string description of the selector including such information
       as the duration of the request.

    """

    return ("<Selector Object (site: {0}, client_provider: {1}, metric: {2}, " +
            "start_time: {3}, duration: {4})>").format(self.site,
                self.client_provider, self.metric, self.start_time.strftime("%Y-%m-%d"),
                self.duration)

class SelectorFileParser(object):
  """ Parser for Telescope, the primary mechanism for specification of
      measurement targets.

  """

  """ Not implemented -- 'hop_count': 'paris-traceroute', """

  supported_metrics = {
                        'download_throughput': 'ndt',
                        'upload_throughput': 'ndt',
                        'minimum_rtt': 'ndt',
                        'average_rtt': 'ndt',
                        'packet_retransmit_rate': 'ndt'
                      }

  def __init__(self):
    self.logger = logging.getLogger('telescope')

  def parse(self, selector_filepath):
    """ Parses a selector file into one or more Selector objects. Each selector object
        corresponds to one dataset. If the selector file specifies multiple subsets, the
        parser will generate a separate selector object for each subset. If the selector
        file specifies multiple metric, the parser will generate a separate selector object
        for each supported metric.

        Args:
          selector_filepath (str): Path to selector file to parse.

        Returns:
          list: A list of parsed selector objects.
    """
    with open(selector_filepath, 'r') as selector_fileinput:
      return self._parse_file_contents(selector_fileinput.read())

  def _parse_file_contents(self, selector_file_contents):
    selector_input_json = json.loads(selector_file_contents)
    self.validate_selector_input(selector_input_json)

    selectors = self._parse_input_for_selectors(selector_input_json)
    
    return selectors

  def _parse_input_for_selectors(self, selector_json):
    """ Parse the selector JSON dictionary and return a list of dictionaries
      flattened for each combination.
      
      Args:
        selector_json (dict): Dictionary parsed from a valid
          selector JSON file with potentially lists for values.
      
      Returns:
        list: List of dictaries representing the possible combinations of
          the selector query.
      
    """
    selectors = []
    has_not_recursed = True

    start_times = selector_json['start_time']
    client_providers = selector_json['client_provider']
    sites = selector_json['site']
    metrics = selector_json['metric']
    
    for start_time, client_provider, site, metric in itertools.product(start_times, client_providers, sites, metrics):

        selector = Selector()
        selector.ip_translation_spec = self.parse_ip_translation(selector_json['ip_translation'])
        selector.duration = self.parse_duration(selector_json['duration'])

        selector.start_time = self.parse_start_time(start_time)
        selector.client_provider = client_provider
        selector.site = site
        selector.metric = metric
        
        selector.mlab_project = SelectorFileParser.supported_metrics[selector.metric]
        selectors.append(selector)
    
    return selectors

  def parse_start_time(self, start_time_string):
    """ Parse the signal start time from the expected timestamp format to
        python datetime format. Must be in UTC time.

        Args:
          measurement (str): Timestamp in format YYYY-MM-DDTHH-mm-SS

        Returns:
          datetime: Python datetime for set timestamp string.

    """
    try:
      timestamp = datetime.datetime.strptime(start_time_string, "%Y-%m-%dT%H:%M:%SZ")
      return utils.make_datetime_utc_aware(timestamp)
    except ValueError:
      raise ValueError('UnsupportedSubsetDateFormat')

  def parse_duration(self, duration_string):
    """ Parse the signal duration from the expected timestamp format to
        integer number of seconds.

        Args:
          duration (str): length in human-readable format, must follow number +
            time type format. (d=days, h=hours, m=minutes, s=seconds), e.g. 30d

        Returns:
          int: Number of seconds in specified time period.

    """

    duration_seconds_to_return = int(0)
    duration_string_segments = re.findall("[0-9]+[a-zA-Z]+", duration_string)

    if len(duration_string_segments) > 0:
      for segment in duration_string_segments:
        numerical_amount = int(re.search("[0-9]+", segment).group(0))
        duration_type = re.search("[a-zA-Z]+", segment).group(0)

        if duration_type == "d":
          duration_seconds_to_return += datetime.timedelta(days = numerical_amount).total_seconds()
        elif duration_type == "h":
          duration_seconds_to_return += datetime.timedelta(hours = numerical_amount).total_seconds()
        elif duration_type == "m":
          duration_seconds_to_return += datetime.timedelta(minutes = numerical_amount).total_seconds()
        elif duration_type == "s":
          duration_seconds_to_return += numerical_amount
        else:
          raise ValueError('UnsupportedSelectorDurationType')
    else:
      raise ValueError('UnsupportedSelectorDuration')

    return duration_seconds_to_return

  def parse_ip_translation(self, ip_translation_dict):
    """ Parse the ip_translation field into an IPTranslationStrategySpec object.

        Args:
          ip_translation_dict (dict): An unprocessed dictionary of
          ip_translation data from the input selector file.

        Returns:
          IPTranslationStrategySpec: An IPTranslationStrategySpec, which specifies
          properties of the IP translation strategy according to the selector file.

    """
    try:
      ip_translation_spec = iptranslation.IPTranslationStrategySpec
      ip_translation_spec.strategy_name = ip_translation_dict['strategy']
      ip_translation_spec.params = ip_translation_dict['params']
      return ip_translation_spec
    except KeyError as e:
      raise ValueError('Missing expected field in ip_translation dict: %s' % e.args[0])

  def validate_selector_input(self, selector_dict):
    if not selector_dict.has_key('file_format_version'):
        raise ValueError('NoSelectorVersionSpecified')
    elif selector_dict['file_format_version'] == 1.0:
        raise ValueError('DeprecatedSelectorVersion')
    elif selector_dict['file_format_version'] == 1.1:
        parser_validator = SelectorFileValidator1_1()
    else:
        raise ValueError('UnsupportedSelectorVersion')

    parser_validator.validate(selector_dict)

class SelectorFileValidator(object):
    def validate(self, selector_dict):
        raise NotImplementedError('Subclasses must implement this function.')
    def validate_common(self, selector_dict):
        if not selector_dict.has_key('duration'):
            raise ValueError('UnsupportedDuration')

        if not selector_dict.has_key('metric') or \
            type(selector_dict['metric']) != list:
                raise ValueError('MetricsRequiresList')

class SelectorFileValidator1_1(SelectorFileValidator):
    def validate(self, selector_dict):
        self.validate_common(selector_dict)
        if selector_dict.has_key('subsets'):
            raise ValueError('SubsetsNoLongerSupported')
