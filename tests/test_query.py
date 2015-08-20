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
import os
import re
import sys
import unittest

sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../telescope')))
import query
import utils


class BigQueryQueryGeneratorTest(unittest.TestCase):

  def setUp(self):
    self.maxDiff = None

  def normalize_whitespace(self, original):
    return re.sub(r'\s+', ' ', original).strip()

  def split_and_normalize_query(self, query_string):
    lines = []
    for line in query_string.splitlines():
      # omit blank lines
      if not line:
        continue
      lines.append(self.normalize_whitespace(line))
    return lines

  def assertQueriesEqual(self, expected, actual):
    expected_lines = self.split_and_normalize_query(expected)
    actual_lines = self.split_and_normalize_query(actual)

    self.assertSequenceEqual(expected_lines, actual_lines)

  def generate_ndt_query(self, start_time, end_time, metric, server_ips,
                         client_ip_blocks, client_country):
    start_time_utc = utils.make_datetime_utc_aware(start_time)
    end_time_utc = utils.make_datetime_utc_aware(end_time)
    generator = query.BigQueryQueryGenerator(start_time_utc,
                                             end_time_utc,
                                             metric,
                                             server_ips = server_ips,
                                             client_ip_blocks = client_ip_blocks,
                                             client_country = client_country)
    return generator.query()

  def generate_download_throughput_query(self, start_time, end_time, server_ips,
                                         client_ip_blocks, client_country):
    return self.generate_ndt_query(start_time,
                                   end_time,
                                   'download_throughput',
                                   server_ips,
                                   client_ip_blocks,
                                   client_country)

  def generate_upload_throughput_query(self, start_time, end_time, server_ips,
                                       client_ip_blocks, client_country):
    return self.generate_ndt_query(start_time,
                                   end_time,
                                   'upload_throughput',
                                   server_ips,
                                   client_ip_blocks,
                                   client_country)

  def generate_average_rtt_query(self, start_time, end_time, server_ips,
                                 client_ip_blocks, client_country):
    return self.generate_ndt_query(start_time,
                                   end_time,
                                   'average_rtt',
                                   server_ips,
                                   client_ip_blocks,
                                   client_country)

  def generate_minimum_rtt_query(self, start_time, end_time, server_ips,
                                 client_ip_blocks, client_country):
    return self.generate_ndt_query(start_time,
                                   end_time,
                                   'minimum_rtt',
                                   server_ips,
                                   client_ip_blocks,
                                   client_country)

  def test_ndt_queries_have_no_trailing_whitespace(self):
    start_time = datetime.datetime(2012, 1, 1)
    end_time = datetime.datetime(2014, 10, 15)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [
        (5, 10),
        (35, 80)
        ]
    query_generators = (self.generate_average_rtt_query,
                        self.generate_minimum_rtt_query,
                        self.generate_upload_throughput_query,
                        self.generate_download_throughput_query)
    for query_generator in query_generators:
      generated_query = query_generator(start_time, end_time, server_ips,
                                        client_ip_blocks, None)
      self.assertNotRegexpMatches(generated_query, r'.*\s\n')

  def test_ndt_download_throughput_query_full_month(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [
        (5, 10),
        (35, 80)
        ]
    query_actual = self.generate_download_throughput_query(start_time,
                                                           end_time,
                                                           server_ips,
                                                           client_ip_blocks,
                                                           None)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1' OR
       web100_log_entry.connection_spec.local_ip = '2.2.2.2')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10 OR
       PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 35 AND 80)"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_download_throughput_query_full_month_plus_one_second(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1, 0, 0, 1)
    server_ips = ['1.1.1.1',]
    client_ip_blocks = [(5, 10),]
    query_actual = self.generate_download_throughput_query(start_time,
                                                           end_time,
                                                           server_ips,
                                                           client_ip_blocks,
                                                           None)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all],
  [plx.google:m_lab.2014_02.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212801))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10)"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_upload_throughput_query_full_month(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [
        (5, 10),
        (35, 80)
        ]
    query_actual = self.generate_upload_throughput_query(start_time,
                                                         end_time,
                                                         server_ips,
                                                         client_ip_blocks,
                                                         None)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.Duration,
  web100_log_entry.snap.HCThruOctetsReceived,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 0
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1' OR
       web100_log_entry.connection_spec.local_ip = '2.2.2.2')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10 OR
       PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 35 AND 80)"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_average_rtt_query_full_month(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [
        (5, 10),
        (35, 80)
        ]
    query_actual = self.generate_average_rtt_query(start_time,
                                                   end_time,
                                                   server_ips,
                                                   client_ip_blocks,
                                                   None)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.CountRTT,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State,
  web100_log_entry.snap.SumRTT
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1' OR
       web100_log_entry.connection_spec.local_ip = '2.2.2.2')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10 OR
       PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 35 AND 80)"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_min_rtt_query_full_month(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [
        (5, 10),
        (35, 80)
        ]
    query_actual = self.generate_minimum_rtt_query(start_time,
                                                   end_time,
                                                   server_ips,
                                                   client_ip_blocks,
                                                   None)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.CountRTT,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.MinRTT,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1' OR
       web100_log_entry.connection_spec.local_ip = '2.2.2.2')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10 OR
       PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 35 AND 80)"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_download_throughput_query_v1_1_all_properties(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    server_ips = ['1.1.1.1', '2.2.2.2']
    client_ip_blocks = [(5, 10)]
    client_country = "us"
    query_actual = self.generate_download_throughput_query(start_time,
                                                            end_time,
                                                            server_ips,
                                                            client_ip_blocks,
                                                            client_country)
    query_expected = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1' OR
       web100_log_entry.connection_spec.local_ip = '2.2.2.2')
  AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10)
  AND (connection_spec.client_geolocation.country_code = 'US')
"""
    self.assertQueriesEqual(query_expected, query_actual)

  def test_ndt_download_throughput_query_v1_1_optional_properties(self):
    start_time = datetime.datetime(2014, 1, 1)
    end_time = datetime.datetime(2014, 2, 1)
    optional_properties = {
        'server_ips': {
          'default': [],
          'test_value': ['1.1.1.1'],
          'test_expectation': "AND (web100_log_entry.connection_spec.local_ip = '1.1.1.1')"
        },
        'client_ip_blocks': {
          'default': [],
          'test_value': [(5, 10)],
          'test_expectation': "AND (PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN 5 AND 10)"
        },
        'client_country': {
          'default': None,
          'test_value': 'us',
          'test_expectation': "AND (connection_spec.client_geolocation.country_code = 'US')"
        },
      }
    query_expected_template = """
SELECT
  connection_spec.data_direction,
  web100_log_entry.log_time,
  web100_log_entry.snap.CongSignals,
  web100_log_entry.snap.HCThruOctetsAcked,
  web100_log_entry.snap.SndLimTimeCwnd,
  web100_log_entry.snap.SndLimTimeRwin,
  web100_log_entry.snap.SndLimTimeSnd,
  web100_log_entry.snap.State
FROM
  [plx.google:m_lab.2014_01.all]
WHERE
  connection_spec.data_direction IS NOT NULL
  AND web100_log_entry.is_last_entry IS NOT NULL
  AND web100_log_entry.snap.HCThruOctetsAcked IS NOT NULL
  AND web100_log_entry.snap.CongSignals IS NOT NULL
  AND web100_log_entry.connection_spec.remote_ip IS NOT NULL
  AND web100_log_entry.connection_spec.local_ip IS NOT NULL
  AND project = 0
  AND web100_log_entry.is_last_entry = True
  AND connection_spec.data_direction == 1
  AND ((web100_log_entry.log_time >= 1388534400) AND (web100_log_entry.log_time < 1391212800))
  {0}
"""

    query_expected = query_expected_template.format(optional_properties['server_ips']['test_expectation'])
    query_actual = self.generate_download_throughput_query(start_time,
                                                            end_time,
                                                            optional_properties['server_ips']['test_value'],
                                                            optional_properties['client_ip_blocks']['default'],
                                                            optional_properties['client_country']['default'])
    self.assertQueriesEqual(query_expected, query_actual)

    query_expected = query_expected_template.format(optional_properties['client_ip_blocks']['test_expectation'])
    query_actual = self.generate_download_throughput_query(start_time,
                                                            end_time,
                                                            optional_properties['server_ips']['default'],
                                                            optional_properties['client_ip_blocks']['test_value'],
                                                            optional_properties['client_country']['default'])
    self.assertQueriesEqual(query_expected, query_actual)

    query_expected = query_expected_template.format(optional_properties['client_country']['test_expectation'])
    query_actual = self.generate_download_throughput_query(start_time,
                                                            end_time,
                                                            optional_properties['server_ips']['default'],
                                                            optional_properties['client_ip_blocks']['default'],
                                                            optional_properties['client_country']['test_value'])
    self.assertQueriesEqual(query_expected, query_actual)


if __name__ == '__main__':
  unittest.main()
