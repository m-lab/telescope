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

import unittest

import metrics_math


class MetricsMathTest(unittest.TestCase):

  def calculate_single_result(self, metric, datarow):
    """Wraps a call to calculate_results_list so that callers can pass in a
       single datarow, rather than a collection of rows.
    """
    input_datarows = [datarow,]
    result_rows = metrics_math.calculate_results_list(metric, input_datarows)
    self.assertEqual(1, len(result_rows))
    return result_rows[0]

  def assertMetricMatchesExpected(self, datarow, metric_name,
                                  timestamp_expected, metric_value_expected):
    """Asserts that, given a datarow, metrics_math library produces the
       expected timestamp and result values for the specified metric.
    """
    result = self.calculate_single_result(metric_name, datarow)
    self.assertEqual(timestamp_expected, result['timestamp'])
    self.assertEqual(metric_value_expected, result['result'])

  def test_calculate_results_list_hop_count(self):
    # TODO: Write this test.
    pass
    # mock_row = {}
    # self.assertMetricMatchesExpected(mock_row, 'hop_count', 1407959123, 15)

  def test_calculate_results_list_minrtt(self):
    mock_row = {
        'web100_log_entry_log_time': '1407959123',
        'web100_log_entry_snap_MinRTT': '125'
        }
    self.assertMetricMatchesExpected(mock_row, 'minimum_rtt', 1407959123, 125)

  def test_calculate_results_list_avgrtt(self):
    mock_row = {
        'web100_log_entry_log_time': '1407959123',
        'web100_log_entry_snap_SumRTT': '25',
        'web100_log_entry_snap_CountRTT': '10'
        }
    self.assertMetricMatchesExpected(mock_row, 'average_rtt', 1407959123, 2.5)

  def test_calculate_results_list_download_throughput(self):
    mock_row = {
        'web100_log_entry_log_time': '1407959123',
        'connection_spec_data_direction': '1',
        'web100_log_entry_snap_HCThruOctetsAcked': '45',
        'web100_log_entry_snap_SndLimTimeRwin': '2',
        'web100_log_entry_snap_SndLimTimeCwnd': '3',
        'web100_log_entry_snap_SndLimTimeSnd': '5'
        }
    # Expected download throughput = (45 / (2 + 3 + 5)) * 8 = 36.0
    self.assertMetricMatchesExpected(mock_row,
                                     'download_throughput',
                                     1407959123,
                                     36.0)

  def test_calculate_results_list_upload_throughput(self):
    mock_row = {
        'web100_log_entry_log_time': '1407959123',
        'connection_spec_data_direction': '0',
        'web100_log_entry_snap_HCThruOctetsReceived': '5',
        'web100_log_entry_snap_Duration': '2'
        }
    # Expected upload throughput = (5 / 2) * 8 = 20.0
    self.assertMetricMatchesExpected(mock_row,
                                     'upload_throughput',
                                     1407959123,
                                     20.0)

  def test_calculate_results_packet_retransmit_rate(self):
    mock_row = {
        'web100_log_entry_log_time': '1407959123',
        'connection_spec_data_direction': '1',
        'web100_log_entry_snap_SegsRetrans': '7',
        'web100_log_entry_snap_DataSegsOut': '2',
        }
    # Expected packet retransmit rate = 7 / 2 = 3.5
    self.assertMetricMatchesExpected(mock_row,
                                     'packet_retransmit_rate',
                                     1407959123,
                                     3.5)

  def test_calculate_throughput(self):
    calculate = metrics_math.calculate_throughput
    self.assertEqual(160.0, calculate(10.0, 0.5))
    self.assertEqual(20.0, calculate(5, 2))
    self.assertEqual(36.0, calculate('9', '2'))

  def test_calculate_minrtt(self):
    calculate = metrics_math.calculate_minrtt
    self.assertEqual(5.0, calculate(5))
    self.assertEqual(5.0, calculate('5'))
    self.assertEqual(5.0, calculate(5.0))
    self.assertEqual(2.5, calculate(2.5))

  def test_calculate_avgrtt(self):
    calculate = metrics_math.calculate_avgrtt
    self.assertEqual(2.5, calculate('5', '2'))
    self.assertEqual(2.5, calculate(5, 2))
    self.assertEqual(2.5, calculate(5.0, 2.0))
    self.assertEqual(10.0, calculate(20.0, 2.0))
    self.assertEqual(1.0, calculate(10.0, 10.0))

  def test_calculate_packet_retransmit_rate(self):
    calculate = metrics_math.calculate_packet_retransmit_rate
    self.assertEqual(2.5, calculate('5', '2'))
    self.assertEqual(2.5, calculate(5, 2))
    self.assertEqual(2.5, calculate(5.0, 2.0))
    self.assertEqual(10.0, calculate(20.0, 2.0))
    self.assertEqual(1.0, calculate(10.0, 10.0))

if __name__ == '__main__':
  unittest.main()

