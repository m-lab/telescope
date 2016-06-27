#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2016 Measurement Lab
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

from __future__ import absolute_import
import unittest

import mock

from telescope import result_csv


class ResultCsvTest(unittest.TestCase):

    def test_metrics_to_csv_returns_empty_string_when_given_empty_list(self):
        self.assertEqual('', result_csv.metrics_to_csv([]))

    def test_metrics_to_csv_processes_single_element_list_correctly(self):
        """Single element lists should create one CSV line in correct order."""
        self.assertEqual('123456,54.6\r\n', result_csv.metrics_to_csv(
            [{'timestamp': 123456,
              'download_mbps': 54.6}]))
        self.assertEqual('123456,54.6\r\n', result_csv.metrics_to_csv(
            [{'timestamp': 123456,
              'upload_mbps': 54.6}]))
        self.assertEqual('123456,54.6\r\n', result_csv.metrics_to_csv(
            [{'timestamp': 123456,
              'average_rtt': 54.6}]))
        self.assertEqual('123456,54.6\r\n', result_csv.metrics_to_csv(
            [{'timestamp': 123456,
              'minimum_rtt': 54.6}]))
        self.assertEqual('123456,54.6\r\n', result_csv.metrics_to_csv(
            [{'timestamp': 123456,
              'packet_retransmit_rate': 54.6}]))

    def test_metrics_to_csv_always_makes_timestamp_first_column(self):

        def mock_get(key, _):
            row = {'timestamp': 123456, 'download_mbps': 54.6}
            return row[key]

        mock_row = mock.MagicMock()
        # metrics_to_csv should make timestamp the first column, even if it
        # recevies the row keys in a different order.
        mock_row.keys.return_value = ['download_mbps', 'timestamp']
        mock_row.get.side_effect = mock_get
        self.assertEqual('123456,54.6\r\n',
                         result_csv.metrics_to_csv([mock_row]))

    def test_metrics_to_csv_processes_three_element_list_correctly(self):
        metrics_list = [
            {'timestamp': 123456, 'download_mbps': 54.6},
            {'timestamp': 234567, 'download_mbps': 19.9},
            {'timestamp': 345678, 'download_mbps': 23.5}
        ]  # yapf: disable
        csv_expected = ('123456,54.6\r\n'
                        '234567,19.9\r\n'
                        '345678,23.5\r\n')  # yapf: disable
        csv_actual = result_csv.metrics_to_csv(metrics_list)
        self.assertMultiLineEqual(csv_expected, csv_actual)


if __name__ == '__main__':
    unittest.main()
