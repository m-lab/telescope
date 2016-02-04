#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2015 Measurement Lab
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
import io
import os
import sys
import unittest

import mock

sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../telescope')))
import iptranslation


class IPTranslationStrategyFactoryTest(unittest.TestCase):

    def _createDummyMaxmindStrategySpec(self):
        strategy_params = {
            'maxmind_dir': '/fake/dir',
            'db_snapshots': ['2012-01-01']
        }
        return iptranslation.IPTranslationStrategySpec('maxmind',
                                                       strategy_params)

    def testCreationSucceedsWhenFileExists(self):
        """Verify that we can create a MaxMind IP translator."""
        mock_file_opener = mock.Mock(return_value=io.BytesIO())
        factory = iptranslation.IPTranslationStrategyFactory(mock_file_opener)
        strategy_spec = self._createDummyMaxmindStrategySpec()
        self.assertIsNotNone(factory.create(strategy_spec))
        mock_file_opener.assert_called_with(
            '/fake/dir/GeoIPASNum2-20120101.csv', 'rb')

    def testFileIOErrorYieldsMissingMaxmindError(self):
        """Verify that we wrap the error on missing/unreadable files."""
        mock_file_opener = mock.Mock(side_effect=IOError('mock error'))
        factory = iptranslation.IPTranslationStrategyFactory(mock_file_opener)
        strategy_spec = self._createDummyMaxmindStrategySpec()
        with self.assertRaises(iptranslation.MissingMaxMindError):
            factory.create(strategy_spec)


class IPTranslationStrategyMaxMindTest(unittest.TestCase):

    def createIPTranslationStrategy(self, mock_file_contents):
        snapshot_datetime = datetime.datetime(2014, 9, 1)
        mock_file = io.BytesIO(mock_file_contents)
        snapshots = [(snapshot_datetime, mock_file),]
        return iptranslation.IPTranslationStrategyMaxMind(snapshots)

    def assertBlocksMatchForSearch(self, mock_file_contents, asn_search_name,
                                   expected_blocks):
        translation_strategy = self.createIPTranslationStrategy(
            mock_file_contents)
        actual_blocks = translation_strategy.find_ip_blocks(asn_search_name)
        self.assertListEqual(expected_blocks, actual_blocks)

    def testVanillaFile(self):
        mock_file_contents = """5,10,"FooISP"
20,25,"BarIsp"
"""

        expected_blocks = [(20, 25)]
        self.assertBlocksMatchForSearch(mock_file_contents, 'bar',
                                        expected_blocks)

    # Verify that repeated searches of the same ISP name return the same results
    def testRepeatedSearches(self):
        mock_file_contents = """5,10,"FooISP"
20,25,"BarIsp"
"""

        translation_strategy = self.createIPTranslationStrategy(
            mock_file_contents)
        actual_blocks1 = translation_strategy.find_ip_blocks('bar')
        actual_blocks2 = translation_strategy.find_ip_blocks('bar')
        self.assertListEqual([(20, 25)], actual_blocks1)
        self.assertListEqual([(20, 25)], actual_blocks2)

    def testLevel3Expansion(self):
        mock_file_contents = """1,15,"Level 3 Communications"
16,20,"Rando Internet Company"
21,25,"GBLX"
"""

        expected_blocks = [(1, 15), (21, 25)]
        self.assertBlocksMatchForSearch(mock_file_contents, 'level3',
                                        expected_blocks)

    def testTimeWarnerExpansion(self):
        mock_file_contents = """57,63,"Time Warner"
92,108,"Time Warner"
109,110,"ConglomCo Internet"
"""

        expected_blocks = [(57, 63), (92, 108)]
        self.assertBlocksMatchForSearch(mock_file_contents, 'twc',
                                        expected_blocks)

    def testCenturyLinkExpansion(self):
        mock_file_contents = """5,9,"Embarq"
34,38,"Red Herring Internet"
45,51,"CenturyLink"
55,58,"CenturyTel"
60,65,"Generic InternetCo"
67,68,"Qwest Internet"
"""

        expected_blocks = [(5, 9), (45, 51), (55, 58), (67, 68)]
        self.assertBlocksMatchForSearch(mock_file_contents, 'centurylink',
                                        expected_blocks)


if __name__ == '__main__':
    unittest.main()
