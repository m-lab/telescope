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

import os
import sys
import unittest

sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../telescope')))
import utils


class UtilsTest(unittest.TestCase):

  def testStripSpecialCharacters(self):
    self.assertEquals('att.csv', utils.strip_special_chars('at&t.csv'))
    self.assertEquals('att.csv', utils.strip_special_chars('at&&&&&&&&t.csv'))
    self.assertEquals('att.csv', utils.strip_special_chars('at&&/;$&&&&&&t.csv'))
    self.assertEquals('maxmin-counts.csv', utils.strip_special_chars('max/min-counts.csv'))
    self.assertEquals('namesplacesdates.csv', utils.strip_special_chars(r'names\places\dates.csv'))
    self.assertEquals('spaces are okay.csv', utils.strip_special_chars('spaces are okay.csv'))

  def testFilenameBuilder(self):
    fake_filepath = utils.build_filename('/tmp/path/',
                                              'date',
                                              'duration',
                                              'site',
                                              'client_provider',
                                              'client_country',
                                              'metric',
                                              '-fake.txt')
    expected_filepath = '/tmp/path/date+duration_site_client_country_client_provider_metric-fake.txt'
    self.assertEquals(expected_filepath, fake_filepath)

if __name__ == '__main__':
  unittest.main()
