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


import copy
import datetime
import unittest

import iptranslation
import selector
import utils

class SelectorTest(unittest.TestCase):

  def parse_file_contents(self, selector_file_contents):
    parser = selector.SelectorFileParser()
    return parser._parse_file_contents(selector_file_contents)

  def assertSelectorMatches(self, selector_expected, selector_actual):
    self.assertEqual(selector_expected.start_time, selector_actual.start_time)
    self.assertEqual(selector_expected.duration, selector_actual.duration)
    self.assertEqual(selector_expected.metric, selector_actual.metric)
    self.assertEqual(selector_expected.ip_translation_spec.strategy_name,
                     selector_actual.ip_translation_spec.strategy_name)
    self.assertDictEqual(selector_expected.ip_translation_spec.params, selector_actual.ip_translation_spec.params)
    self.assertEqual(selector_expected.site_name, selector_actual.site_name)
    self.assertEqual(selector_expected.client_provider, selector_actual.client_provider)

  def assertParsedSelectorsMatch(self, selectors_expected, selector_file_contents):
    selectors_actual = self.parse_file_contents(selector_file_contents)
    self.assertEqual(len(selectors_expected), len(selectors_actual))

    # The parser parses the subsets in reverse order, so we must compare
    # selectors in reverse.
    for i in reversed(range(len(selectors_expected))):
      self.assertSelectorMatches(selectors_expected[i], selectors_actual[i])

  def assertParsedSingleSelectorMatches(self, selector_expected, selector_file_contents):
    self.assertParsedSelectorsMatch([selector_expected], selector_file_contents)

  def testDeprecatedFileFormats(self):
    selector_file_contents = """{
           "file_format_version": 1,
           "duration": "30d",
           "metric":"average_rtt",
           "ip_translation":{
             "strategy":"maxmind",
             "params":{
               "db_snapshots":["2014-08-04"]
             }
           },
           "subsets":[
              {
                 "site":"lga02",
                 "client_provider":"comcast",
                 "start_time":"2014-02-01T00:00:00Z"
              }
           ]
        }"""
    self.assertRaises(ValueError, self.parse_file_contents, selector_file_contents)
  
  def testDeprecatedSubsetFunction(self):
    selector_file_contents = """{
              "file_format_version": 1.1,
              "duration": "30d",
              "metric":"average_rtt",
              "ip_translation":{
                  "strategy":"maxmind",
                  "params":{
                      "db_snapshots":["2014-08-04"]
                  }
          },
          "subsets":[
              {
                  "site":"lga02",
                  "client_provider":"comcast",
                  "start_time":"2014-02-01T00:00:00Z"
              },
              {
                  "site":"lga01",
                  "client_provider":"comcast",
                  "start_time":"2014-02-01T00:00:00Z"
              }
              ]
          }"""
    self.assertRaises(ValueError, self.parse_file_contents, selector_file_contents)
  
  def testValidInputv1_1(self):
    selector_file_contents = """{
            "file_format_version": 1.1,
            "duration": "30d",
            "metric":"average_rtt",
            "ip_translation":{
                "strategy":"maxmind",
                "params":{
                    "db_snapshots":["2014-08-04"]
                }
            },
            "site":"lga02",
            "client_provider":"comcast",
            "start_time":"2014-02-01T00:00:00Z"
        }"""
    selector_expected = selector.Selector()
    selector_expected.start_time = utils.make_datetime_utc_aware(datetime.datetime(2014, 2, 1))
    selector_expected.duration = 30 * 24 * 60 * 60
    selector_expected.metric = 'average_rtt'
    selector_expected.ip_translation_spec = (
         iptranslation.IPTranslationStrategySpec('maxmind',
                                                 {'db_snapshots': ['2014-08-04']}))
    selector_expected.site_name = 'lga02'
    selector_expected.client_provider = 'comcast'
    self.assertParsedSingleSelectorMatches(selector_expected, selector_file_contents)

  def testInvalidJson(self):
    selector_file_contents = """{
   "file_format_version": 1,
   "duration": "30d",
   "metric":"average_rtt",
   "ip_translation":{
     "strategy":"maxmind",
     "params":{
       "db_snapshots":["2014-08-04"]
     }
   },
    "site":"lga02",
    "client_provider":"comcast",
    "start_time":"2014-02-01T00:00:00Z"
"""
    # The final closing curly brace is missing, so this should fail
    self.assertRaises(ValueError, self.parse_file_contents, selector_file_contents)


if __name__ == '__main__':
  unittest.main()
