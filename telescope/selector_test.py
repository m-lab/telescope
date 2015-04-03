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
import json
import unittest
import itertools

import iptranslation
import selector
import utils


class SelectorFileParserTest(unittest.TestCase):

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
    self.assertEqual(selector_expected.site, selector_actual.site)
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
           "metrics":"average_rtt",
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
              "metrics":"average_rtt",
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
  
  def testValidInput_v1dot1_Simple(self):
    selector_file_contents = """{
            "file_format_version": 1.1,
            "duration": "30d",
            "metrics": ["average_rtt"],
            "ip_translation":{
                "strategy":"maxmind",
                "params":{
                    "db_snapshots":["2014-08-04"]
                }
            },
            "sites": ["lga02"],
            "client_providers": ["comcast"],
            "start_times": ["2014-02-01T00:00:00Z"]
        }"""
    selector_expected = selector.Selector()
    selector_expected.start_time = utils.make_datetime_utc_aware(datetime.datetime(2014, 2, 1))
    selector_expected.duration = 30 * 24 * 60 * 60
    selector_expected.metric = 'average_rtt'
    selector_expected.ip_translation_spec = (
         iptranslation.IPTranslationStrategySpec('maxmind',
                                                 {'db_snapshots': ['2014-08-04']}))
    selector_expected.site = 'lga02'
    selector_expected.client_provider = 'comcast'
    self.assertParsedSingleSelectorMatches(selector_expected, selector_file_contents)

  def testValidInput_v1dot1_Complex(self):
    selector_file_contents = """{
            "file_format_version": 1.1,
            "duration": "30d",
            "metrics": ["minimum_rtt", "download_throughput", "average_rtt"],
            "ip_translation":{
                "strategy":"maxmind",
                "params":{
                    "db_snapshots":["2014-08-04"]
                }
            },
            "sites": ["lga01", "lga02"],
            "client_providers": ["comcast", "verizon"],
            "start_times": ["2014-02-01T00:00:00Z"]
        }"""
    
    selectors_expected = []
    selector_base = selector.Selector()
    selector_base.start_time = utils.make_datetime_utc_aware(datetime.datetime(2014, 2, 1))
    selector_base.duration = 30 * 24 * 60 * 60
    selector_base.ip_translation_spec = (
         iptranslation.IPTranslationStrategySpec('maxmind',
                                                 {'db_snapshots': ['2014-08-04']}))
    sites = ['lga01', 'lga02']
    client_providers = ['comcast', 'verizon']
    metrics = ['minimum_rtt', 'download_throughput', 'average_rtt']

    for client_provider, site, metric in itertools.product(client_providers, sites, metrics):
      selector_copy = copy.copy(selector_base)
      selector_copy.metric = metric
      selector_copy.client_provider = client_provider
      selector_copy.site = site
      selectors_expected.append(selector_copy)

    self.assertParsedSelectorsMatch(selectors_expected, selector_file_contents)

  def testInvalidJson(self):
    selector_file_contents = """{
   "file_format_version": 1,
   "duration": "30d",
   "metrics":"average_rtt",
   "ip_translation":{
     "strategy":"maxmind",
     "params":{
       "db_snapshots":["2014-08-04"]
     }
   },
    "sites":"lga02",
    "client_providers":"comcast",
    "start_times":"2014-02-01T00:00:00Z"
"""
    # The final closing curly brace is missing, so this should fail
    self.assertRaises(ValueError, self.parse_file_contents, selector_file_contents)


class SelectorJsonEncoderTest(unittest.TestCase):

  def assertJsonEqual(self, expected, actual):
    self.assertDictEqual(json.loads(expected), json.loads(actual))

  def testEncodeSimpleSelectorA(self):
    s = selector.Selector()
    s.start_time = datetime.datetime(2015, 4, 2, 10, 27, 34)
    s.duration = 45
    s.site_name = 'mia01'
    s.client_provider = 'twc'
    s.metric = 'upload_throughput'
    s.ip_translation_spec = (iptranslation.IPTranslationStrategySpec(
        'maxmind', {'db_snapshots': ['2015-02-05']}))

    encoded_expected = """
{
  "file_format_version": 1.0,
  "duration": "45d",
  "metric":"upload_throughput",
  "ip_translation":{
    "strategy":"maxmind",
    "params":{
      "db_snapshots":["2015-02-05"]
    }
  },
  "subsets":[
     {
        "site":"mia01",
        "client_provider":"twc",
        "start_time":"2015-04-02T10:27:34Z"
     }
  ]
}"""
    encoded_actual = selector.SelectorJsonEncoder().encode(s)
    self.assertJsonEqual(encoded_expected, encoded_actual)

  def testEncodeSimpleSelectorB(self):
    s = selector.Selector()
    s.start_time = datetime.datetime(2014, 2, 1)
    s.duration = 22
    s.site_name = 'lga02'
    s.client_provider = 'comcast'
    s.metric = 'download_throughput'
    s.ip_translation_spec = (iptranslation.IPTranslationStrategySpec(
        'maxmind', {'db_snapshots': ['2014-08-04']}))

    encoded_expected = """
{
  "file_format_version": 1.0,
  "duration": "22d",
  "metric":"download_throughput",
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
    encoded_actual = selector.SelectorJsonEncoder().encode(s)
    self.assertJsonEqual(encoded_expected, encoded_actual)


if __name__ == '__main__':
  unittest.main()
