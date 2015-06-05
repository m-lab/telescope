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

import time
import unittest

import external

import apiclient
import mock


class MockHttpError(apiclient.errors.HttpError):

  def __init__(self, error_code):
    self.resp = mock.Mock()
    self.resp.status = error_code
    self.uri = ''
    self.content = ''

  def __repr__(self):
    return 'Mock HTTP Error code %d' % self.resp.status


def _construct_mock_bigquery_response(mock_rows):
  """Convert a list of result rows to BigQuery's response format.

  Given a list of mock rows, put them in a JSON response that matches BigQuery's
  response format (at least enough to work with BigQueryJobResultCollector).

  Args:
    mock_rows: (list) A list of dicts in which each element represents a result
      row, e.g.:
      [{'foo': 'bar1', 'faz': 'baz1'},
       {'foo': 'bar2', 'faz': 'baz2'},
       ...]

  Returns:
    (dict) A dictionary representing mock_rows in BigQuery response format.
  """
  mock_response = {
      'schema': {
          'fields': [{'name': f} for f in mock_rows[0].keys()]
          }
      }
  mock_response['rows'] = []
  for mock_row in mock_rows:
    value_row = [{'v': v} for v in mock_row.values()]
    mock_response['rows'].append({'f': value_row})
  mock_response['totalRows'] = len(mock_response['rows'])
  return mock_response


class BigQueryJobResultCollectorTest(unittest.TestCase):

  def setUp(self):
    # Mock out calls to time.sleep to keep tests running quickly
    sleep_patch = mock.patch.object(time, 'sleep', autospec=True)
    self.addCleanup(sleep_patch.stop)
    sleep_patch.start()

    self.dummy_job_id = 42
    self.dummy_project_id = 57
    self.mock_jobs_service = mock.Mock()
    self.collector = external.BigQueryJobResultCollector(self.mock_jobs_service,
                                                         self.dummy_project_id)

  def test_single_page_multiple_rows(self):
    mock_result_rows = [
        {'fieldA': 'valueA1', 'fieldB': 'valueB1'},
        {'fieldA': 'valueA2', 'fieldB': 'valueB2'}]
    mock_response = _construct_mock_bigquery_response(mock_result_rows)
    self.mock_jobs_service.getQueryResults().execute.return_value = (
        mock_response)
    rows_expected = mock_result_rows
    rows_actual = self.collector.collect_results(self.dummy_job_id)
    self.assertEqual(rows_expected, rows_actual)

  def test_single_page_no_rows(self):
    self.mock_jobs_service.getQueryResults().execute.return_value = {
        'totalRows': 0}
    rows_expected = []
    rows_actual = self.collector.collect_results(self.dummy_job_id)
    self.assertEqual(rows_expected, rows_actual)

  def test_multiple_pages(self):
    # Create the first response with a page token indicating more results
    mock_result_rows1 = [{'fieldA': 'valueA1', 'fieldB': 'valueB1'},
                         {'fieldA': 'valueA2', 'fieldB': 'valueB2'}]
    mock_response1 = _construct_mock_bigquery_response(mock_result_rows1)
    mock_response1['pageToken'] = 'dummy_page_token'

    # Create the second response with no additional results indicated
    mock_result_rows2 = [{'fieldA': 'valueA3', 'fieldB': 'valueB1'},]
    mock_response2 = _construct_mock_bigquery_response(mock_result_rows2)
    self.mock_jobs_service.getQueryResults().execute.side_effect = (
        mock_response1, mock_response2)

    rows_expected = []
    rows_expected.extend(mock_result_rows1)
    rows_expected.extend(mock_result_rows2)

    rows_actual = self.collector.collect_results(self.dummy_job_id)
    self.assertEqual(rows_expected, rows_actual)

  def test_collector_translates_http_404_to_table_does_not_exist(self):
    self.mock_jobs_service.getQueryResults().execute.side_effect = (
        MockHttpError(404))

    with self.assertRaises(external.TableDoesNotExist):
      self.collector.collect_results(self.dummy_job_id)

  def test_collector_translates_http_400_to_job_failure(self):
    self.mock_jobs_service.getQueryResults().execute.side_effect = (
        MockHttpError(400))

    with self.assertRaises(external.BigQueryJobFailure):
      self.collector.collect_results(self.dummy_job_id)

  def test_collector_ignores_two_http_500_errors(self):
    """Keep retrying if the first two HTTP requests fail."""
    mock_result_rows = [
        {'fieldA': 'valueA1', 'fieldB': 'valueB1'},
        {'fieldA': 'valueA2', 'fieldB': 'valueB2'}]
    mock_response = _construct_mock_bigquery_response(mock_result_rows)
    self.mock_jobs_service.getQueryResults().execute.side_effect = (
        MockHttpError(500), MockHttpError(500), mock_response)
    rows_expected = mock_result_rows
    rows_actual = self.collector.collect_results(self.dummy_job_id)
    self.assertEqual(rows_expected, rows_actual)

  def test_collector_fails_after_five_http_500_errors(self):
    """After 5 HTTP errors, bail out."""
    self.mock_jobs_service.getQueryResults().execute.side_effect = (
        MockHttpError(500))

    with self.assertRaises(external.BigQueryCommunicationError):
      self.collector.collect_results(self.dummy_job_id)
    self.assertEqual(
        5, self.mock_jobs_service.getQueryResults().execute.call_count)


if __name__ == '__main__':
  unittest.main()

