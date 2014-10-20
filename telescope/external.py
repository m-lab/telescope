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


import httplib2
import logging
import datetime
import time

from ssl import SSLError

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import AccessTokenRefreshError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow

from httplib import ResponseNotReady

class QueryFailure(Exception):
  def __init__(self, http_code, caught_error):
    self.code = http_code
    Exception.__init__(self, caught_error)

class TableDoesNotExist(Exception):
  def __init__(self):
    Exception.__init__(self)

class APIConfigError(Exception):
  def __init__(self):
    Exception.__init__(self)

class GoogleAPIAuthConfig:
  """ Google API requires an object with preferences for logging and
      authentication. Rather than pass with argparse, for now we manually
      build the default configurations to pass directly. Not guaranteed to
      stay in the future.

  """
  logging_level = 'ERROR'
  noauth_local_webserver = False
  auth_host_port = [8080, 8090]
  auth_host_name = 'localhost'

class GoogleAPIAuth:

  def __init__(self, credentials_filepath, is_headless = False):
    self.logger = logging.getLogger('telescope')
    self.credentials_filepath = credentials_filepath
    self._set_headless_mode(is_headless)
    self.project_id = self._find_project_id_opportunistically()

  def _set_headless_mode(self, is_headless):
    GoogleAPIAuthConfig.noauth_local_webserver = is_headless

  def authenticate_with_google(self):

    flow = flow_from_clientsecrets('client_secrets.json',
                                   scope='https://www.googleapis.com/auth/bigquery')
    storage = Storage(self.credentials_filepath)
    credentials = storage.get()

    http = httplib2.Http()
    if credentials is None or credentials.invalid:
      credentials = run_flow(flow= flow, storage=storage, flags= GoogleAPIAuthConfig, http=http)
      self.logger.info("Successfully authenticated with Google, moving on to building query.")

    http = credentials.authorize(http)

    return build('bigquery', 'v2', http=http)

  def _find_project_id_opportunistically(self):

    authenticated_service = self.authenticate_with_google()
    projects_handler = authenticated_service.projects()
    projects_list = projects_handler.list().execute()

    if projects_list['totalItems'] == 0:
      raise APIConfigError()
    else:
      project_numeric_id = projects_list['projects'][0]['numericId']

    return project_numeric_id

class BigQueryCall:

  def __init__(self, google_auth_config):

    self.logger = logging.getLogger('telescope')

    try:
      self.authenticated_service = google_auth_config.authenticate_with_google()
      self.project_id = google_auth_config.project_id
    except (SSLError, AttributeError, HttpError, httplib2.ServerNotFoundError, ResponseNotReady) as caught_error:
      raise QueryFailure(None, caught_error)

    return None

  def retrieve_job_data(self, job_id, timeout = 0):
    max_results_per_get = 100000
    job_data_to_return = []
    job_collection = self.authenticated_service.jobs()

    query_request = {'projectId': self.project_id,
                      'jobId': job_id,
                      'maxResults':  max_results_per_get,
                      'timeoutMs': timeout}

    while True:
      try:
        query_results_response = job_collection.getQueryResults(**query_request).execute()

        assert query_results_response['jobComplete'] == True, 'IncompleteBigQuery'

        if int(query_results_response['totalRows']) == 0:
          self.logger.warn('BigQuery Report Job Completed, but no rows found. This ' +
                            'is likely due to no data being present for site, ' +
                            'client and time combination. Believing that, I will ' +
                            'produce an empty file. The life of measurement is ' +
                            'solitary, poor, nasty, brutish, and short.')
          break
        else:
          fieldnames = [field['name'] for field in query_results_response['schema']['fields']]

          for results_row in query_results_response['rows']:
            new_results_row = dict(zip(fieldnames, [result_value['v'] for result_value in results_row['f']]))
            job_data_to_return.append(new_results_row)

          if query_results_response.has_key('pageToken'):
            query_request['pageToken'] = query_results_response['pageToken']
            self.logger.debug("Large result, have found {count} iterating with new page token.".format(
                count = len(job_data_to_return)))
          else:
            self.logger.debug("Complete, found {count}.".format(count = len(job_data_to_return)))
            break
      except (SSLError, HttpError, ResponseNotReady) as caught_error:
        if caught_error.resp.status == 404:
          raise TableDoesNotExist()
        elif caught_error.resp.status in [403, 500, 503]:
          raise QueryFailure(caught_error.resp.status, caught_error)
        else:
          self.logger.warn(('Encountered error ({caught_error}) retrieving ' +
                            '{notification_identifier} results, could be temporary, ' +
                            'not bailing out.').format(caught_error = caught_error,
                                                      notification_identifier = job_id))
        time.sleep(10)
      except (Exception, AttributeError, httplib2.ServerNotFoundError) as caught_error:
          self.logger.warn(('Encountered error ({caught_error}) retrieving ' +
                            '{notification_identifier} results, could be temporary, ' +
                            'not bailing out.').format(caught_error = caught_error,
                                                      notification_identifier = job_id))

          time.sleep(10)
    return job_data_to_return

  def run_asynchronous_query(self, query_string, batch_mode = False):
    job_reference_id = None

    if self.project_id is None:
      self.logger.error('Cannot continue since I have not found a project id.')
      return None

    try:
      job_collection = self.authenticated_service.jobs()
      job_definition = {'configuration': {'query': { 'query': query_string }}}

      if batch_mode is True:
        job_definition['configuration']['query']['priority'] = 'BATCH'

      job_collection_insert = job_collection.insert(projectId = self.project_id, body = job_definition).execute()
      job_reference_id = job_collection_insert['jobReference']['jobId']
    except (HttpError, ResponseNotReady) as caught_http_error:
      self.logger.error('HTTP error when running asynchronous query: {error}'.format(
          error = caught_http_error.resp))
    except (Exception, httplib2.ServerNotFoundError) as caught_generic_error:
      self.logger.error('Unknown error when running asynchronous query: {error}'.format(
          error = caught_generic_error))

    return job_reference_id

  def monitor_query_queue(self, job_id, job_metadata, query_object = None, callback_function = None):

    query_object = query_object or self

    if self.project_id is not None:
      started_checking = datetime.datetime.utcnow()

      notification_identifier = "{metric}, {site}, {client_provider}, {date}, {duration}".format(**job_metadata)
      self.logger.info('Queued request for {notification_identifier}, received job id: {job_id}'.format(
          notification_identifier = notification_identifier, job_id = job_id))

      while True:
        try:
          job_collection = query_object.authenticated_service.jobs()
          job_collection_state = job_collection.get(projectId = self.project_id, jobId = job_id).execute()
        except (SSLError, Exception, AttributeError, HttpError, httplib2.ServerNotFoundError) as caught_error:
          self.logger.warn(('Encountered error ({caught_error}) monitoring ' +
                            'for {notification_identifier}, could be temporary, ' +
                            'not bailing out.').format(caught_error = caught_error,
                                                      notification_identifier = notification_identifier))
          job_collection_state = None

        if job_collection_state is not None:
          time_waiting = int((datetime.datetime.utcnow() - started_checking).total_seconds())

          if job_collection_state['status']['state'] == 'RUNNING':
            self.logger.info(('Waiting for {notification_identifier} to complete, spent {time_waiting} '
                              'seconds so far.').format(notification_identifier = notification_identifier,
                                                        time_waiting = time_waiting))
            time.sleep(10)
          elif job_collection_state['status']['state'] == 'PENDING':
            self.logger.info(('Waiting for {notification_identifier} to submit, spent {time_waiting} '
                              'seconds so far.').format(notification_identifier = notification_identifier,
                                                        time_waiting = time_waiting))
            time.sleep(60)
          elif job_collection_state['status']['state'] == 'DONE' and callback_function is not None:
            self.logger.info('Found completion status for {notification_identifier}.'.format(
                notification_identifier = notification_identifier))
            callback_function(job_id, query_object = self)
            break
          else:
            raise Exception('UnknownBigQueryResponse')
    return None
