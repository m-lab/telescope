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
import httplib
import httplib2
import logging
import os
import time

from ssl import SSLError

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run_flow


class BigQueryError(Exception):
    pass


class BigQueryJobFailure(BigQueryError):
    """Indicates that a BigQuery job's result was retrieved, but the query failed.

    Raised when BigQuery reports a job has failed. Additional attempts to
    retrieve the job status will not change the result.
    """

    def __init__(self, http_code, cause):
        self.code = http_code
        super(BigQueryJobFailure, self).__init__(cause)


class BigQueryCommunicationError(BigQueryError):
    """An error occurred trying to communicate with BigQuery

    This error is raised when the application fails to communicate with BigQuery.
    It does not indicate that the result of the query itself failed, but rather
    that the result of the query is indeterminate because the application failed
    to retrieve status from BigQuery.
    """

    def __init__(self, message, cause):
        self.cause = cause
        super(BigQueryCommunicationError, self).__init__(
            '%s (%s)' % (message, self.cause))


class TableDoesNotExist(BigQueryError):

    def __init__(self):
        super(TableDoesNotExist, self).__init__()


class APIConfigError(BigQueryError):

    def __init__(self):
        super(APIConfigError, self).__init__()


class GoogleAPIAuthConfig:
    """Google API requires an object with preferences for logging and
    authentication. Rather than pass with argparse, for now we manually
    build the default configurations to pass directly. Not guaranteed to
    stay in the future.

    """
    logging_level = 'ERROR'
    noauth_local_webserver = False
    auth_host_port = [8080, 8090]
    auth_host_name = 'localhost'


class GoogleAPIAuth:

    def __init__(self, credentials_filepath, is_headless=False):
        self.logger = logging.getLogger('telescope')
        self.credentials_filepath = credentials_filepath
        self._set_headless_mode(is_headless)
        self.project_id = self._find_project_id_opportunistically()

    def _set_headless_mode(self, is_headless):
        GoogleAPIAuthConfig.noauth_local_webserver = is_headless

    def authenticate_with_google(self):

        flow = flow_from_clientsecrets(
            os.path.join(os.path.dirname(__file__),
                         'resources/client_secrets.json'),
            scope='https://www.googleapis.com/auth/bigquery')
        storage = Storage(self.credentials_filepath)
        credentials = storage.get()

        http = httplib2.Http()
        if credentials is None or credentials.invalid:
            credentials = run_flow(flow=flow,
                                   storage=storage,
                                   flags=GoogleAPIAuthConfig,
                                   http=http)
            self.logger.info(
                'Successfully authenticated with Google, moving on to building query.')

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


class BigQueryJobResultCollector(object):
    """Collect the results from a BigQuery job."""

    def __init__(self, jobs_service, project_id):
        """Class to collect the results from a BigQuery job when the job completes.

        Args:
            jobs_service: BigQuery jobs service instance.
            project_id: (int) ID of project for which to retrieve results.
        """
        self.logger = logging.getLogger('telescope')
        self._jobs_service = jobs_service
        self._project_id = project_id

    def collect_results(self, job_id):
        """Wait until a job is complete and gather all results.

        Args:
            job_id: (str) Job ID for which to retrieve results.

        Returns:
            (list) A list of result rows from the completed BigQuery job.
        """
        collected_rows = []
        is_first_chunk = True
        page_token = None

        while is_first_chunk or page_token:
            results_response = self._wait_for_results_chunk(job_id, page_token)
            results_chunk, page_token = self._parse_query_results_response(
                results_response)
            collected_rows.extend(results_chunk)
            if page_token:
                self.logger.debug(
                    ('Query contains additional results (found %d rows so'
                     ' far). Fetching additional rows with new page '
                     'token.'), len(collected_rows))
            is_first_chunk = False

        return collected_rows

    def _wait_for_results_chunk(self, job_id, page_token):
        """Retrieve a single chunk of results from a completed BigQuery job.

        Retrieve a fixed number of BigQuery result rows, where the number
        requested may be greater or less than the total number of rows
        available.

        Args:
            job_id: (str) Job ID for which to retrieve results.
            page_token: Token that indicates which page of results for which to
                retrieve results or None to retrieve the first page of results.

        Returns:
            (list) A list of result rows in the current chunk of results.
        """
        MAX_RESULTS_PER_GET = 100000
        query_request = {
            'projectId': self._project_id,
            'jobId': job_id,
            'maxResults': MAX_RESULTS_PER_GET,
            'timeoutMs': 0
        }
        if page_token is not None:
            query_request['pageToken'] = page_token

        retries_remaining = 4
        while True:
            try:
                return self._execute_job_query(query_request)
            except BigQueryCommunicationError as e:
                if retries_remaining > 0:
                    sleep_seconds = 10
                    logging.warning(
                        ('Failed to communicate with BigQuery to retrieve '
                         'job %s. Retrying in %d seconds... (%d attempts '
                         'remaining)'), job_id, sleep_seconds,
                        retries_remaining)
                    time.sleep(sleep_seconds)
                    retries_remaining -= 1
                else:
                    raise e

    def _execute_job_query(self, bq_query):
        """Executes a query to retrieve BigQuery query results.

        Args:
            bq_query: (dict) Parameter set of the query to execute.

        Returns:
            (dict) The result of the query in the form of a dictionary.

        Raises:
            TableDoesNotExist: Query specified a table that does not exist.
            BigQueryJobFailure: The job completed, but the query failed.
            BigQueryCommunicationError: Could not communicate with BigQuery.
        """
        try:
            return self._jobs_service.getQueryResults(**bq_query).execute()
        except HttpError as e:
            if e.resp.status == 404:
                raise TableDoesNotExist()
            elif e.resp.status == 400:
                raise BigQueryJobFailure(e.resp.status, e)
            else:
                raise BigQueryCommunicationError(
                    'Failed to communicate with BigQuery', e)
        except Exception as e:
            raise BigQueryCommunicationError(
                'Failed to communicate with BigQuery', e)

    def _parse_query_results_response(self, results_response):
        """Parse the response dictionary from BigQuery.

        Parse the response dictionary from BigQuery into result rows and a page
        token.

        Args:
            results_response: A response dictionary from BigQuery representing the
                results of a BigQuery query job.

        Returns:
          (list, token) A two-tuple where the first element is a list of result
          rows in the format:

            [{'fieldA': 'valueA1', 'fieldB': 'valueB1'}, # row 1
             {'fieldA': 'valueA2', 'fieldB': 'valueB2'}] # row 2

          and the second element is a page token indicating the next page of
          results or None if there are no more results available.
        """
        parsed_rows = []
        if int(results_response['totalRows']) == 0:
            self.logger.warn(
                'BigQuery query completed successfully, but result '
                'contained no rows.')
            return parsed_rows, None

        fields = [field['name']
                  for field in results_response['schema']['fields']]

        for results_row in results_response['rows']:
            # yapf: disable
            parsed_row = dict(zip(
                fields,
                [result_value['v'] for result_value in results_row['f']]))
            # yapf: enable
            parsed_rows.append(parsed_row)

        if 'pageToken' in results_response:
            page_token = results_response['pageToken']
        else:
            page_token = None

        return parsed_rows, page_token


def get_authenticated_service(google_auth_config):
    try:
        authenticated_service = google_auth_config.authenticate_with_google()
    except (SSLError, HttpError, httplib2.ServerNotFoundError,
            httplib.ResponseNotReady) as e:
        raise BigQueryCommunicationError(
            'Failed to communicate with BigQuery during authentication', e)

    return authenticated_service


class BigQueryCall(object):

    def __init__(self, authenticated_service, project_id):
        self.logger = logging.getLogger('telescope')
        self._authenticated_service = authenticated_service
        self._project_id = project_id

    def retrieve_job_data(self, job_id):
        result_collector = BigQueryJobResultCollector(
            self._authenticated_service.jobs(), self._project_id)
        return result_collector.collect_results(job_id)

    def run_asynchronous_query(self, query_string):
        job_reference_id = None

        try:
            job_collection = self._authenticated_service.jobs()
            job_definition = {
                'configuration': {'query': {'query': query_string}}
            }

            job_collection_insert = job_collection.insert(
                projectId=self._project_id,
                body=job_definition).execute()
            job_reference_id = job_collection_insert['jobReference']['jobId']
        except (HttpError, httplib.ResponseNotReady) as e:
            raise BigQueryCommunicationError(
                'Failed to communicate with BigQuery', e)

        return job_reference_id

    def monitor_query_queue(self,
                            job_id,
                            job_metadata,
                            query_object=None,
                            callback_function=None):

        query_object = query_object or self

        started_checking = datetime.datetime.utcnow()

        notification_identifier = ', '.join(filter(None, job_metadata.values()))
        self.logger.info('Queued request for %s, received job id: %s',
                         notification_identifier, job_id)

        while True:
            try:
                job_collection = query_object._authenticated_service.jobs()
                job_collection_state = job_collection.get(
                    projectId=self._project_id,
                    jobId=job_id).execute()
            except (SSLError, Exception, AttributeError, HttpError,
                    httplib2.ServerNotFoundError) as caught_error:
                self.logger.warn(
                    'Encountered error (%s) monitoring for %s, could '
                    'be temporary, not bailing out.', caught_error,
                    notification_identifier)
                job_collection_state = None

            if job_collection_state is not None:
                time_waiting = int((datetime.datetime.utcnow() -
                                    started_checking).total_seconds())

                if job_collection_state['status']['state'] == 'RUNNING':
                    self.logger.info(
                        'Waiting for %s to complete, spent %d seconds so '
                        'far.', notification_identifier, time_waiting)
                    time.sleep(10)
                elif job_collection_state['status']['state'] == 'PENDING':
                    self.logger.info(
                        'Waiting for %s to submit, spent %d seconds so '
                        'far.', notification_identifier, time_waiting)
                    time.sleep(60)
                elif (
                    (job_collection_state['status']['state'] == 'DONE') and
                        callback_function is not None):
                    self.logger.info('Found completion status for %s.',
                                     notification_identifier)
                    callback_function(job_id, query_object=self)
                    break
                else:
                    raise Exception('UnknownBigQueryResponse')
        return None
