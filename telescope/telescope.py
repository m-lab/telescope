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

import argparse
import copy
import csv
import datetime
import logging
import os
import Queue
import random
import threading
import time

import external
import iptranslation
import mlab
import query
import selector
import utils

MAX_THREADS = 100


class TelescopeError(Exception):
    pass


class NoClientNetworkBlocksFound(TelescopeError):

    def __init__(self, provider_name):
        Exception.__init__(
            self,
            'Could not find IP blocks associated with client provider %s.' % (
                provider_name))


class MLabServerResolutionFailed(TelescopeError):

    def __init__(self, inner_exception):
        Exception.__init__(self, 'Failed to resolve M-Lab server IPs: %s' % (
            inner_exception.message))


class ExternalQueryHandler(object):
    """Monitors jobs in BigQuery and retrieves their results.

    Monitors external jobs in BigQuery and retrieves and processed the resulting
    data when the job completes.
    """

    def __init__(self, filepath, metadata):
        """Inits ExternalQueryHandler ouput and metadata information.

        Args:
            filepath: (str) Where the processed results will be stored.
            metadata: (dict) Metadata on the query for output labels and further
              processing of received values.
        """
        self._metadata = metadata
        self._filepath = filepath

        self._has_succeeded = False  # Whether the query has returned a result.
        self._has_failed = False  # Whether the query has received a fatal error.

    @property
    def has_succeeded(self):
        """Indicates whether the test has successfully completed."""
        return self._has_succeeded

    @property
    def has_failed(self):
        """Indicates whether the test has encountered a fatal error."""
        return self._has_failed

    def retrieve_data_upon_job_completion(self, job_id, query_object=None):
        """Waits for a BigQuery job to complete, then processes its output.

        Waits for a BigQuery job to complete, then retrieves the data, and
        writes the result to an output data file.

        Args:
          job_id: (str) ID of job for which to retrieve data.
          query_object: (external.BigQueryCall) Query object responsible for
            retrieving data from BigQuery.

        Returns:
          (bool) True if data was successfully retrieved, processed, and written
          to file, False otherwise.
        """
        logger = logging.getLogger('telescope')

        if query_object:
            try:
                bq_query_returned_data = query_object.retrieve_job_data(job_id)
                logger.debug(
                    'Received data, processing according to %s metric.',
                    self._metadata['metric'])

                write_metric_calculations_to_file(self._filepath,
                                                  bq_query_returned_data)
                self._has_succeeded = True
            except (ValueError, external.BigQueryJobFailure,
                    external.BigQueryCommunicationError) as caught_error:
                logger.error(
                    ('Caught {caught_error} for ({site}, {client_provider}, {metric}, '
                     '{date}).').format(caught_error=caught_error,
                                        **
                                        self._metadata))
            except external.TableDoesNotExist:
                logger.error(
                    ('Requested tables for ({site}, {client_provider}, {metric}, {date}'
                     ') do not exist, moving on.').format(
                         **self._metadata))
                self._has_failed = True
        return self._has_succeeded


def setup_logger(verbosity_level=0):
    """Create and configure application logging mechanism.

    Args:
      verbosity_level: (int) Specifies how much information to log. 0 logs
        informational messages and below. Values > 0 log all messages.

    Returns:
      (logging.Logger) Logger object for the application.
    """
    logger = logging.getLogger('telescope')
    console_handler = logging.StreamHandler()
    logger.addHandler(console_handler)

    if verbosity_level > 0:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


def write_metric_calculations_to_file(data_filepath,
                                      metric_calculations,
                                      should_write_header=False):
    """Writes metric data to a file in CSV format.

    Args:
        data_filepath: (str) File path to which to write data.
        metric_calculations: (list) A list of dictionaries containing the values
          of retrieved metrics.
        should_write_header: (bool) Indicates whether the output file should
          contain a header line to identify each column of data.

    Returns:
      (bool) True if the file was written successfully.
    """
    logger = logging.getLogger('telescope')
    try:
        with open(data_filepath, 'w') as data_file_raw:
            if metric_calculations:
                data_file_csv = csv.DictWriter(
                    data_file_raw,
                    fieldnames=metric_calculations[0].keys(),
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_MINIMAL)
                if should_write_header:
                    data_file_csv.writeheader()
                data_file_csv.writerows(metric_calculations)
        return True
    except IOError as caught_error:
        if caught_error.errno == 24:
            logger.error(
                'When writing raw output, caught %s, trying again shortly.',
                caught_error)
            write_metric_calculations_to_file(
                data_filepath, metric_calculations, should_write_header)
            time.sleep(20)
        else:
            logger.error('When writing raw output, caught %s, cannot move on.',
                         caught_error)
    except Exception as caught_error:
        logger.error('When writing raw output, caught %s, cannot move on.',
                     caught_error)
    return False


def write_bigquery_to_file(bigquery_filepath, query_string):
    """Writes BigQuery query string to a file.

    Args:
      bigquery_filepath: (str) Output file path.
      query_string: (str) BigQuery query string to write to file.

    Returns:
      (bool) True if query was written to file successfully, False otherwise.
    """
    logger = logging.getLogger('telescope')
    try:
        with open(bigquery_filepath, 'w') as bigquery_file_raw:
            bigquery_file_raw.write(query_string)
        return True
    except Exception as caught_error:
        logger.error('When writing bigquery, caught %s.', caught_error)

    return False


def selectors_from_files(selector_files):
    """Parses Selector objects from a list of selector files.

    N.B.: Parsing errors are logged, but do not cause the function to fail.

    Args:
      selector_files: (list) A list of filenames of selector files.

    Returns:
      (list) A list of Selector objects that were successfully parsed.
    """
    logger = logging.getLogger('telescope')
    parser = selector.SelectorFileParser()
    selectors = []
    for selector_file in selector_files:
        logger.debug('Attempting to parse selector file at: %s', selector_file)
        try:
            selectors.extend(parser.parse(selector_file))
        except Exception as caught_error:
            logger.error('Failed to parse selector file: %s', caught_error)
            continue
    return selectors


def shuffle_selectors(selectors):
    """Shuffles a list of selectors into random order."""
    selectors_copy = copy.copy(selectors)
    random.shuffle(selectors_copy)
    return selectors_copy


def create_ip_translator(ip_translator_spec):
    factory = iptranslation.IPTranslationStrategyFactory()
    return factory.create(ip_translator_spec)


def generate_query(selector, ip_translator, mlab_site_resolver):
    """Generates BigQuery SQL corresponding to the given Selector object.

    Args:
        selector: (selector.Selector) Selector object that specifies what data to
            retrieve.
        ip_translator: (iptranslation.IPTranslationStrategy) Translator from ASN
            name to associated IP address blocks.
        mlab_site_resolver: (mlab.MLabSiteResolver) Resolver to translate M-Lab
            site IDs to a set of IP addresses.

    Returns:
        (str, int) A 2-tuple containing the query string and the number of tables
        referenced in the query.
    """
    logger = logging.getLogger('telescope')

    start_time_datetime = selector.start_time
    end_time_datetime = start_time_datetime + datetime.timedelta(
        seconds=selector.duration)

    client_ip_blocks = []
    if selector.client_provider:
        client_ip_blocks = ip_translator.find_ip_blocks(
            selector.client_provider)
        if not client_ip_blocks:
            raise NoClientNetworkBlocksFound(selector.client_provider)

    server_ips = []
    if selector.site:
        try:
            retrieved_site_ips = mlab_site_resolver.get_site_ndt_ips(
                selector.site)
            for retrieved_site_ip in retrieved_site_ips:
                server_ips.append(retrieved_site_ip)
                logger.debug('Found IP for %s of %s.', selector.site,
                             retrieved_site_ip)
        except Exception as caught_error:
            raise MLabServerResolutionFailed(caught_error)

    query_generator = query.BigQueryQueryGenerator(
        start_time_datetime,
        end_time_datetime,
        selector.metric,
        server_ips=server_ips,
        client_ip_blocks=client_ip_blocks,
        client_country=selector.client_country)
    return query_generator.query()


def duration_to_string(duration_seconds):
    """Converts a number of seconds into a duration string.

    Serializes an amount of time in seconds to a human-readable string
    representing the time in days, hours, minutes, and seconds.

    Args:
        duration_seconds: (int) Total number of seconds.

    Returns:
        (str) The amount of time represented in a human-readable shorthand
        string.
    """
    duration_string = ''
    remaining_seconds = int(duration_seconds)

    units_per_metric = int(remaining_seconds / (60 * 60 * 24))
    if units_per_metric > 0:
        duration_string += '{0}d'.format(units_per_metric)
        remaining_seconds %= 60 * 60 * 24

    units_per_metric = int(remaining_seconds / (60 * 60))
    if units_per_metric > 0:
        duration_string += '{0}h'.format(units_per_metric)
        remaining_seconds %= 60 * 60

    units_per_metric = int(remaining_seconds / (60))
    if units_per_metric > 0:
        duration_string += '{0}m'.format(units_per_metric)
        remaining_seconds %= 60

    if remaining_seconds != 0:
        duration_string += '{0}s'.format(remaining_seconds)

    return duration_string


def wait_to_respect_thread_limit(concurrent_thread_limit, queue_size):
    """Waits until the number of active threads is lower than the thread limit.

    Waits until the number of active threads (including both background worker
    threads and the main thread) have dropped below the maximum number of
    permitted concurrent threads.

    Args:
        concurrent_thread_limit: (int) Maximum number of permitted concurrent
            threads.

        queue_size: (int) Total number of jobs waiting in work queue.
    """
    logger = logging.getLogger('telescope')
    active_thread_count = threading.activeCount()
    while active_thread_count >= concurrent_thread_limit:
        logger.debug(('Reached thread limit (%d), cooling off. Currently %d '
                      'active threads and %d in queue.'),
                     concurrent_thread_limit, active_thread_count, queue_size)
        time.sleep(20)
        active_thread_count = threading.activeCount()


def process_selector_queue(selector_queue, google_auth_config):
    """Processes the queue of Selector objects waiting for processing.

    Processes the queue of Selector objects by launching BigQuery jobs for each
    Selector and spawning threads to gather the results. Enforces query rate
    limits so that queue processing obeys limits on maximum simultaneous
    threads.

    Args:
        selector_queue: (Queue.Queue) A queue of Selector objects to process.
        google_auth_config: (external.GoogleAPIAuth) Object containing GoogleAPI
            auth data.

    Returns:
        (list) A list of 2-tuples where the first element is the spawned worker
        thread that waits on query results and the second element is the object
        that stores the results of the query.
    """
    logger = logging.getLogger('telescope')
    thread_monitor = []

    while not selector_queue.empty():
        (bq_query_string, thread_metadata, data_filepath,
         _) = selector_queue.get(False)

        try:
            authenticated_service = external.get_authenticated_service(
                google_auth_config)
            bq_query_call = external.BigQueryCall(authenticated_service,
                                                  google_auth_config.project_id)
            bq_job_id = bq_query_call.run_asynchronous_query(bq_query_string)
        except (external.BigQueryJobFailure,
                external.BigQueryCommunicationError) as caught_error:
            logger.warn('Caught request error %s on query, cooling down for a '
                        'minute.', caught_error)
            selector_queue.put((bq_query_string, thread_metadata, data_filepath,
                                True))
            time.sleep(60)
            bq_job_id = None

        if bq_job_id is None:
            logger.warn(
                ('No job id returned for {site} of {metric} (concurrent '
                 'threads: {thread_count}).').format(
                     thread_count=threading.activeCount(),
                     **
                     thread_metadata))
            selector_queue.put((bq_query_string, thread_metadata, data_filepath,
                                True))
            continue

        external_query_handler = ExternalQueryHandler(data_filepath,
                                                      thread_metadata)
        external_query_handler.queue_set = (bq_query_string, thread_metadata,
                                            data_filepath, True)

        new_thread = threading.Thread(
            target=bq_query_call.monitor_query_queue,
            args=(bq_job_id, thread_metadata, None,
                  external_query_handler.retrieve_data_upon_job_completion))
        new_thread.daemon = True
        new_thread.start()
        thread_monitor.append((new_thread, external_query_handler))

        concurrent_thread_limit = MAX_THREADS
        wait_to_respect_thread_limit(concurrent_thread_limit,
                                     selector_queue.qsize())

    return thread_monitor


def main(args):
    selector_queue = Queue.Queue()
    logger = setup_logger(args.verbosity)

    selectors = selectors_from_files(args.selector_in)
    # The selectors were likely provided in order. Shuffle them to get better
    # concurrent distribution on BigQuery tables.
    selectors = shuffle_selectors(selectors)

    ip_translator_factory = iptranslation.IPTranslationStrategyFactory()
    mlab_site_resolver = mlab.MLabSiteResolver()
    for data_selector in selectors:
        thread_metadata = {
            'date': data_selector.start_time.strftime('%Y-%m-%d-%H%M%S'),
            'duration': duration_to_string(data_selector.duration),
            'site': data_selector.site,
            'client_provider': data_selector.client_provider,
            'client_country': data_selector.client_country,
            'metric': data_selector.metric
        }
        data_filepath = utils.build_filename(
            args.output, thread_metadata['date'], thread_metadata['duration'],
            thread_metadata['site'], thread_metadata['client_provider'],
            thread_metadata['client_country'], thread_metadata['metric'],
            '-raw.csv')
        if not args.ignorecache and utils.check_for_valid_cache(data_filepath):
            logger.info(('Raw data file found (%s), assuming this is '
                         'cached copy of same data and moving off. Use '
                         '--ignorecache to suppress this behavior.'),
                        data_filepath)
            continue

        logger.debug('Did not find existing data file: %s', data_filepath)
        logger.debug(
            ('Generating Query for subset of {site}, {client_provider}, '
             '{date}, {duration}.').format(**thread_metadata))

        data_selector.ip_translation_spec.params['maxmind_dir'] = (
            args.maxminddir)

        try:
            ip_translator = ip_translator_factory.create(
                data_selector.ip_translation_spec)
            bq_query_string = generate_query(
                data_selector, ip_translator, mlab_site_resolver)
        except MLabServerResolutionFailed as caught_error:
            logger.error('Failed to resolve M-Lab servers: %s', caught_error)
            # This error is fatal, so bail out here.
            return None
        except Exception as caught_error:
            logger.error('Failed to generate queries: %s', caught_error)
            continue

        if args.savequery:
            bigquery_filepath = utils.build_filename(
                args.output, thread_metadata['date'],
                thread_metadata['duration'], thread_metadata['site'],
                thread_metadata['client_provider'],
                thread_metadata['client_country'], thread_metadata['metric'],
                '-bigquery.sql')
            write_bigquery_to_file(bigquery_filepath, bq_query_string)
        if not args.dryrun:
            # Offer Queue a tuple of the BQ statement, metadata, and a boolean
            # that indicates that the loop has not attempted to run the query
            # thus far (failed queries are pushed back to the end of the loop).
            selector_queue.put((bq_query_string, thread_metadata, data_filepath,
                                False))
        else:
            logger.warn(
                'Dry run flag caught, built query and reached the point that '
                'it would be posted, moving on.')
    try:
        if not args.dryrun:
            logger.info('Finished processing selector files, approximately %d '
                        'queries to be performed.', selector_queue.qsize())
            if os.path.exists(args.credentials_filepath) is False:
                logger.warn(
                    'No credentials for Google appear to exist, next step '
                    'will be an authentication mechanism for its API.')

            try:
                google_auth_config = external.GoogleAPIAuth(
                    args.credentials_filepath,
                    is_headless=args.noauth_local_webserver)
            except external.APIConfigError:
                logger.error(
                    'Could not find developer project, please create one in '
                    'Developer Console to continue. (See README.md)')
                return None

            while not selector_queue.empty():
                thread_monitor = process_selector_queue(selector_queue,
                                                        google_auth_config)

                for (existing_thread, external_query_handler) in thread_monitor:
                    existing_thread.join()
                    # Join together all defined attributes of thread_metadata for a user
                    # friendly notiication string.
                    identifier_string = ', '.join(filter(
                        None, thread_metadata.values()))

                    if (not external_query_handler.has_succeeded and
                            not external_query_handler.has_failed):
                        selector_queue.put(external_query_handler.queue_set)
                    elif external_query_handler.has_failed:
                        logger.debug('Fatal error on %s, moving along.',
                                     identifier_string)
                    else:
                        logger.debug('Successfully retrieved %s.',
                                     identifier_string)

    except KeyboardInterrupt:
        logger.error('Caught interruption, shutting down now.')

    return False


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='M-Lab Telescope',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('selector_in',
                        nargs='+',
                        default=None,
                        help='Selector JSON datafile(s) to parse.')
    parser.add_argument(
        '-v',
        '--verbosity',
        action='count',
        help=('variable output verbosity (e.g., -vv is more than '
              '-v)'))
    parser.add_argument(
        '-o',
        '--output',
        default='processed/',
        help=('Output file path. If the folder does not exist, it'
              ' will be created.'),
        type=utils.create_directory_if_not_exists)
    parser.add_argument('--maxminddir',
                        default='resources/',
                        help='MaxMind GeoLite ASN snapshot directory.')
    parser.add_argument('--savequery',
                        default=False,
                        action='store_true',
                        help=('Save the BigQuery statement to the [output] '
                              'directory as a .sql file.'))
    parser.add_argument('--dryrun',
                        default=False,
                        action='store_true',
                        help=('Run up until the query process (best used with '
                              '--savequery).'))
    parser.add_argument('--ignorecache',
                        default=False,
                        action='store_true',
                        help='Overwrite cached query results if they exist.')
    parser.add_argument(
        '--noauth_local_webserver',
        default=False,
        action='store_true',
        help=('Authenticate to Google using another method than a'
              ' local webserver.'))
    parser.add_argument(
        '--credentialspath',
        dest='credentials_filepath',
        default='bigquery_credentials.dat',
        help=('Google API Credentials. If it does not exist, will'
              ' trigger Google auth.'))

    args = parser.parse_args()
    main(args)
