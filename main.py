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
import json
import csv
import os
import socket
import logging
import datetime
import threading
import time
import Queue

from ssl import SSLError

import telescope.selector
import telescope.query
import telescope.metrics_math
import telescope.mlab
import telescope.filters
import telescope.external


class NoClientNetworkBlocksFound(Exception):
  def __init__(self, provider_name):
    Exception.__init__(self,
                       'Could not find IP blocks associated with client provider {client_provider}.'.format(
                           client_provider = provider_name))

class MLabServerResolutionFailed(Exception):
  def __init__(self, inner_exception):
    Exception.__init__(self,
                       'Failed to resolve M-Lab server IPs: {error_message}'.format(
                           error_message = inner_exception.message))

class ExternalQueryHandler:
  """ Monitors external jobs in BigQuery and retrieves and processed the
      resulting data when the job completes.
  """

  def __init__(self):
    self.result = False
    self.metadata = None
    self.fatal_error = None

  def retrieve_data_upon_job_completion(self, job_id, query_object = None):
    """ Waits for a BigQuery job to complete, then retrieves the data, runs
        appropriate filtering on the data, and writes the result to an output
        data file.

        Args:
          job_id (str): ID of job for which to retrieve data.

          query_object (telescope.external.BigQueryCall): Query object
            responsible for retrieving data from BigQuery.

        Returns:
          (bool) True if data was successfully retrieved, processed, and
          written to file, False otherwise.
    """
    logger = logging.getLogger('telescope')
    self.result = False

    if query_object is not None:
      try:
        bq_query_returned_data = query_object.retrieve_job_data(job_id)
        logger.debug('Received data, processing according to {metric} metric.'.format(metric = self.metadata['metric']))

        validation_results = telescope.filters.filter_measurements_list(
            self.metadata['metric'], bq_query_returned_data)
        number_kept = len(validation_results)
        number_discarded = len(bq_query_returned_data) - len(validation_results)
        logger.info(("Filtered measurements, kept {number_kept} and discarded " +
                      "{number_discarded}.").format(number_kept = number_kept,
                                                  number_discarded = number_discarded))

        subset_metric_calculations = telescope.metrics_math.calculate_results_list(
            self.metadata['metric'], validation_results)

        write_metric_calculations_to_file(self.metadata['data_filepath'], subset_metric_calculations)
        self.result = True
      except (ValueError, telescope.external.QueryFailure) as caught_error:
        logger.error("Caught {caught_error} for ({site}, {client_provider}, {metric}).".format(
            caught_error = caught_error, site = self.metadata['site'],
            client_provider = self.metadata['client_provider'], metric = self.metadata['metric']))
      except telescope.external.TableDoesNotExist:
        logger.error(("Requested tables for ({site}, {client_provider}, " +
                      "{metric}) do not exist, moving on.").format( site = self.metadata['site'],
                          client_provider = self.metadata['client_provider'], metric = self.metadata['metric']))
        self.fatal_error = True
    return self.result


def setup_logger(verbosity_level = 0):
  """ Create and configure application logging mechanism.

      Args:
        verbosity_level (int): Specifies how much information to log. 0 logs
        informational messages and below. Values > 0 log all messages.

      Returns:
        (logging.Logger): Logger object for the application.
  """
  logger = logging.getLogger('telescope')
  console_handler = logging.StreamHandler()
  logger.addHandler(console_handler)

  if verbosity_level > 0:
    logger.setLevel(logging.DEBUG)
  else:
    logger.setLevel(logging.INFO)
  return logger


def create_directory_if_not_exists(directory_name):
  if not os.path.exists(directory_name):
    try:
      os.makedirs(directory_name)
    except OSError:
      raise argparse.ArgumentError(('{0} does not exist, is not readable or '
                                    'could not be created.').format(directory_name))
  return directory_name


def write_metric_calculations_to_file(data_filepath, metric_calculations, should_write_header = False):
  """ Writes metric data to a file in CSV format.

      Args:
        data_filepath (str): File path to which to write data.

        metric_calculations (list): A list of dictionaries containing the
        values of retrieved metrics.

        should_write_header (bool): Indicates whether the output file should
        contain a header line to identify each column of data.

      Returns:
        (bool) True if the file was written successfully, False otherwise.
  """
  logger = logging.getLogger('telescope')
  try:
    with open(data_filepath, 'w') as data_file_raw:
      if type(metric_calculations) is list and len(metric_calculations) > 0:
        data_file_csv = csv.DictWriter(data_file_raw,
                                        fieldnames = metric_calculations[0].keys(),
                                        delimiter=',',
                                        quotechar='"', quoting=csv.QUOTE_MINIMAL)
        if should_write_header == True:
          data_file_csv.writeheader()
        data_file_csv.writerows(metric_calculations )
    return True
  except IOError as caught_error:
    if caught_error.errno == 24:
      logger.error(("When writing raw output, caught {error}, " +
                      "trying again shortly.").format(error = caught_error))
      write_metric_calculations_to_file(data_filepath, metric_calculations, should_write_header)
      time.sleep(20)
    else:
      logger.error(("When writing raw output, caught {error}, " +
                      "cannot move on.").format(error = caught_error))
  except Exception as caught_error:
    logger.error(("When writing raw output, caught {error}, " +
                    "cannot move on.").format(error = caught_error))
  return False


def build_filename(resource_type, outpath, date, duration, site, client_provider, metric):
  """ Builds an output filename that reflects the data being written to file.

      Args:
        resource_type (str): Indicates what type of data will be stored in the
        file.

        outpath (str): Indicates the path (excluding filename) where the file
        will be written.

        date (str): A string indicating the start time of the data window the
        file represents.

        duration (str): A string indicating the duration of the data window the
        file represents.

        site (str): The name of the M-Lab site from which the data was collected
        (e.g. lga01)

        client_provider (str): The name of the client provider associated with
        the test results.

        metric (str): The name of the metric this data represents (e.g.
        download_throughput).

     Returns:
       (str): The generated full pathname of the output file.
  """
  extensions = { 'data': 'raw.csv', 'bigquery': 'bigquery.sql'}
  filename_format = "{date}+{duration}_{site}_{client_provider}_{metric}-{extension}"

  filename = filename_format.format(date = date,
                                    duration = duration,
                                    site = site,
                                    client_provider = client_provider,
                                    metric = metric,
                                    extension = extensions[resource_type])
  filepath = os.path.join(outpath, filename)
  return filepath


def write_bigquery_to_file(bigquery_filepath, query_string):
  """ Writes BigQuery query string to a file.

      Args:
        bigquery_filepath (str): Output file path.

        query_string (str): BigQuery query string to write to file.

      Returns:
        (bool) True if query was written to file successfully, False otherwise.
  """
  logger = logging.getLogger('telescope')
  try:
    with open(bigquery_filepath, 'w') as bigquery_file_raw:
      bigquery_file_raw.write(query_string)
    return True
  except Exception as caught_error:
    logger.error("When writing bigquery, caught {error}.".format(error = caught_error))

  return False


def selectors_from_files(selector_files):
  """ Parses Selector objects from a list of selector files.

      N.B.: Parsing errors are logged, but do not cause the function to fail.

      Args:
        slector_files (list): A list of filenames of selector files.

      Returns:
        (list): A list of Selector objects that were successfully parsed.
  """
  logger = logging.getLogger('telescope')
  parser = telescope.selector.SelectorFileParser()
  selectors = []
  for selector_file in selector_files:
    logger.debug('Attempting to parse selector file at: %s', selector_file)
    try:
      selectors.extend(parser.parse(selector_file))
    except Exception as caught_error:
      logger.error('Failed to parse selector file: %s', caught_error)
      continue
  return selectors

def create_ip_translator(ip_translator_spec):
  factory = telescope.iptranslation.IPTranslationStrategyFactory()
  return factory.create(ip_translator_spec)

def generate_query(selector, ip_translator, mlab_site_resolver):
  """ Generates the query string necessary to retrieve the data specified in a
      selector object.

      Args:
        selector (telescope.selector.Selector): Selector object that specifies what
        data to retrieve.

        ip_translator (telescope.iptranslation.IPTranslationStrategy): Translator from
        ASN name to associated IP address blocks.

        mlab_site_resolver (telescope.mlab.MLabSiteResolver): Resolver to translate M-Lab
        site IDs to a set of IP addresses.

      Returns:
        (str, int): A 2-tuple containing the query string and the number of tables
        referenced in the query.
  """
  logger = logging.getLogger('telescope')

  start_time_datetime = selector.start_time
  end_time_datetime = start_time_datetime + datetime.timedelta(seconds = selector.duration)

  network_lookup_found_blocks = ip_translator.find_ip_blocks(
      selector.client_provider)
  if len(network_lookup_found_blocks) == 0:
    raise NoClientNetworkBlocksFound(selector.client_provider)

  server_ips = []
  try:
    for retrieved_site_ip in mlab_site_resolver.get_site_ips(selector.site_name,
                                                             mlab_project = selector.mlab_project):
      server_ips.append(retrieved_site_ip)
      logger.debug("Found IP for {site} of {site_ip} on test {test}.".format(
          site=selector.site_name, site_ip = retrieved_site_ip, test = selector.mlab_project))
  except Exception as caught_error:
    raise MLabServerResolutionFailed(caught_error)

  query_generator = telescope.query.BigQueryQueryGenerator(start_time_datetime,
                                                     end_time_datetime,
                                                     selector.metric,
                                                     selector.mlab_project,
                                                     server_ips,
                                                     network_lookup_found_blocks)
  return (query_generator.query(), query_generator.table_span())

def duration_to_string(duration_seconds):
  """ Serializes an amount of time in seconds to a human-readable string
      representing the time in days, hours, minutes, and seconds.

      Args:
        duration_seconds (int): Total number of seconds.

      Returns:
        (str): The amount of time represented in a human-readable shorthand
        string.

  """
  duration_string = ''
  remaining_seconds = int(duration_seconds)

  units_per_metric = int(remaining_seconds / (60*60*24))
  if units_per_metric > 0:
    duration_string += '{0}d'.format(units_per_metric)
    remaining_seconds = remaining_seconds % (60*60*24)

  units_per_metric = int(remaining_seconds / (60*60))
  if units_per_metric > 0:
    duration_string += '{0}h'.format(units_per_metric)
    remaining_seconds = remaining_seconds % (60*60)

  units_per_metric = int(remaining_seconds / (60))
  if units_per_metric > 0:
    duration_string += '{0}m'.format(units_per_metric)
    remaining_seconds = remaining_seconds % (60)

  if remaining_seconds != 0:
    duration_string += '{0}s'.format(remaining_seconds)

  return duration_string

def process_selector_queue(selector_queue, google_auth_config, batchmode = 'automatic', max_tables_without_batch = 2, concurrent_thread_limit = 18):
  """ Processes the queue of Selector objects by launching BigQuery jobs for
      each Selector and spawning threads to gather the results. Enforces query
      rate limits so that queue processing obeys limits on maximum simultaneous
      threads.

      Args:
        selector_queue (Queue.Queue): A queue of Selector objects to process.

        google_auth_config (external.GoogleAPIAuth): Object containing
        GoogleAPI auth data.

        batchmode (str): Indicates the batch mode to operate under.

        max_tables_without_batch (int): When batchmode is set to 'query', this
        indicates the maximum number of database tables that can appear in the
        SELECT portion of a query before the job is automatically converted to
        batch mode.

        concurrent_thread_limit: Indicates the maximum number of threads to run
        when batchmode is not set to 'all'.

      Returns:
        (list): A list of 2-tuples where the first element is the spawned
        worker thread that waits on query results and the second element is the
        object that stores the results of the query.
  """
  logger = logging.getLogger('telescope')
  thread_monitor = []

  while not selector_queue.empty():
    bq_query_string, bq_table_span, thread_metadata, has_been_run = selector_queue.get(False)

    """
      Enforce concurrent rate limit and allow fine-grain controls over batch
      mode. Aggressively batching also allows a fire-everything-and-wait
      strategy, so we will skip the queue rate.
    """
    max_tables_without_batch = 2
    if batchmode == 'all':
      is_batched_query = True
    elif batchmode == 'automatic' and bq_table_span > max_tables_without_batch:
      logger.info(("Found {0} tables, when the maximum for non-batched " +
                    "mode is {1}, setting query to batched mode (this will " +
                    "increase the amount of time but lower failure rate). " +
                    "This behavior can be controled with the --batchmode " +
                    "argument.").format(bq_table_span, max_tables_without_batch))
      is_batched_query = True
    else:
      is_batched_query = False

    try:
      bq_query_call = telescope.external.BigQueryCall(google_auth_config)
      bq_job_id = bq_query_call.run_asynchronous_query(bq_query_string, batch_mode = is_batched_query)
    except (SSLError, telescope.external.QueryFailure) as caught_error:
      logger.warn(("Caught request error {caught_error} on query, cooling " +
                    "down for a minute.").format(caught_error = caught_error))
      selector_queue.put( (bq_query_string, bq_table_span, thread_metadata, True) )
      time.sleep(60)
      bq_job_id = None

    if bq_job_id is None:
      logger.warn(("No job id returned for {site} of {metric} (concurrent threads: " +
                    "{thread_count}).").format(thread_count = threading.activeCount(), **thread_metadata))
      selector_queue.put( (bq_query_string, bq_table_span, thread_metadata, True) )
      continue

    external_query_handler = ExternalQueryHandler()
    external_query_handler.queue_set = (bq_query_string, bq_table_span, thread_metadata, True)
    external_query_handler.metadata = thread_metadata
    new_thread = threading.Thread(target=bq_query_call.monitor_query_queue,
                                    args = (bq_job_id, thread_metadata, None,
                                            external_query_handler.retrieve_data_upon_job_completion))
    new_thread.daemon = True
    new_thread.start()
    thread_monitor.append( (new_thread, external_query_handler) )

    if batchmode != 'all':
      while threading.activeCount() >= concurrent_thread_limit:
        logger.debug(("Reached thread limit ({thread_limit}), cooling off. Currently " +
                      "{thread_count} active threads and {queue_size} in queue.").format(thread_limit = concurrent_thread_limit,
                                                      thread_count = threading.activeCount(),
                                                      queue_size = selector_queue.qsize()))
        time.sleep(20)
  return thread_monitor

def main(args):

  selector_queue = Queue.Queue()
  logger = setup_logger(args.verbosity)

  selectors = selectors_from_files(args.selector_in)
  ip_translator_factory = telescope.iptranslation.IPTranslationStrategyFactory()
  mlab_site_resolver = telescope.mlab.MLabSiteResolver()
  for selector in selectors:
    thread_metadata = {
                      'date': selector.start_time.strftime('%Y-%m-%d-%H%M%S'),
                      'duration': duration_to_string(selector.duration),
                      'site': selector.site_name,
                      'client_provider': selector.client_provider,
                      'metric': selector.metric,
                      'mlab_project': selector.mlab_project,
                    }
    thread_metadata['data_filepath'] = build_filename('data',
                                                      args.output,
                                                      thread_metadata['date'],
                                                      thread_metadata['duration'],
                                                      thread_metadata['site'],
                                                      thread_metadata['client_provider'],
                                                      thread_metadata['metric'])
    if (args.ignorecache is False and
        telescope.utils.check_for_valid_cache(thread_metadata['data_filepath']) is True):
      logger.info(('Raw data file found ({data_filepath}), assuming this is cached copy of same data and ' +
                   'moving off. Use --ignorecache to suppress this behavior.').format(**thread_metadata))
      continue

    logger.debug('Did not find existing data file: {data_filepath}'.format(**thread_metadata))
    logger.debug(('Generating Query for subset of {site}, {client_provider}, {date}, ' +
                  '{duration}.').format(**thread_metadata))

    selector.ip_translation_spec.params['maxmind_dir'] = args.maxminddir

    try:
      ip_translator = ip_translator_factory.create(selector.ip_translation_spec)
      bq_query_string, bq_table_span = generate_query(selector, ip_translator, mlab_site_resolver)
    except MLabServerResolutionFailed as caught_error:
      logger.error('Failed to resolve M-Lab servers: %s', caught_error)
      # This error is fatal, so bail out here.
      return None
    except Exception as caught_error:
      logger.error('Failed to generate queries: %s', caught_error)
      continue

    if args.savequery == True:
      bigquery_filepath = build_filename('bigquery',
                                         args.output,
                                         thread_metadata['date'],
                                         thread_metadata['duration'],
                                         thread_metadata['site'],
                                         thread_metadata['client_provider'],
                                         thread_metadata['metric'])
      write_bigquery_to_file(bigquery_filepath, bq_query_string)
    if args.dryrun is False:
      """ Offer Queue a tuple of the BQ statement, BQ table span, metadata,
          and a boolean that indicates that the loop has not attempted to
          run the query thus far (failed queries are pushed back to the end
          of the loop).
      """
      selector_queue.put( (bq_query_string, bq_table_span, thread_metadata, False) )
    else:
      logger.warn('Dry run flag caught, built query and reached the point that it would be posted, ' +
                  'moving on.')
  try:
    if args.dryrun is False:
      logger.info(("Finished processing selector files, approximately {0} queries " +
                    "to be performed.").format(selector_queue.qsize()))
      if os.path.exists(args.credentials_filepath) is False:
        logger.warn('No credentials for Google appear to exist, next step will be an authentication ' +
                    'mechanism for its API.')

      try:
        google_auth_config = telescope.external.GoogleAPIAuth(
            args.credentials_filepath, is_headless = args.noauth_local_webserver)
      except telescope.external.APIConfigError:
        logger.error("Could not find developer project, please create one in " +
                          "Developer Console to continue. (See README.md)")
        return None

      while not selector_queue.empty():

        thread_monitor = process_selector_queue(selector_queue, google_auth_config, batchmode = args.batchmode)

        for (existing_thread, external_query_handler) in thread_monitor:
          existing_thread.join()
          if external_query_handler.result != True and external_query_handler.fatal_error != True:
            selector_queue.put( external_query_handler.queue_set )
          elif external_query_handler.result != True and external_query_handler.fatal_error == True:
            logger.debug(('Fatal error on {site}, {client_provider}, {date}, ' +
                '{duration}, moving along.').format(**thread_metadata))
          else:
            logger.debug(('Successfully retrieved {site}, {client_provider}, {date}, ' +
                          '{duration}.').format(**thread_metadata))

  except KeyboardInterrupt:
    logger.error("Caught Interruption, Shutting Down Now.")

  return False

if __name__ == "__main__":
  parser = argparse.ArgumentParser(
      prog='M-Lab Telescope',
      formatter_class=argparse.ArgumentDefaultsHelpFormatter)

  parser.add_argument('selector_in', nargs='+', default=None,
                        help='Selector JSON datafile(s) to parse.')
  parser.add_argument('-v', '--verbosity', action="count",
                        help="variable output verbosity (e.g., -vv is more than -v)")
  parser.add_argument('-o', '--output', default='processed/',
                        help='Output file path. If the folder does not exist, it will be created.',
                        type=create_directory_if_not_exists)
  parser.add_argument('--maxminddir', default='resources/', help='MaxMind GeoLite ASN snapshot directory.')
  parser.add_argument('--savequery', default=False, action='store_true',
                        help='Save the BigQuery statement to the [output] directory as a .sql')
  parser.add_argument('--dryrun', default=False, action='store_true',
                        help='Run up until the query process (best used with --savequery).')
  parser.add_argument('--ignorecache', default=False, action='store_true',
                        help='Overwrite cached query results if they exist.')
  parser.add_argument('--noauth_local_webserver', default=False, action='store_true',
                        help='Authenticate to Google using another method than a local webserver')
  parser.add_argument('--batchmode', default='automatic', choices=['all', 'automatic', 'none'],
                        help='Control how batch mode is used to query BigQuery.')
  parser.add_argument('--credentialspath', dest='credentials_filepath', default='bigquery_credentials.dat',
                      help='Google API Credentials. If it does not exist, will trigger Google auth.')

  args = parser.parse_args()
  main(args)
