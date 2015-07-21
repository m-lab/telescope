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
import logging

from dateutil import rrule
import dateutil.relativedelta
import utils


class BigQueryQueryGenerator(object):

  database_name = 'plx.google'
  table_format = '[{database_name}:m_lab.{table_date}.all]'

  def __init__(self, start_time, end_time, metric, server_ips,
               client_ip_blocks):
    self.logger = logging.getLogger('telescope')
    self._select_list = self._build_select_list(metric)
    self._table_list = self._build_table_list(start_time, end_time)
    self._conditional_dict = {}
    self._add_data_direction_conditional(metric)
    self._add_log_time_conditional(start_time, end_time)
    self._add_client_network_blocks_conditional(client_ip_blocks)
    self._add_server_ips_conditional(server_ips)
    self._query = self._create_query_string()

  def query(self):
    return self._query

  def table_span(self):
    return len(self._table_list)

  def _build_table_list(self, start_time, end_time):
    """Enumerates monthly BigQuery tables covered between two datetime objects.

    Args:
      start_time: (datetime) Start date that the queried tables should cover.
      end_time: (datetime) End date that the queried tables should cover.
    Returns:
      list: List of M-Lab tables covering the dates passed to function.

    Notes:
      * Rounds the start down to the first day of month, and rounds the end to
        the last day, so that rrule's enumeration of months does not fall short
        due to the duration of the search being less that the length of a
        month. The latter occurs through rounding the end date down to the
        first second of the first day of that month, using relative delta to
        add another month to the date and then subtracting one second.

      * Between these two periods, rrule enumerates datetime objects that we
        use to build table names from the class-defined string format.
    """
    table_names = []

    start_time_fixed = datetime.datetime(start_time.year, start_time.month, 1)
    end_time_inclusive = end_time - datetime.timedelta(seconds=1)
    end_time_fixed = (datetime.datetime(end_time_inclusive.year,
                                        end_time_inclusive.month, 1) +
                      dateutil.relativedelta.relativedelta(months=1) -
                      datetime.timedelta(seconds=1))

    months = rrule.rrule(rrule.MONTHLY, dtstart=start_time_fixed).between(
            start_time_fixed, end_time_fixed, inc=True)
    for iterated_month in months:
      iterated_table = BigQueryQueryGenerator.table_format.format(
          database_name=self.database_name,
          table_date=iterated_month.strftime('%Y_%m'))
      table_names.append(iterated_table)

    return table_names

  def _build_select_list(self, metric):

    metric_names_to_return = set(['web100_log_entry.log_time',
                                  'connection_spec.data_direction',
                                  'web100_log_entry.snap.State'
                                 ])
    metric_data_directions = {
        's2c': ['web100_log_entry.snap.HCThruOctetsAcked',
                'web100_log_entry.snap.SndLimTimeRwin',
                'web100_log_entry.snap.SndLimTimeCwnd',
                'web100_log_entry.snap.SndLimTimeSnd',
                'web100_log_entry.snap.CongSignals'
               ],
        'c2s': ['web100_log_entry.snap.HCThruOctetsReceived',
                'web100_log_entry.snap.Duration'
               ]
        }

    metric_types = {
        'upload_throughput': metric_data_directions['c2s'],
        'download_throughput': metric_data_directions['s2c'],
        'minimum_rtt': (['web100_log_entry.snap.MinRTT',
                         'web100_log_entry.snap.CountRTT'] +
                        metric_data_directions['s2c']),
        'average_rtt': (['web100_log_entry.snap.SumRTT',
                         'web100_log_entry.snap.CountRTT'] +
                        metric_data_directions['s2c']),
        'packet_retransmit_rate': (['web100_log_entry.snap.SegsRetrans',
                                    'web100_log_entry.snap.DataSegsOut'] +
                                   metric_data_directions['s2c']),
    }

    if metric == 'all':
      for metric_names in metric_types.itervalues():
        metric_names_to_return |= metric_names
    elif metric in metric_types:
      metric_names_to_return |= set(metric_types[metric])
    else:
      raise ValueError('UnsupportedMetric')

    sorted_metric_names = sorted(list(metric_names_to_return))
    return sorted_metric_names

  def _create_query_string(self):
    built_query_format = ('SELECT\n\t{select_list}\nFROM\n\t{table_list}\n'
                          'WHERE\n\t{conditional_list}')
    non_null_fields = ['connection_spec.data_direction',
                       'web100_log_entry.is_last_entry',
                       'web100_log_entry.snap.HCThruOctetsAcked',
                       'web100_log_entry.snap.CongSignals',
                       'web100_log_entry.connection_spec.remote_ip',
                       'web100_log_entry.connection_spec.local_ip']
    tool_specific_conditions = ['project = 0',
                                'web100_log_entry.is_last_entry = True']

    non_null_conditions = []
    for field in non_null_fields:
      non_null_conditions.append('%s IS NOT NULL' % field)

    select_list_string = ',\n\t'.join(self._select_list)
    table_list_string = ',\n\t'.join(self._table_list)

    conditional_list_string = '\n\tAND '.join(non_null_conditions +
                                              tool_specific_conditions)

    if 'data_direction' in self._conditional_dict:
      conditional_list_string += '\n\tAND {data_direction}'.format(
          data_direction=self._conditional_dict['data_direction'])

    log_times_joined = ' OR\n\t'.join(self._conditional_dict['log_time'])
    conditional_list_string += '\n\tAND ({log_times})'.format(
        log_times=log_times_joined)

    server_ips_joined = ' OR\n\t\t'.join(self._conditional_dict['server_ip'])
    conditional_list_string += '\n\tAND ({server_ips})'.format(
        server_ips=server_ips_joined)

    client_ips_joined = ' OR\n\t\t'.join(
        self._conditional_dict['client_network_block'])
    conditional_list_string += '\n\tAND ({client_ips})'.format(
        client_ips=client_ips_joined)

    built_query_string = built_query_format.format(
        select_list=select_list_string,
        table_list=table_list_string,
        conditional_list=conditional_list_string)

    return built_query_string

  def _add_log_time_conditional(self, start_time_datetime, end_time_datetime):
    if 'log_time' not in self._conditional_dict:
      self._conditional_dict['log_time'] = set()

    utc_absolutely_utc = utils.unix_timestamp_to_utc_datetime(0)
    start_time = int(
        (start_time_datetime - utc_absolutely_utc).total_seconds())
    end_time = int(
        (end_time_datetime - utc_absolutely_utc).total_seconds())

    new_statement = ('(web100_log_entry.log_time >= {start_time})'
                     ' AND (web100_log_entry.log_time < {end_time})').format(
                         start_time=start_time,
                         end_time=end_time)

    self._conditional_dict['log_time'].add(new_statement)

  def _add_data_direction_conditional(self, metric):
    if metric in ['download_throughput', 'minimum_rtt', 'average_rtt',
                  'packet_retransmit_rate']:
      data_direction = 1
    elif metric in ['upload_throughput']:
      data_direction = 0
    self._conditional_dict['data_direction'] = (
        'connection_spec.data_direction == %d' % data_direction)

  def _add_client_network_blocks_conditional(self, client_ip_blocks):
    # remove duplicates, warn if any are found
    unique_client_ip_blocks = list(set(client_ip_blocks))
    if len(client_ip_blocks) != len(unique_client_ip_blocks):
      self.logger.warning('Client IP blocks contained duplicates.')

    # sort the blocks for the sake of consistent query generation
    unique_client_ip_blocks = sorted(unique_client_ip_blocks,
                                     key=lambda block: block[0])

    self._conditional_dict['client_network_block'] = []
    for start_block, end_block in client_ip_blocks:
      new_statement = (
          'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN '
          '{start_block} AND {end_block}').format(
              start_block=start_block,
              end_block=end_block)
      self._conditional_dict['client_network_block'].append(new_statement)

  def _add_server_ips_conditional(self, server_ips):
    # remove duplicates, warn if any are found
    unique_server_ips = list(set(server_ips))
    if len(server_ips) != len(unique_server_ips):
      self.logger.warning('Server IPs contained duplicates.')

    # sort the IPs for the sake of consistent query generation
    unique_server_ips.sort()

    self._conditional_dict['server_ip'] = []
    for server_ip in unique_server_ips:
      new_statement = (
          'web100_log_entry.connection_spec.local_ip = \'{server_ip}\''.format(
              server_ip=server_ip))
      self._conditional_dict['server_ip'].append(new_statement)

