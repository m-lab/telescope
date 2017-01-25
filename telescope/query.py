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

import logging

import utils


def _seconds_to_microseconds(seconds):
    return seconds * 1000000


def _is_server_to_client_metric(metric):
    return metric in ('download_throughput', 'minimum_rtt', 'average_rtt',
                      'packet_retransmit_rate')


def _create_test_validity_conditional(metric):
    """Creates BigQuery SQL clauses to specify validity rules for an NDT test.

    Args:
        metric: (string) The metric for which to add the conditional.

    Returns:
        (string) A set of SQL clauses that specify conditions an NDT test must
        meet to be considered a valid, completed test.
    """
    # NDT test is supposed to last 10 seconds, give some buffer for tests that
    # ended slighly before 10 seconds.
    MIN_DURATION = _seconds_to_microseconds(9)

    # Tests that last > 1 hour are likely erroneous.
    MAX_DURATION = _seconds_to_microseconds(3600)

    # A test that did not exchange at least 8,192 bytes is likely erroneous.
    MIN_BYTES = 8192

    # web100 state variable constants from
    # http://www.web100.org/download/kernel/tcp-kis.txt
    STATE_CLOSED = 1
    STATE_ESTABLISHED = 5
    STATE_TIME_WAIT = 11

    # For RTT metrics, exclude results of tests with 10 or fewer round trip time
    # samples, because there are not enough samples to accurately estimate the
    # RTT.
    MIN_RTT_SAMPLES = 10

    conditions = []
    # Must have completed the TCP three-way handshake.
    conditions.append((
        '(web100_log_entry.snap.State = {state_closed}\n\t'
        '\tOR (web100_log_entry.snap.State >= {state_established}\n\t'
        '\t\tAND web100_log_entry.snap.State <= {state_time_wait}))').format(
            state_closed=STATE_CLOSED,
            state_established=STATE_ESTABLISHED,
            state_time_wait=STATE_TIME_WAIT))

    # Must have been determined to be unaffected by platform error.
    conditions.append((
        'blacklist_flags == 0'))

    if _is_server_to_client_metric(metric):
        # Must leave slow start phase of TCP, indicated by reaching
        # congestion at least once.
        conditions.append('web100_log_entry.snap.CongSignals > 0')
        # Must send at least the minimum number of bytes.
        conditions.append('web100_log_entry.snap.HCThruOctetsAcked >= %d' %
                          MIN_BYTES)
        # Must last for at least the minimum test duration.
        conditions.append(
            ('(web100_log_entry.snap.SndLimTimeRwin +\n\t'
             '\tweb100_log_entry.snap.SndLimTimeCwnd +\n\t'
             '\tweb100_log_entry.snap.SndLimTimeSnd) >= %u') % MIN_DURATION)
        # Must not exceed the maximum test duration.
        conditions.append(
            ('(web100_log_entry.snap.SndLimTimeRwin +\n\t'
             '\tweb100_log_entry.snap.SndLimTimeCwnd +\n\t'
             '\tweb100_log_entry.snap.SndLimTimeSnd) < %u') % MAX_DURATION)

        # Exclude results of tests with fewer than 10 round trip time samples,
        # because there are not enough samples to accurately estimate the RTT.
        if metric == 'minimum_rtt' or metric == 'average_rtt':
            conditions.append('web100_log_entry.snap.CountRTT > %u' %
                              MIN_RTT_SAMPLES)
    else:
        # Must receive at least the minimum number of bytes.
        conditions.append('web100_log_entry.snap.HCThruOctetsReceived >= %u' %
                          MIN_BYTES)
        # Must last for at least the minimum test duration.
        conditions.append('web100_log_entry.snap.Duration >= %u' % MIN_DURATION)
        # Must not exceed the maximum test duration.
        conditions.append('web100_log_entry.snap.Duration < %u' % MAX_DURATION)
    return '\n\tAND '.join(conditions)


def _create_select_clauses(metric):
    clauses = ['web100_log_entry.log_time AS timestamp']
    metric_to_clause = {
        'download_throughput':
        ('8 * (web100_log_entry.snap.HCThruOctetsAcked /\n\t\t'
         '(web100_log_entry.snap.SndLimTimeRwin +\n\t\t'
         ' web100_log_entry.snap.SndLimTimeCwnd +\n\t\t'
         ' web100_log_entry.snap.SndLimTimeSnd)) AS download_mbps'),
        'upload_throughput':
        ('8 * (web100_log_entry.snap.HCThruOctetsReceived /\n\t\t'
         ' web100_log_entry.snap.Duration) AS upload_mbps'),
        'minimum_rtt': ('web100_log_entry.snap.MinRTT AS minimum_rtt'),
        'average_rtt':
        ('(web100_log_entry.snap.SumRTT / web100_log_entry.snap.CountRTT) '
         'AS average_rtt'),
        'packet_retransmit_rate':
        ('(web100_log_entry.snap.SegsRetrans /\n\t\t'
         ' web100_log_entry.snap.DataSegsOut) AS packet_retransmit_rate'),
    }
    clauses.append(metric_to_clause[metric])

    return ',\n\t'.join(clauses)


class BigQueryQueryGenerator(object):

    def __init__(self,
                 start_time,
                 end_time,
                 metric,
                 server_ips=None,
                 client_ip_blocks=None,
                 client_country=None):
        self.logger = logging.getLogger('telescope')
        self._metric = metric
        self._conditional_dict = {}
        self._add_data_direction_conditional(metric)
        self._add_log_time_conditional(start_time, end_time)

        if client_ip_blocks:
            self._add_client_ip_blocks_conditional(client_ip_blocks)
        if client_country:
            self._add_client_country_conditional(client_country)
        if server_ips:
            self._add_server_ips_conditional(server_ips)
        self._query = self._create_query_string()

    def query(self):
        return self._query

    def _create_query_string(self):
        built_query_format = ('SELECT\n\t{select_clauses}\n'
                              'FROM\n\t{table}\n'
                              'WHERE\n\t{conditional_list}')
        conditional_list_string = ''

        if 'data_direction' in self._conditional_dict:
            conditional_list_string += self._conditional_dict['data_direction']

        conditional_list_string += '\n\t AND %s' % (
            _create_test_validity_conditional(self._metric))

        log_times_joined = ' OR\n\t'.join(self._conditional_dict['log_time'])
        conditional_list_string += '\n\tAND (%s)' % log_times_joined

        if 'server_ips' in self._conditional_dict:
            server_ips_joined = ' OR\n\t\t'.join(self._conditional_dict[
                'server_ips'])
            conditional_list_string += '\n\tAND (%s)' % server_ips_joined

        if 'client_ip_blocks' in self._conditional_dict:
            client_ip_blocks_joined = ' OR\n\t\t'.join(self._conditional_dict[
                'client_ip_blocks'])
            conditional_list_string += '\n\tAND (%s)' % client_ip_blocks_joined
        if 'client_country' in self._conditional_dict:
            conditional_list_string += '\n\tAND %s' % (
                self._conditional_dict['client_country'])

        built_query_string = built_query_format.format(
            select_clauses=_create_select_clauses(self._metric),
            table='plx.google:m_lab.ndt.all',
            conditional_list=conditional_list_string)

        return built_query_string

    def _add_log_time_conditional(self, start_time_datetime, end_time_datetime):
        if 'log_time' not in self._conditional_dict:
            self._conditional_dict['log_time'] = set()

        utc_absolutely_utc = utils.unix_timestamp_to_utc_datetime(0)
        start_time = int((start_time_datetime - utc_absolutely_utc
                         ).total_seconds())
        end_time = int((end_time_datetime - utc_absolutely_utc).total_seconds())

        new_statement = (
            '(web100_log_entry.log_time >= {start_time})'
            ' AND (web100_log_entry.log_time < {end_time})').format(
                start_time=start_time, end_time=end_time)

        self._conditional_dict['log_time'].add(new_statement)

    def _add_data_direction_conditional(self, metric):
        conditional = ''
        if _is_server_to_client_metric(metric):
            data_direction = 1
        else:
            data_direction = 0
            conditional += '\n\tAND connection_spec.data_direction IS NOT NULL'
        self._conditional_dict['data_direction'] = (
            'connection_spec.data_direction = %d' % data_direction + conditional
        )

    def _add_client_ip_blocks_conditional(self, client_ip_blocks):
        # remove duplicates, warn if any are found
        unique_client_ip_blocks = list(set(client_ip_blocks))
        if len(client_ip_blocks) != len(unique_client_ip_blocks):
            self.logger.warning('Client IP blocks contained duplicates.')

        # sort the blocks for the sake of consistent query generation
        unique_client_ip_blocks = sorted(unique_client_ip_blocks,
                                         key=lambda block: block[0])

        self._conditional_dict['client_ip_blocks'] = []
        for start_block, end_block in client_ip_blocks:
            new_statement = (
                'PARSE_IP(web100_log_entry.connection_spec.remote_ip) BETWEEN '
                '{start_block} AND {end_block}').format(start_block=start_block,
                                                        end_block=end_block)
            self._conditional_dict['client_ip_blocks'].append(new_statement)

    def _add_server_ips_conditional(self, server_ips):
        # remove duplicates, warn if any are found
        unique_server_ips = list(set(server_ips))
        if len(server_ips) != len(unique_server_ips):
            self.logger.warning('Server IPs contained duplicates.')

        # sort the IPs for the sake of consistent query generation
        unique_server_ips.sort()

        self._conditional_dict['server_ips'] = []
        for server_ip in unique_server_ips:
            new_statement = 'web100_log_entry.connection_spec.local_ip = \'%s\'' % server_ip
            self._conditional_dict['server_ips'].append(new_statement)

    def _add_client_country_conditional(self, client_country):
        self._conditional_dict['client_country'] = (
            'connection_spec.client_geolocation.country_code = \'%s\'' %
            (client_country.upper()))
