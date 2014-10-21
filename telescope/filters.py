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


_MINIMUM_DURATION = 9000000 # greater than or equal to 9 seconds
_MAXIMUM_DURATION = 3600000000 #  test lasted for an hour or more, probably erroneous
_MINIMUM_PACKETS = 8192 # 6 packets == 8192 bytes.


def filter_measurements_list(metric, measurements_list):
  """Applies measurement validition functions across a list of measurements.

    Args:
      metric (str): name of M-Lab metric to apply validation rules on for
        provided measurements.
      measurements_list (list): List of dicts with Measurement Lab and web100
        variables for per measurement.

    Returns:
      dict: Dictionary with two lists, categorize with keep and discard lists
        based on validation but always actually retain for statistical
        purposes.

  """
  filter_functions = {
                            'download_throughput': _filter_download_throughput_measurement,
                            'upload_throughput': _filter_upload_throughput_measurement,
                            'minimum_rtt': _filter_minimum_rtt_measurement,
                            'average_rtt': _filter_average_rtt_measurement,
                            'packet_retransmit_rate': _filter_packet_retransmit_rate_measurement,
                            'hop_count': _filter_hop_count_measurement
                          }
  assert metric in filter_functions.keys()
  return filter(filter_functions[metric], measurements_list)

def _filter_c2s_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for upload or client-to-server test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for test.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * web100_log_entry.snap.Duration
      * web100_log_entry.snap.HCThruOctetsReceived
      * connection_spec.data_direction
    Validity Rules:
      Must last for more than, or equal to, 9 seconds and be less than, or
        equal to one hour.
          * Duration >= 9000000
          * Duration < 3600000000
      Must exchange at least 8192 bytes.
          * HCThruOctetsReceived >= 8192
    Notes:
      C2S tests do not maintain CongSignals variables

  """

  required_fields = ['web100_log_entry_snap_Duration', 'web100_log_entry_snap_HCThruOctetsReceived',
                     'connection_spec_data_direction']

  for required_field in required_fields:
    if required_field not in measurement.keys():
      raise ValueError('MissingField: ' + required_field)

  assert int(measurement['connection_spec_data_direction']) == 0

  return (_MINIMUM_DURATION 
          <= int(measurement['web100_log_entry_snap_Duration']) 
          < _MAXIMUM_DURATION) \
      and (int(measurement['web100_log_entry_snap_HCThruOctetsReceived']) 
          >= _MINIMUM_PACKETS) \
      and _filter_invalid_tcp_state(measurement)

def _filter_s2c_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for download or server-to-client test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for test.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * web100_log_entry.snap.SndLimTimeRwin
      * web100_log_entry.snap.SndLimTimeCwnd
      * web100_log_entry.snap.SndLimTimeSnd
      * web100_log_entry.snap.CongSignals
      * web100_log_entry.snap.HCThruOctetsAcked
      * connection_spec.data_direction
    Validity Rules:
      Must last for more than, or equal to, 9 seconds and be less than, or
        equal to one hour.
          * (SndLimTimeRwin + SndLimTimeCwnd + SndLimTimeSnd) >= 9000000
          * (SndLimTimeRwin + SndLimTimeCwnd + SndLimTimeSnd) < 3600000000
      Must exchange at least 8192 bytes.
          * HCThruOctetsAcked >= 8192
      Must leave slow start phase of TCP, through reaching Congestion at
        least once.
          * CongSignals > 0

  """

  required_fields = ['web100_log_entry_snap_SndLimTimeRwin', 'web100_log_entry_snap_SndLimTimeCwnd',
                      'web100_log_entry_snap_SndLimTimeSnd', 'web100_log_entry_snap_CongSignals',
                      'web100_log_entry_snap_HCThruOctetsAcked', 'connection_spec_data_direction']

  if False in [required_field in measurement.keys() for required_field in required_fields]:
    raise ValueError('MissingField')

  assert int(measurement['connection_spec_data_direction']) == 1

  if not ((int(measurement['web100_log_entry_snap_SndLimTimeRwin']) +
                              int(measurement['web100_log_entry_snap_SndLimTimeCwnd']) +
                              int(measurement['web100_log_entry_snap_SndLimTimeSnd'])) >= _MINIMUM_DURATION):
    return False
  if not ((int(measurement['web100_log_entry_snap_SndLimTimeRwin']) +
                            int(measurement['web100_log_entry_snap_SndLimTimeCwnd']) +
                            int(measurement['web100_log_entry_snap_SndLimTimeSnd'])) < _MAXIMUM_DURATION):
    return False
  if not (int(measurement['web100_log_entry_snap_HCThruOctetsAcked']) >= _MINIMUM_PACKETS):
    return False
  if not (int(measurement['web100_log_entry_snap_CongSignals']) > 0):
    return False
  if not _filter_invalid_tcp_state(measurement):
    return False

  return True

def _filter_invalid_tcp_state(measurement):
  """Applies measurement validity rules to ensure that the test concluded the
     TCP 3-way handshake and established a connnection.

    Args:
      measurement (dict): Measurement Lab and web100 variables for test.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * web100_log_entry_snap_State
    Validity Rules:
      Connection must be established (and possibly closed):
          * State == 1 || (State >= 5 && State <= 11)
  """
  # State variables from http://www.web100.org/download/kernel/tcp-kis.txt
  STATE_CLOSED = 1
  STATE_ESTABLISHED = 5
  STATE_TIME_WAIT = 11

  if 'web100_log_entry_snap_State' not in measurement:
    raise ValueError('MissingField')

  state = int(measurement['web100_log_entry_snap_State'])

  return (state == STATE_CLOSED) or ((state >= STATE_ESTABLISHED) and (state <= STATE_TIME_WAIT))

def _filter_download_throughput_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for download throughput test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for one
        measurement.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * All normal NDT server-to-client fields
    Validity Rules:
      * Same as normal NDT server-to-client validity

  """
  return _filter_s2c_measurement(measurement)

def _filter_upload_throughput_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for upload throughput test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for one
        measurement.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * All normal NDT client-to-server fields
    Validity Rules:
      * Same as normal NDT client-to-server validity

  """
  return _filter_c2s_measurement(measurement)

def _filter_minimum_rtt_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for server-to-client Minimum Round Trip Time test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for one
        measurement.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * All normal NDT server-to-client fields
      * web100_log_entry.snap.MinRTT
      * web100_log_entry.snap.CountRTT
    Validity Rules:
      * All normal NDT server-to-client validity rules.
      * web100_log_entry.snap.MinRTT is defined (int != 0)
      * web100_log_entry.snap.CountRTT > 0

  """
  return (_filter_s2c_measurement(measurement) and
                (int(measurement['web100_log_entry_snap_MinRTT']) != 0) and
                (int(measurement['web100_log_entry_snap_CountRTT']) > 0))

def _filter_average_rtt_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for server-to-client Average Round Trip Time test.

    Args:
      measurement (dict): Measurement Lab and web100 variables for one
        measurement.

    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * All normal NDT server-to-client fields
      * web100_log_entry.snap.SumRTT
      * web100_log_entry.snap.CountRTT
    Validity Rules:
      * All normal NDT server-to-client validity rules.
      * web100_log_entry.snap.SumRTT is defined (int != 0)
      * web100_log_entry.snap.CountRTT > 0

  """
  return (_filter_s2c_measurement(measurement) and
                (int(measurement['web100_log_entry_snap_SumRTT']) != 0) and
                (int(measurement['web100_log_entry_snap_CountRTT']) > 0))

def _filter_packet_retransmit_rate_measurement(measurement):
  """Applies measurement validity rules and tests presence of required fields
      for server-to-client Packet Retransmit Rate metric.

    Args:
      measurement (dict): Measurement Lab and web100 variables for one
        measurement.
    Returns:
      bool: True if valid measurement, False otherwise.

    Required Measurements Fields:
      * All normal NDT server-to-client fields
      * web100_log_entry.snap.SegsRetrans
      * web100_log_entry.snap.DataSegsOut
    Validity Rules:
      * All normal NDT server-to-client validity rules.
      * web100_log_entry.snap.SegsRetrans is defined (int != 0)
      * web100_log_entry.snap.DataSegsOut > 0
    Reference: https://code.google.com/p/m-lab/wiki/PDEChartsNDT#Packet_retransmission

  """
  return (_filter_s2c_measurement(measurement) and
                (int(measurement['web100_log_entry_snap_SegsRetrans']) != 0) and
                (int(measurement['web100_log_entry_snap_DataSegsOut']) > 0))

def _filter_hop_count_measurement(measurement):
  """ Hop count measurement validation has yet to be defined.

  """
  raise NotImplementedError("Hop count validation is not yet defined.")
