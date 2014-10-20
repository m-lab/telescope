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
                            'download_throughput': filter_download_throughput_measurement,
                            'upload_throughput': filter_upload_throughput_measurement,
                            'minimum_rtt': filter_minimum_rtt_measurement,
                            'average_rtt': filter_average_rtt_measurement,
                            'packet_retransmit_rate': filter_packet_retransmit_rate_measurement,
                            'hop_count': filter_hop_count_measurement
                          }
  assert metric in filter_functions.keys()

  return filter(filter_functions[metric], measurements_list)

def filter_c2s_measurement(measurement):
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

  if False in [required_field in measurement.keys() for required_field in required_fields]:
    raise ValueError('MissingField')

  assert int(measurement['connection_spec_data_direction']) == 0

  minimum_duration = 9000000 # greater than or equal to 9 seconds
  maximum_duration = 3600000000 #  test lasted for an hour or more, probably erroneous
  minimum_bytes = 8192

  if not (int(measurement['web100_log_entry_snap_Duration']) >= minimum_duration):
    return False
  if not (int(measurement['web100_log_entry_snap_Duration']) < maximum_duration):
    return False
  if not (int(measurement['web100_log_entry_snap_HCThruOctetsReceived']) >= minimum_bytes):
    return False
  if not filter_invalid_tcp_state(measurement):
    return False

  return True

def filter_s2c_measurement(measurement):
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

  minimum_duration = 9000000 # greater than or equal to 9 seconds
  maximum_duration = 3600000000 # test lasted for an hour or more, probably erronuous
  minimum_bytes = 8192

  if not ((int(measurement['web100_log_entry_snap_SndLimTimeRwin']) +
                              int(measurement['web100_log_entry_snap_SndLimTimeCwnd']) +
                              int(measurement['web100_log_entry_snap_SndLimTimeSnd'])) >= minimum_duration):
    return False
  if not ((int(measurement['web100_log_entry_snap_SndLimTimeRwin']) +
                            int(measurement['web100_log_entry_snap_SndLimTimeCwnd']) +
                            int(measurement['web100_log_entry_snap_SndLimTimeSnd'])) < maximum_duration):
    return False
  if not (int(measurement['web100_log_entry_snap_HCThruOctetsAcked']) >= minimum_bytes):
    return False
  if not (int(measurement['web100_log_entry_snap_CongSignals']) > 0):
    return False
  if not filter_invalid_tcp_state(measurement):
    return False

  return True

def filter_invalid_tcp_state(measurement):
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

def filter_download_throughput_measurement(measurement):
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

  if not filter_s2c_measurement(measurement):
    return False

  return True
def filter_upload_throughput_measurement(measurement):
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
  if not filter_c2s_measurement(measurement):
    return False

  return True
def filter_minimum_rtt_measurement(measurement):
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
  if not filter_s2c_measurement(measurement):
    return False
  if not (int(measurement['web100_log_entry_snap_MinRTT']) != 0):
    return False
  if not (int(measurement['web100_log_entry_snap_CountRTT']) > 0):
    return False

  return True

def filter_average_rtt_measurement(measurement):
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
  if not filter_s2c_measurement(measurement):
    return False
  if not (int(measurement['web100_log_entry_snap_SumRTT']) != 0):
    return False
  if not (int(measurement['web100_log_entry_snap_CountRTT']) > 0):
    return False

  return True

def filter_packet_retransmit_rate_measurement(measurement):
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
  if not filter_s2c_measurement(measurement):
    return False
  if not (int(measurement['web100_log_entry_snap_SegsRetrans']) != 0):
    return False
  if not (int(measurement['web100_log_entry_snap_DataSegsOut']) > 0):
    return False

  return True

def filter_hop_count_measurement(measurement):
  """ Hop count measurement validation has yet to be defined.

  """
  raise NotImplementedError("Hop count validation is not yet defined.")

