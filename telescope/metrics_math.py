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


def calculate_results_list(metric, input_datarows):
    datarows_to_return = []

    for datarow in input_datarows:
        calculated_result = None

        if metric == 'minimum_rtt':
            timestamp = datarow['web100_log_entry_log_time']
            calculated_result = (
                calculate_minrtt(datarow['web100_log_entry_snap_MinRTT']))
        elif metric == 'average_rtt':
            timestamp = datarow['web100_log_entry_log_time']
            calculated_result = calculate_avgrtt(
                datarow['web100_log_entry_snap_SumRTT'],
                datarow['web100_log_entry_snap_CountRTT'])
        elif metric == 'download_throughput':
            assert datarow['connection_spec_data_direction'] == '1'

            timestamp = datarow['web100_log_entry_log_time']
            data_transfered = float(
                datarow['web100_log_entry_snap_HCThruOctetsAcked'])
            time_spent = (int(datarow['web100_log_entry_snap_SndLimTimeRwin']) +
                          int(datarow['web100_log_entry_snap_SndLimTimeCwnd']) +
                          int(datarow['web100_log_entry_snap_SndLimTimeSnd']))
            calculated_result = calculate_throughput(data_transfered,
                                                     time_spent)

        elif metric == 'upload_throughput':
            assert datarow['connection_spec_data_direction'] == '0'

            timestamp = datarow['web100_log_entry_log_time']
            data_transfered = float(
                datarow['web100_log_entry_snap_HCThruOctetsReceived'])
            time_spent = int(datarow['web100_log_entry_snap_Duration'])
            calculated_result = calculate_throughput(data_transfered,
                                                     time_spent)

        elif metric == 'packet_retransmit_rate':
            assert datarow['connection_spec_data_direction'] == '1'

            timestamp = datarow['web100_log_entry_log_time']
            segments_retransmitted = float(
                datarow['web100_log_entry_snap_SegsRetrans'])
            total_segments_sent = float(
                datarow['web100_log_entry_snap_DataSegsOut'])
            calculated_result = calculate_packet_retransmit_rate(
                segments_retransmitted, total_segments_sent)
        else:
            raise Exception('UnsupportedMetric')

        if calculated_result is not None:
            datarows_to_return.append({
                'timestamp': int(timestamp),
                'result': calculated_result
            })

    return datarows_to_return


def calculate_throughput(data_transfered, time_spent):
    return (float(data_transfered) / float(time_spent)) * 8


def calculate_minrtt(minrtt):
    return float(minrtt)


def calculate_avgrtt(sumrtt, countrtt):
    return float(sumrtt) / float(countrtt)


def calculate_packet_retransmit_rate(segments_retransmitted,
                                     total_segments_sent):
    return float(segments_retransmitted) / float(total_segments_sent)
