#!/usr/bin/env python
# -*- coding: UTF-8 -*-
#
# Copyright 2016 Measurement Lab
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
"""Provides functions to convert NDT results to CSV format."""

import csv
import io


def metrics_to_csv(metrics):
    """Converts a list of result metrics to CSV format.

    Args:
        metrics: (list) A list of dictionaries containing the values
          of NDT metrics, such as:

          [{'timestamp': 1466792556, 'average_rtt': 20.3},
           {'timestamp': 1466792559, 'average_rtt': 25.2},
           {'timestamp': 1466792553, 'average_rtt': 21.9}]

    Returns:
        The specified result list as a headerless, CSV-formatted string, such
        as:

          1466792556,20.3
          1466792559,25.2
          1466792553,21.9

        The rows are not in sorted order, but the timestamp field is always the
        first column.
    """
    if not metrics:
        return ''
    # Ensure that timestamp is the first column of the CSV.
    fieldnames_ordered = metrics[0].keys()
    fieldnames_ordered.remove('timestamp')
    fieldnames_ordered.insert(0, 'timestamp')

    output_buffer = io.BytesIO()
    data_file_csv = csv.DictWriter(output_buffer, fieldnames=fieldnames_ordered)
    data_file_csv.writerows(metrics)
    return output_buffer.getvalue()
