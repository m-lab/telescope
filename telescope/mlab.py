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


import socket

class DNSResolutionError(Exception):
  def __init__(self, hostname):
    Exception.__init__(self, 'Failed to resolve hostname `%s\'' % hostname)

class MLabSiteResolver(object):
  def __init__(self):
    self._cache = {}

  def get_site_ips(self, site_id, mlab_project):
    """ Get a list of a Measurement Lab site and slice's addresses.

        Args:
          site_id (str): M-Lab site identifier, should be an airport code and
            a two-digit number.
          mlab_project (str): Name of the tool.

        Returns:
          list: List of the IP addresses associated with the slices for a tool
            running on the location's M-Lab nodes.

        Notes:
          * Different tools generally have their own IP addresses per node. Where
            they do not, the difference should be handled transparently by this
            function.
    """
    node_addresses_to_return = []

    for node_id in ['mlab1', 'mlab2', 'mlab3']:
      slice_hostname = self._generate_hostname(site_id, node_id, mlab_project)
      ip_address = self._resolve_hostname(slice_hostname)
      node_addresses_to_return.append(ip_address)

    return node_addresses_to_return

  def _generate_hostname(self, site_id, node_id, mlab_project):
    if mlab_project == 'ndt':
      slice_prefix = "ndt.iupui"
    elif mlab_project == 'paris-traceroute':
      slice_prefix = "npad.iupui"
    else:
      raise ValueError('UnknownMLabProject')

    hostname_format = "{slice_prefix}.{node_id}.{site_id}.measurement-lab.org"
    hostname = hostname_format.format(slice_prefix = slice_prefix,
                                      node_id = node_id,
                                      site_id = site_id)
    return hostname

  def _resolve_hostname(self, hostname):
    if hostname in self._cache:
      return self._cache[hostname]

    try:
      ip_address = socket.gethostbyname(hostname)
    except socket.gaierror:
      raise DNSResolutionError(hostname)
    self._cache[hostname] = ip_address
    return ip_address

def get_site_transit(site_id):
  site_transit_map = {
      'atl01': 'level3',
      'atl02': 'cogent',
      'dfw01': 'cogent',
      'iad01': 'xo',
      'iad02': 'cogent',
      'lax01': 'cogent',
      'lga01': 'internap',
      'lga02': 'cogent',
      'mia01': 'level3',
      'mia02': 'cogent',
      'nuq01': 'google',
      'nuq02': 'isc',
      'nuq03': 'cogent',
      'ord01': 'level3',
      'ord02': 'cogent',
      'sea01': 'cogent',
      }
  if site_transit_map.has_key(site_id):
    return site_transit_map[site_id]

  return None

def parse_pt_data(input_data):
  """ Takes in all paris-traceroute data returned from a query and transforms
      measurements into a more easily usable data structure.

    Args:
      input_data (list): List of dicts with Measurement Lab and web100
        variables for per paris-traceroute hop.

    Returns:
      list: List of dictionaries with two keys 'log_time' -- a unixtimestamp
        string -- and 'hops' -- a list of unique hop addresses.

    Note:
      * This function is not fully validated, and we caution against its use.
        Path data may include loops or unresponsive hops, which would skew
        the results. Resulting hop set may also be out of original path order.

  """
  input_data_dict = {}

  for data_row in input_data:
    data_row_key = (data_row['connection_spec_server_ip'], data_row['connection_spec_client_ip'], data_row['test_id'])

    if not input_data_dict.has_key(data_row_key):
      input_data_dict[data_row_key] = {'log_time': data_row['log_time'], 'hops': [] }
      input_data_dict[data_row_key]['hops'] = [data_row['connection_spec_server_ip'], data_row['connection_spec_client_ip']]

    path_position_prior_to_client = len(input_data_dict[data_row_key]['hops']) - 1

    if data_row['paris_traceroute_hop_src_ip'] not in input_data_dict[data_row_key]['hops']:
      input_data_dict[data_row_key]['hops'].insert(path_position_prior_to_client, data_row['paris_traceroute_hop_src_ip'])
      path_position_prior_to_client += 1

    if data_row['paris_traceroute_hop_dest_ip'] not in input_data_dict[data_row_key]['hops']:
      input_data_dict[data_row_key]['hops'].insert(path_position_prior_to_client, data_row['paris_traceroute_hop_dest_ip'])

  return input_data_dict.values()
