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

    def get_site_ndt_ips(self, site_id):
        """Get a list of a Measurement Lab site and slice's addresses.

        Args:
            site_id (str): M-Lab site identifier, should be an airport code and
                a two-digit number.

        Returns:
            list: List of the IP addresses associated with the slices for a tool
                running on the location's M-Lab nodes.

        Notes:
            * Different tools generally have their own IP addresses per node.
              Where they do not, the difference should be handled transparently
              by this function.
        """
        node_addresses_to_return = []

        for node_id in ['mlab1', 'mlab2', 'mlab3']:
            slice_hostname = self._generate_hostname(site_id, node_id)
            ip_address = self._resolve_hostname(slice_hostname)
            node_addresses_to_return.append(ip_address)

        return node_addresses_to_return

    def _generate_hostname(self, site_id, node_id):
        hostname = 'ndt.iupui.{node_id}.{site_id}.measurement-lab.org'.format(
            node_id=node_id,
            site_id=site_id)
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
