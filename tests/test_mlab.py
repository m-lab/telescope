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

import os
import socket
import sys
import unittest

import mox

sys.path.insert(1, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../telescope')))
import mlab


class MLabTest(unittest.TestCase):

    def setUp(self):
        self.mock = mox.Mox()
        self.mock.StubOutWithMock(socket, 'gethostbyname')

    def tearDown(self):
        self.mock.UnsetStubs()

    def create_mock_dns_lookup_result(self, hostname, dns_result):
        socket.gethostbyname(hostname).AndReturn(dns_result)

    def create_mock_dns_lookup_failure(self, hostname):
        socket.gethostbyname(hostname).AndRaise(socket.gaierror)

    def test_get_site_ndt_ips_valid_ndt_slices(self):
        self.create_mock_dns_lookup_result(
            'ndt.iupui.mlab1.nuq01.measurement-lab.org', '1.1.1.1')
        self.create_mock_dns_lookup_result(
            'ndt.iupui.mlab2.nuq01.measurement-lab.org', '1.1.1.2')
        self.create_mock_dns_lookup_result(
            'ndt.iupui.mlab3.nuq01.measurement-lab.org', '1.1.1.3')
        self.mock.ReplayAll()
        expected_ips = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        resolver = mlab.MLabSiteResolver()
        self.assertListEqual(expected_ips, resolver.get_site_ndt_ips('nuq01'))

        self.mock.VerifyAll()

    def test_get_site_ndt_ips_dns_failure(self):
        self.create_mock_dns_lookup_result(
            'ndt.iupui.mlab1.nuq01.measurement-lab.org', '1.1.1.1')
        self.create_mock_dns_lookup_failure(
            'ndt.iupui.mlab2.nuq01.measurement-lab.org')
        self.create_mock_dns_lookup_result(
            'ndt.iupui.mlab3.nuq01.measurement-lab.org', '1.1.1.3')
        self.mock.ReplayAll()
        resolver = mlab.MLabSiteResolver()
        self.assertRaises(mlab.DNSResolutionError, resolver.get_site_ndt_ips,
                          'nuq01')


if __name__ == '__main__':
    unittest.main()
