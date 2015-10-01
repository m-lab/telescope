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

import csv
import datetime
import logging
import os
import re


class MissingMaxMindError(Exception):

    def __init__(self, db_location, io_error):
        Exception.__init__(
            self, 'Failed to open MaxMind database at %s\nError: %s' % (
                db_location, io_error))


class IPTranslationStrategySpec(object):
    """Specification of how to create an IPTranslationStrategy object.

    Specifies the parameters required for IPTranslationFactory to create an
    IPTranslationStrategy object.

    Attributes:
        strategy_name: (str) The name of this IP translation strategy.
        params: (dict) A dictionary of parameters specific to this type of IP
            translation strategy.
    """

    def __init__(self, strategy_name, params):
        self.strategy_name = strategy_name
        self.params = params


class IPTranslationStrategyFactory(object):

    def __init__(self, file_opener=open):
        self._file_opener = file_opener
        self._cache = {}

    def create(self, ip_translation_spec):
        if ip_translation_spec in self._cache:
            return self._cache[ip_translation_spec]
        if ip_translation_spec.strategy_name == 'maxmind':
            ip_translator = self._create_maxmind_strategy(
                ip_translation_spec.params)
        else:
            ValueError('UnrecognizedIPTranslationStrategy')
        self._cache[ip_translation_spec] = ip_translator
        return ip_translator

    def _create_maxmind_strategy(self, maxmind_params):
        db_snapshot_strings = maxmind_params['db_snapshots']
        if len(db_snapshot_strings) == 0:
            raise ValueError('IPTranslationStrategyNoDatesSpecified')

        maxmind_dir = maxmind_params['maxmind_dir']
        snapshots = []
        for db_snapshot_string in db_snapshot_strings:
            snapshot_datetime = datetime.datetime.strptime(db_snapshot_string,
                                                           '%Y-%m-%d')
            snapshot_path = IPTranslationStrategyMaxMind.get_maxmind_snapshot_path(
                snapshot_datetime, maxmind_dir)
            try:
                snapshot_file = self._file_opener(snapshot_path, 'rb')
                snapshot = (snapshot_datetime, snapshot_file)
                snapshots.append(snapshot)
            except IOError as io_error:
                raise MissingMaxMindError(snapshot_path, io_error)

        return IPTranslationStrategyMaxMind(snapshots)


class IPTranslationStrategy(object):

    def find_ip_blocks(self, asn_search_name):
        raise NotImplementedError()


class IPTranslationStrategyMaxMind(IPTranslationStrategy):

    def __init__(self, snapshots):
        """Creates a new MaxMind IP translator.

        Args:
           snapshots (list): A list of 2-tuples where the first element is a
               datetime and the second element is a file handle to the snapshot
               at that date.
        """
        self.logger = logging.getLogger('telescope')
        if len(snapshots) > 1:
            raise NotImplementedError(
                'Multiple MaxMind snapshot processing not yet implemented.')
        snapshot_file = snapshots[0][1]
        self._network_map = self._parse_maxmind_snapshot(snapshot_file)
        self._cache = {}

    def find_ip_blocks(self, asn_search_name):
        """Search memory-cached copy of map of network maps.

        Currently, searches each (block_start_address, block_end_address,
        asn_name) list based on a case-insensitive match of the AS name.

        Args:
            asn_search_name (str): string to search AS names in order to
                identify network blocks.

        Returns:
            list: Matching tuples of (block_start_address, block_end_address,
            asn_name), empty if no network found.

        Notes:
            * Maintains and consults an internal cache of results since lookup
              process is relatively slow and results should not change.
        """
        if asn_search_name in self._cache:
            return self._cache[asn_search_name]

        notification_cache = set()
        blocks_to_return = []

        asn_search_terms = self._translate_short_name(asn_search_name)
        asn_name_re = re.compile(asn_search_terms, re.IGNORECASE)

        for block_dict in self._network_map:
            if asn_name_re.search(block_dict['asn_name']) is not None:
                if not (block_dict['asn_name'] in notification_cache):
                    self.logger.debug((
                        'Found IP block associated with name {asn_name} searching for term '
                        '{asn_search_name}.').format(
                            asn_name=block_dict['asn_name'],
                            asn_search_name=asn_search_name))
                    notification_cache.add(block_dict['asn_name'])
                block_start = int(block_dict['block_start'])
                block_end = int(block_dict['block_end'])
                blocks_to_return.append((block_start, block_end))
        self._cache[asn_search_name] = blocks_to_return
        return blocks_to_return

    @staticmethod
    def get_maxmind_snapshot_path(snapshot_datetime, maxmind_dir):
        """Generates the expected path of the MaxMind snapshot file based on the
        date of the snapshot and the snapshot directory.

        Args:
            snapshot_datetime (str): Datetime of when snapshot was created.

        Returns:
            string: Pathname of MaxMind database file.
        """
        date_string = snapshot_datetime.strftime('%Y%m%d')
        snapshot_filename = 'GeoIPASNum2-%s.csv' % date_string
        return os.path.join(os.path.dirname(__file__), maxmind_dir,
                            snapshot_filename)

    def _parse_maxmind_snapshot(self, snapshot_file):
        """Parses a MaxMind snapshot file into a list of blocks with associated
        ASN names.

        Args:
            snapshot_filename (str): Filename of MaxMind snapshot to parse.

        Returns:
            list: A list of dicts, each containing a 'block_start', 'block_end',
            and 'asn_name' entry, according to entries in the MaxMind
            snapshot.
        """
        block_rows = []
        csvReader = csv.DictReader(
            snapshot_file,
            fieldnames=['block_start', 'block_end', 'asn_name'])
        for block_row in csvReader:
            block_rows.append(block_row)
        self.logger.debug('Parsed %d blocks from MaxMind snapshot',
                          len(block_rows))
        return block_rows

    def _translate_short_name(self, short_name):
        """Translates an ISP shortname into a regex that matches all company names
        that are part of the ISP.

        Args:
            short_name (str): A short name for an ISP, such as 'twc' for Time
                Warner Cable.

        Returns:
            str: A regex string that matches company names that are part of the
            specified ISP. For example, level3 translates to:
                '(Level 3 Communications)|(GBLX)'
        """
        short_name_map = {
            'twc': ['Time Warner'],
            'centurylink': ['Qwest', 'Embarq', 'Centurylink', 'Centurytel'],
            'level3': ['Level 3 Communications', 'GBLX'],
            'cablevision': ['Cablevision Systems', 'CSC Holdings',
                            'Cablevision Infrastructure',
                            'Cablevision Corporate', 'Optimum Online',
                            'Optimum WiFi', 'Optimum Network']
        }
        if short_name in short_name_map:
            long_names = short_name_map[short_name]
            return self._regex_xor_names(long_names)

        return re.escape(short_name)

    def _regex_xor_names(self, names):
        """Converts a list of names into a regex that matches any of the names.

        Args:
            names (list): A list of ISP names.

        Returns:
            str: A regex that matches any name in the list. For example, the
            list ['foo', 'bar', 'baz'] would result in '(foo)|(bar)|(baz)'.
        """
        escaped_names = []
        for name in names:
            escaped_names.append(re.escape(name))
        return '(' + ')|('.join(escaped_names) + ')'
