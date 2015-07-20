import datetime
import io
import iptranslation
import mock
import unittest

from mock import patch

class IPTranslationStrategyMaxMindTest(unittest.TestCase):

  def createIPTranslationStrategy(self, mock_file_contents):
    snapshot_datetime = datetime.datetime(2014, 9, 1)
    mock_file = io.BytesIO(mock_file_contents)
    snapshots = [
        (snapshot_datetime, mock_file),
        ]
    return iptranslation.IPTranslationStrategyMaxMind(snapshots)

  def assertBlocksMatchForSearch(self, mock_file_contents, asn_search_name, expected_blocks):
    translation_strategy = self.createIPTranslationStrategy(mock_file_contents)
    actual_blocks = translation_strategy.find_ip_blocks(asn_search_name)
    self.assertListEqual(expected_blocks, actual_blocks)

  def testVanillaFile(self):
    mock_file_contents = """5,10,"FooISP"
20,25,"BarIsp"
"""
    expected_blocks = [(20, 25)]
    self.assertBlocksMatchForSearch(mock_file_contents, 'bar', expected_blocks)

  # Verify that repeated searches of the same ISP name return the same results
  def testRepeatedSearches(self):
    mock_file_contents = """5,10,"FooISP"
20,25,"BarIsp"
"""
    translation_strategy = self.createIPTranslationStrategy(mock_file_contents)
    actual_blocks1 = translation_strategy.find_ip_blocks('bar')
    actual_blocks2 = translation_strategy.find_ip_blocks('bar')
    self.assertListEqual([(20, 25)], actual_blocks1)
    self.assertListEqual([(20, 25)], actual_blocks2)

  def testLevel3Expansion(self):
    mock_file_contents = """1,15,"Level 3 Communications"
16,20,"Rando Internet Company"
21,25,"GBLX"
"""
    expected_blocks = [
        (1, 15),
        (21, 25)
        ]
    self.assertBlocksMatchForSearch(mock_file_contents, 'level3', expected_blocks)

  def testTimeWarnerExpansion(self):
    mock_file_contents = """57,63,"Time Warner"
92,108,"Time Warner"
109,110,"ConglomCo Internet"
"""
    expected_blocks = [
        (57, 63),
        (92, 108)
        ]
    self.assertBlocksMatchForSearch(mock_file_contents, 'twc', expected_blocks)

  def testCenturyLinkExpansion(self):
    mock_file_contents = """5,9,"Embarq"
34,38,"Red Herring Internet"
45,51,"CenturyLink"
55,58,"CenturyTel"
60,65,"Generic InternetCo"
67,68,"Qwest Internet"
"""
    expected_blocks = [
        (5, 9),
        (45, 51),
        (55, 58),
        (67, 68)
        ]
    self.assertBlocksMatchForSearch(mock_file_contents, 'centurylink', expected_blocks)

if __name__ == '__main__':
  unittest.main()
