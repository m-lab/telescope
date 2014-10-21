import filters
import unittest

class FiltersTest(unittest.TestCase):
  def test_filter_measurements_list(self):
    fake_good_data = {
      'web100_log_entry_snap_Duration': 10000000,
      'web100_log_entry_snap_HCThruOctetsReceived': 10000,
      'connection_spec_data_direction': 0,
      'web100_log_entry_snap_State': 1
    }
    fake_toolong_data = {
      'web100_log_entry_snap_Duration': 1000000000000,
      'web100_log_entry_snap_HCThruOctetsReceived': 10000,
      'connection_spec_data_direction': 0,
      'web100_log_entry_snap_State': 1

    }
    fake_tooshort_data = {
      'web100_log_entry_snap_Duration': 10000,
      'web100_log_entry_snap_HCThruOctetsReceived': 10000,
      'connection_spec_data_direction': 0,
      'web100_log_entry_snap_State': 1

    }
    fake_toolittle_data = {
      'web100_log_entry_snap_Duration': 10000000,
      'web100_log_entry_snap_HCThruOctetsReceived': 1000,
      'connection_spec_data_direction': 0,
      'web100_log_entry_snap_State': 1

    }
    self.assertTrue(filters._filter_c2s_measurement(fake_good_data))
    self.assertFalse(filters._filter_c2s_measurement(fake_toolong_data))
    self.assertFalse(filters._filter_c2s_measurement(fake_tooshort_data))
    self.assertFalse(filters._filter_c2s_measurement(fake_toolittle_data))
    good_data_tuple = tuple([fake_good_data])
    filtered_data_tuple = tuple(filters.filter_measurements_list(
        'upload_throughput', [fake_good_data]))
    self.assertEqual(good_data_tuple, filtered_data_tuple)
    
