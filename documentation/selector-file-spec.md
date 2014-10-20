# Overview

The data subset selector file specifies subsets of M-Lab data, either for single dataset analysis or A-B comparisons.

# Example File

```json
{
  "file_format_version":1,
  "duration": "30d",
  "metric":"download_throughput",
  "ip_translation":{
    "strategy":"maxmind",
    "params":{
      "db_snapshots":["2012-05-09", "2012-05-14", "2012-06-03"]
    }
  },
  "subsets":[
    {
      "site":"lga01",
      "client_provider":"Verizon",
      "start_time":"2014-07-01T00:00:00+00:00"
    },
    {
      "site":"lga02",
      "client_provider":"Verizon",
      "start_time":"2014-07-01T00:00:00+00:00"
    }
  ]
}
```

# Field Specifications

`file_format_version`: Specifies the version of this format file. This must be 1.

`duration`: Duration of time window (in days). Value must end with 'd'.

`metric`: Text name of the metric for which to gather data. Valid values are:
* `all` - Retrieves data for all metrics.
* `download_minimum_rtt`
* `download_average_rtt`
* `download_maximum_segment_size`
* `upload_maximum_segment_size`
* `packet_retransmit_rate`
* `download_throughput`
* `upload_throughput`
* `hop_count`

`ip_translation`: Specifies a dictionary of settings that describe how to translate the IP addresses found in the M-Lab data into client providers (as specified in client_provider fields).

`strategy`: Specifies the strategy to use in order to translate IP addresses into providers. This value must be:
* `maxmind` - Use the MaxMind database.

`params`: Specifies the parameters to the IP translation strategy.

`db_snapshots`: Specifies the snapshot dates (in YYYY-MM-DD format) of the MaxMind databases that are required to resolve IP addresses to providers. This field is optional. If not specified or specified as an empty list, consuming programs should use database snapshots closest in time to the snapshots specified and should suppress warnings to the user about missing snapshots.

`selector`: This field is not yet defined.

`subsets`: An array containing either 1 or 2 items specifying the subsets of data to select.
* Note: In a valid `subsets` array, only one value should differ between the two items in a pair, while all others should be equal. If more than one value is unequal, the consuming application should reject the data as erroneous. For example, if the first subset specifies lga01 and Verizon, the second subset cannot specify lax01 and Comcast because then the subsets would be varying by more than one parameter.

`site`: Name of M-Lab site where data was collected.

`client_provider`: Text name of the client provider. This is the substring that should appear in all AS names when a mapping is performed from this parameter to corresponding ASes. For example, "Verizon" should match AS names "Verizon Online LLC", "MCI Communications Services, Inc. d/b/a Verizon Business", "Verizon Data Services LLC", etc.

`start_time`: Start time of the window in which to collect test results (in ISO 8601 format). This value must end in +00:00 (i.e. only UTC time zone is supported).
