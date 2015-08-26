# Overview

The data subset selector file specifies subsets of M-Lab data for single dataset analysis.

# Example File

```json
{
    "file_format_version": 1.1,
    "duration": "30d",
    "metrics": ["download_throughput"],
    "ip_translation":{
        "strategy": "maxmind",
        "params": {
          "db_snapshots": ["2014-08-04"]
        }
    },
    "sites": ["lga01"],
    "client_providers": ["Verizon"],
    "start_time": "2014-07-01T00:00:00Z"
}
```

# Field Specifications

`file_format_version`: Specifies the version of this format file. This must be '1.1'.

`duration`: Duration of time window (in days). Value must end with 'd'.

`metrics`: A list of metrics for which to gather data. Valid values are:
* `average_rtt`
* `minimum_rtt`
* `download_throughput`
* `upload_throughput`
* `packet_retransmit_rate`

`ip_translation`: Specifies a dictionary of settings that describe how to translate the IP addresses found in the M-Lab data into client providers (as specified in client_provider fields).

`strategy`: Specifies the strategy to use in order to translate IP addresses into providers. This value must be:
* `maxmind` - Use the MaxMind database.

`params`: Specifies the parameters to the IP translation strategy.

`db_snapshots`: Specifies the snapshot dates (in YYYY-MM-DD format) of the MaxMind databases that are required to resolve IP addresses to providers. This field is optional. If not specified or specified as an empty list, consuming programs should use database snapshots closest in time to the snapshots specified and should suppress warnings to the user about missing snapshots.

`sites` _(optional)_: A list of M-Lab sites indicated by their id (e.g. iad01) where the measurement was conducted against.

`client_providers` _(optional)_: A list of names of client providers. The client provider name is the substring that should appear in all AS names when a mapping is performed from this parameter to corresponding ASes. For example, "Verizon" should match AS names "Verizon Online LLC", "MCI Communications Services, Inc. d/b/a Verizon Business", "Verizon Data Services LLC", etc. Supports a limited number of provider metanames that translate to all known AS name queries for that provider:

* `twc`: Time Warner Cable
* `centurylink`: CenturyLink
* `level3`: Level 3 Communications
* `cablevision`: Cablevision Communications

`client_countries` _(optional)_: A list of ISO 3166-1 alpha-2 country code(s) associated with the IP address of the measurement client. The geolocation of the client address is recorded within BigQuery at the time of measurement based on MaxMind current to that date.

`start_times`: List of start times of the window in which to collect test results (in ISO 8601 format). Start time values must end in `Z` (i.e. only UTC time zone is supported).

# Changelog 

## As of version 1.1

* Added optional `client_countries` property.
* The properties `metric`, `client_provider` and `site` are now represented by the lists `metrics`, `client_providers` and `sites`. 
* Made `client_providers` and `sites` optional.

# Deprecated

## As of version 1.1

`subsets`: An array containing either 1 or 2 items specifying the subsets of data to select.
* Note: In a valid `subsets` array, only one value should differ between the two items in a pair, while all others should be equal. If more than one value is unequal, the consuming application should reject the data as erroneous. For example, if the first subset specifies lga01 and Verizon, the second subset cannot specify lax01 and Comcast because then the subsets would be varying by more than one parameter.

