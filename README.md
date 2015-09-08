**Measurement Lab: Telescope**

[![Build
Status](https://travis-ci.org/m-lab/telescope.svg?branch=master)](https://travis-ci.org/m-lab/telescope)
[![Coverage
Status](https://coveralls.io/repos/m-lab/telescope/badge.svg?branch=master&service=github)](https://coveralls.io/github/m-lab/telescope?branch=master)

**Dependencies**

***Packages***

* google-api-python-client
* python-dateutil

`pip install -r requirements.txt`

**Test Dependencies**

These additional packages are required to run Telescope unit tests.

* mock
* mox

`pip install -r test-requirements.txt`

***Google Developer Console***

Telescope requires the user to have a Google account with at least one project in [Google Developer Console](https://console.developers.google.com/).

*Note*: Telescope makes queries against the M-Lab BigQuery dataset, which is free and therefore will not accrue charges against your account nor require Billing to be enabled.

To create a project:

1. Go to https://console.developers.google.com
1. Click the "Create Project" button
1. Enter any value for Project Name and Project ID
1. Click "Create"

**QuickStart**

1. `pip install -r requirements.txt`
1. Ensure that you have a project in the Google Developer Console (see above)
1. `python telescope/telescope.py documentation/examples/interconnection_study_example.json`
1. The first time you run Telescope, it will prompt you to grant OAuth permission to your Google account in order to make queries to the [M-Lab datastore in BigQuery](https://cloud.google.com/bigquery/docs/dataset-mlab)
1. When OAuth authentication completes, Telescope will perform the queries specified by the selector file. The queries may take between 45 seconds and 10 minutes to comlete.

When all queries complete, the results will be placed in the `processed/` folder. Each output file will contain results for the specified metric in CSV format of (UNIX timestamp, value).

**Working with Selector Files**

Telescope takes as input "selector files," which specify what data to retrieve. Example selector files are available in `documentation/examples`. It is simple to modify these example selector files to instruct Telescope to retrieve the data of your choice. Full documentation of selector files is available in `documentation/selector-file-spec.md`.
