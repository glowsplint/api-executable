# BigSchedules Production API Caller

This script will call the BigSchedules API using our production credentials. Ensure that the number of seconds per iterations do not exceed 1 per second.
For the production environment, the API call limit is maximum 2,000 calls per day and 150 calls per minute, with a maximum of 30,000 calls per month.

The script requires an input file 'BigSchedules Port Pairs - \*.csv', as well as the secrets.py file containing the production key.

## Features

1. If calls have already been done today (i.e. the json files exist in the <today_path> directory), it will not rerun those calls.
2. Will raise an exception if PulseSecure is on
