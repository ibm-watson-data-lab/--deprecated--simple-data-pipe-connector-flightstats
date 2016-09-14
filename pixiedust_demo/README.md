# pixiedust_flightpredict

[Pixiedust](https://github.com/ibm-cds-labs/pixiedust) demo of the [Predict Flight Delays with Apache Spark MLLib, FlightStats, and Weather Data](https://developer.ibm.com/clouddataservices/2016/08/04/predict-flight-delays-with-apache-spark-mllib-flightstats-and-weather-data/) tutorial.  


## Installation

1. Clone the [Github repo](https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats)

2. Run `!pip install` from with a notebook cell:
  
  ```
  !pip install --user --upgrade --no-deps -e <cloned_github_repo>/pixiedust_demo
  ```
  
  where `<cloned_github_repo>` is the full path to the directory of cloned `simple-data-pipe-connector-flightstats` repo (e.g., `/Users/username/gitrepos/simple-data-pipe-connector-flightstats`)  


## Usage

From a notebook cell run:

```
from pixiedust_flightpredict import *
flightPredict()
```

## Uninstall

From a notebook cell run: `!pip uninstall pixiedust_flightpredict `