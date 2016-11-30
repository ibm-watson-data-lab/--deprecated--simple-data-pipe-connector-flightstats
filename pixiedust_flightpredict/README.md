# pixiedust_flightpredict

[Pixiedust](https://github.com/ibm-cds-labs/pixiedust) demo of the [Predict Flight Delays with Apache Spark MLLib, FlightStats, and Weather Data](https://developer.ibm.com/clouddataservices/2016/08/04/predict-flight-delays-with-apache-spark-mllib-flightstats-and-weather-data/) tutorial.


## Usage

1. Download the [Flight Predict with Pixiedust](https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats/blob/master/notebook/Flight%20Predict%20with%20Pixiedust.ipynb) notebook.

2. Open the notebook in a Jupyter environment.

3. Follow the instructions in the notebook to run the application.

The instructions in notebook use the [pixiedust-flightpredict](https://pypi.python.org/pypi/pixiedust-flightpredict) version available in PyPI. To use a local version of the plugin:

1. Clone the [Github repo](https://github.com/ibm-cds-labs/simple-data-pipe-connector-flightstats).

2. Replace the cell:

	`!pip install --upgrade --user pixiedust-flightpredict`
	
	with
  
  `!pip install --user --upgrade -e <cloned_github_repo>/pixiedust_flightpredict`  
  
	where `<cloned_github_repo>` is the full path to the directory of cloned `simple-data-pipe-connector-flightstats` repo (e.g., `/Users/username/gitrepos/simple-data-pipe-connector-flightstats`)  

