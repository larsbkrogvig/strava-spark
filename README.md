# strava-spark
Analyzing my Strava history with Spark

## Usage
How to use this thing. Run the code in a directory with the following subdirectory structure:
* _strava-activities_
  * _athlete1_
    * <_.gpx-files_>
  * _athlete2_
    * <_.gpx-files_>
  * ...

The _.gpx_ files should follow this naming convention:
> [id]-[activity type].gpx

Typical _activity types_ are:
* Ride
* Run
* NordicSki

### Loading the dataset
In a `pyspark` program like `analysis.py`:
```
from classes import StravaLoader
df = StravaLoader().get_dataset()
```
In a `pyspark` shell:
```
from classes import StravaLoader
df = StravaLoader(sc=sc, sqlContext=sqlContext).get_dataset()
```