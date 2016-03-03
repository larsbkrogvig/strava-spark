#!/bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/py/classes.py -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/py/analysis.py -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/py/schema.p -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/config/00-pyspark-setup-EMR.py -P /home/hadoop/
fi


