#!/bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/classes.py -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/analysis.py -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/schema.p -P /home/hadoop/IPythonNB/
wget https://raw.githubusercontent.com/larsbkrogvig/strava-spark/master/00-pyspark-setup-EMR.py -P /home/hadoop/
fi


