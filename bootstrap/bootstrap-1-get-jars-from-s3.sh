#!/bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
aws s3 cp s3://larsbk/jars/spark-xml_2.10-0.3.2.jar /home/hadoop/
fi
