#!/bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
echo '' >> /home/hadoop/.bashrc
echo 'export SPARK_HOME=/usr/lib/spark/' >> /home/hadoop/.bashrc
echo 'export PYSPARK_SUBMIT_ARGS="--jars /home/hadoop/spark-xml_2.10-0.3.2.jar"' >> /home/hadoop/.bashrc
fi

