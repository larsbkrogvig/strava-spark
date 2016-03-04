#!/bin/bash
set -x -e

#Installing iPython Notebook
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
cd /home/hadoop
sudo pip install virtualenv
#mkdir IPythonNB # I already did this in a previous bootstrap action
cd IPythonNB
/usr/local/bin/virtualenv -p /usr/bin/python2.7 venv
source venv/bin/activate

#Install ipython and dependency
pip install "ipython[notebook]"
pip install requests numpy pandas
pip install matplotlib
pip install boto3

#Create profile 	
ipython profile create default

#Run on master /slave based on configuration
echo "c = get_config()" >  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.ip = '*'" >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.open_browser = False"  >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.port = 8192" >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py

# Place downloaded pyspark conifg in correct directory
mv /home/hadoop/00-pyspark-setup-EMR.py /home/hadoop/.ipython/profile_default/startup/00-default-setup.py

echo "sh SPARK_HOME = "$SPARK_HOME
echo "sh PYSPARK_SUBMIT_ARGS = "$PYSPARK_SUBMIT_ARGS

nohup ipython notebook --no-browser > /mnt/var/log/python_notebook.log &
fi