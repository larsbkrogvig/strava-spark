#!/bin/bash
set -x -e

#Installing iPython Notebook
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
cd /home/hadoop
sudo yum -y install git-all
sudo pip install virtualenv

# Clone strava-spark reop
git clone https://github.com/larsbkrogvig/strava-spark.git 

cd strava-spark/py
/usr/local/bin/virtualenv -p /usr/bin/python2.7 venv
source venv/bin/activate

#Install ipython and dependency
pip install "ipython[notebook]"
pip install requests numpy cython boto3
pip install matplotlib

#Install pandas
git clone https://github.com/pydata/pandas.git
cd pandas
python setup.py install
python setup.py build_ext --inplace
cd ..

#Create profile 	
ipython profile create default

#Run on master /slave based on configuration
echo "c = get_config()" >  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.ip = '*'" >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.open_browser = False"  >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py
echo "c.NotebookApp.port = 8192" >>  /home/hadoop/.ipython/profile_default/ipython_notebook_config.py

# Place downloaded pyspark conifg in correct directory
mv /home/hadoop/strava-spark/config/00-pyspark-setup-EMR.py /home/hadoop/.ipython/profile_default/startup/00-default-setup.py

nohup ipython notebook --no-browser > /mnt/var/log/python_notebook.log &
fi