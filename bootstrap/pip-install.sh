#!/bin/bash
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
sudo pip install boto3
fi

