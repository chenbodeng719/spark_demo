#!/bin/bash
# sudo su
sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hsconn.zip /home/emr_tool/
sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hbase_jar.zip /home/emr_tool/
# sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hbase-site.xml /home/emr_tool/
# aws s3 cp hsconn.zip s3://hiretual-ml-data-test/cbd/emr_tool/
# aws s3 cp hbase_jar.zip s3://hiretual-ml-data-test/cbd/emr_tool/
# aws s3 cp hbase-site.xml s3://hiretual-ml-data-test/cbd/emr_tool/
cd /home/emr_tool
sudo unzip hsconn.zip
sudo unzip hbase_jar.zip

sudo pip3 install boto3 scikit-learn