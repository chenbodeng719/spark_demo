#!/bin/bash
# sudo su
sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hsconn.zip /home/hadoop/
sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hbase_jar.zip /home/hadoop/
sudo aws s3 cp s3://hiretual-ml-data-test/cbd/emr_tool/hbase-site.xml /usr/lib/spark/conf/
# aws s3 cp hsconn.zip s3://hiretual-ml-data-test/cbd/emr_tool/
# aws s3 cp hbase_jar.zip s3://hiretual-ml-data-test/cbd/emr_tool/
cd /home/hadoop
sudo unzip hsconn.zip
sudo unzip hbase_jar.zip
