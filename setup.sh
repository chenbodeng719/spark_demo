#!/bin/bash
# sudo su
sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hsconn.zip /home/emr_tool/
sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hbase_jar.zip /home/emr_tool/
# sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hbase-site.xml /home/emr_tool/
# aws s3 cp s3://htm-test/chenbodeng/mytest/hadoop_jars.zip ./
# sudo aws s3 cp s3://htm-test/chenbodeng/mytest/spark_jars.zip /home/hadoop/
cd /home/emr_tool
sudo unzip hsconn.zip
sudo unzip hbase_jar.zip
sudo pip3 install boto3 scikit-learn


