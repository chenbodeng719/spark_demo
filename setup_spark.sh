#!/bin/bash
# sudo su
sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hsconn.zip /home/hadoop/
sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hbase_jar.zip /home/hadoop/
sudo aws s3 cp s3://htm-test/chenbodeng/mytest/hbase-site.xml /home/hadoop/
# aws s3 cp s3://htm-test/chenbodeng/mytest/hadoop_jars.zip ./
# sudo aws s3 cp s3://htm-test/chenbodeng/mytest/spark_jars.zip /home/hadoop/
cd /home/hadoop
sudo unzip hsconn.zip
sudo unzip hbase_jar.zip

pip3 install peewee