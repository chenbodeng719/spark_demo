# spark demo

## tips 
- Your pyspark job project must contain the following files. 
    - python file which has main function in it.
    - setup shell file which has something to do before main function.


## main func
### spark
- main.py
- setup.sh
### hbase
- main_hbase.py
- setup_hbase.sh
```
aws s3 cp main_hbase.py s3://htm-test/chenbodeng/mytest/ && aws s3 cp setup_hbase.sh s3://htm-test/chenbodeng/mytest/

aws s3 cp main.py s3://htm-test/chenbodeng/mytest/ 
aws s3 cp jar/hbase-spark-1.0.0.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/hbase-client-3.0.0-alpha-3.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/scala-reflect-2.11.12.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/scala-library-2.11.12.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/spark-sql_2.11-2.4.0.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/scalatest_2.11-3.0.5.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/hbase-client-2.1.0.jar s3://htm-test/chenbodeng/mytest/ && aws s3 cp jar/shc-core-1.1.0.3.1.7.5000-4.jar s3://htm-test/chenbodeng/mytest/



aws s3 cp jar/hbase-shaded-mapreduce-2.4.9.jar s3://htm-test/chenbodeng/mytest/
aws s3 cp jar/hbase-spark-1.0.1_spark-3.2.0-hbase-2.4.9-cern1_1.jar s3://htm-test/chenbodeng/mytest/
aws s3 cp jar/hbase-spark-protocol-shaded-1.0.1_spark-3.2.0-hbase-2.4.9-cern1_1.jar s3://htm-test/chenbodeng/mytest/
aws s3 cp hbase.sh s3://htm-test/chenbodeng/mytest/

```