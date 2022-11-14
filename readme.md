# spark demo

## tips 
- Your pyspark job project must contain the following files. 
    - python file which has main function in it.
    - setup shell file which has something to do before main function.


## main func
### spark
- main.py
- setup.sh4
- command
```
python -u main.py --cate spark_hbase_local
```
## command
- common
```
aws s3 cp main.py s3://htm-test/chenbodeng/mytest/
aws s3 cp setup_spark.sh s3://htm-test/chenbodeng/mytest/
aws s3 cp s3://htm-test/chenbodeng/mytest/hsconn.zip ./

aws s3 rm s3://htm-test/chenbodeng/hbase/ --recursive


wget https://repo1.maven.org/maven2/org/apache/hive/hive-hbase-handler/3.1.3/hive-hbase-handler-3.1.3.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-hbase-handler/3.1.2/hive-hbase-handler-3.1.2.jar

```


- hbase
```
create 'mytable','f1'
put 'mytable', 'row1', 'f1:name', 'Gokhan'
put 'mytable', 'row2', 'f1:name', 'test'
put 'mytable', 'test3', 'f1:name', 'test3'
put 'mytable', 'test4', 'f1:name', 'test3'
scan 'mytable'
scan 'candidate', {'LIMIT' => 5}
major_compact 'candidate'
```

- hive


```
CREATE EXTERNAL TABLE myhivetable (rowkey STRING, name STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f1:name')
TBLPROPERTIES ('hbase.table.name' = 'mytable');	

CREATE TABLE IF NOT EXISTS myhivetable_out
STORED AS PARQUET
AS 
SELECT * FROM myhivetable limit 2

CREATE EXTERNAL TABLE parquet_hive (
  foo string
) STORED AS PARQUET
LOCATION 's3://htm-test/chenbodeng/mytest/parquet_hive';

INSERT INTO parquet_hive SELECT name FROM myhivetable;


aws s3 cp s3://htm-test/chenbodeng/mytest/hive-site.xml ./


INSERT INTO TABLE myhivetable VALUES ('test','test');


```