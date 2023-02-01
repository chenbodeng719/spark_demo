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
aws s3 cp schema.py s3://htm-test/chenbodeng/mytest/
aws s3 cp hive-site.xml s3://htm-test/chenbodeng/mytest/
aws s3 cp  s3://htm-test/chenbodeng/mytest/hive-site.xml ./
aws s3 cp util.py s3://htm-test/chenbodeng/mytest/spark_demo/
aws s3 cp setup.sh s3://htm-test/chenbodeng/mytest/setup_spark.sh
aws s3 cp s3://htm-test/chenbodeng/mytest/hsconn.zip ./
aws s3 cp ./main.py s3://htm-test/chenbodeng/mytest/main.py

aws s3 rm s3://htm-test/chenbodeng/hbase_fuck/ --recursive


zip -r spark_demo.zip spark_demo -x spark_demo/jar/\* spark_demo/.git/\*
aws s3 cp spark_demo.zip s3://htm-test/chenbodeng/mytest/spark_demo/


wget https://repo1.maven.org/maven2/org/apache/hive/hive-hbase-handler/3.1.3/hive-hbase-handler-3.1.3.jar
wget https://repo1.maven.org/maven2/org/apache/hive/hive-hbase-handler/3.1.2/hive-hbase-handler-3.1.2.jar

```


- hbase
```
create 'mytable','f1'
create 'candidate_test','f1'
put 'mytable', 'row1', 'f1:name', 'Gokhan'
put 'mytable', 'row2', 'f1:name', 'test'
put 'mytable', 'test3', 'f1:name', 'test3'
put 'mytable', 'test4', 'f1:name', 'test3'
scan 'mytable'
scan 'candidate', {'LIMIT' => 5}
scan 'candidate_test', {  LIMIT => 10 }
major_compact 'candidate'
```

- hive


```


SELECT distinct ua.candidate_id FROM user_activity_tbl_hive ua where ua.`timestamp` >= 1669824000 and ua.`timestamp` <= 1669910400


SELECT distinct (ua.candidate_id),ch.oridata FROM user_activity_tbl_hive ua join hive_candidate ch on ua.candidate_id = ch.uid where ua.`timestamp` >= 1669824000 and ua.`timestamp` <= 1669910400  

SELECT ua.candidate_id,ch.oridata FROM user_activity_tbl_hive ua LEFT OUTER JOIN hive_candidate ch on ua.candidate_id = ch.uid

set hbase.zookeeper.quorum=ec2-18-144-4-89.us-west-1.compute.amazonaws.com;


CREATE EXTERNAL TABLE hive_candidate (uid STRING, oridata STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f1:data')
TBLPROPERTIES ('hbase.table.name' = 'candidate');	

CREATE EXTERNAL TABLE hive_candidate_test (uid STRING, oridata STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f1:data')
TBLPROPERTIES ('hbase.table.name' = 'candidate_test');	

select * from hive_candidate limit 3;

INSERT OVERWRITE TABLE all_profile SELECT uid,oridata from hive_candidate
INSERT OVERWRITE TABLE all_profile SELECT uid,oridata from hive_candidate_test


aws s3 rm s3://hiretual-ml-data-test/dataplat_test/data/all_profile_hive --recursive

sudo -u hdfs hadoop fs -mkdir /user/jovyan
sudo -u hdfs hadoop fs -chown jovyan /user/jovyan
a
sudo -u hdfs hadoop fs -mkdir /user/emr-notebook
sudo -u hdfs hadoop fs -chown emr-notebook /user/emr-notebook



aws s3 ls s3://hiretual-ml-data-test/dataplat_test/data/all_profile

```

- jupyterhub
```
docker exec jupyterhub useradd -m -s /bin/bash -N kuangzhengli
docker exec jupyterhub bash -c "echo kuangzhengli:kuangzhengli | chpasswd"
```