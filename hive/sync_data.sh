####### execute hive ######
# aws s3 cp sync_data.sh s3://htm-test/chenbodeng/mytest/hive_sync_data.sh
# aws s3 cp s3://htm-test/chenbodeng/mytest/hive_sync_data.sh ./
# nohup bash hive_sync_data.sh   2>&1 > hive_sync_data.out &

read -r -d  '' sql << EOF
USE default;

CREATE EXTERNAL TABLE IF NOT EXISTS hive_candidate (uid STRING, oridata STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f1:data')
    TBLPROPERTIES ('hbase.table.name' = 'candidate')


CREATE EXTERNAL TABLE IF NOT EXISTS hive_candidate_test (uid STRING, oridata STRING) STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
    WITH SERDEPROPERTIES ('hbase.columns.mapping' = ':key,f1:data')
    TBLPROPERTIES ('hbase.table.name' = 'candidate_test')


CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`all_profile` (uid STRING,oridata STRING) stored as PARQUET LOCATION "s3://hiretual-ml-data-test/dataplat_test/data/all_profile_hive" TBLPROPERTIES( "parquet.compression"="SNAPPY", "auto.purge"="true" );

CREATE EXTERNAL TABLE IF NOT EXISTS `default`.`all_profile_test` (uid STRING,oridata STRING) stored as PARQUET LOCATION "s3://hiretual-ml-data-test/dataplat_test/data/all_profile_hive_test" TBLPROPERTIES( "parquet.compression"="SNAPPY", "auto.purge"="true" );


INSERT OVERWRITE TABLE all_profile_test SELECT uid,oridata from hive_candidate_test;
EOF

############  execute begin   ###########
echo $sql
/usr/lib/hive/bin/hive -e "$sql"

exitCode=$?
if [ $exitCode -ne 0 ];then
    echo "[error] hive execute failed!"
    exit $exitCode
fi