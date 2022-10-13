if [[ $(echo "exists 'mytable'" | hbase shell | grep 'not exist') ]]; 
then 
    echo "create 'mytable','cf1';put 'mytable', 'row1', 'cf1:col1', 'val1';" | hbase shell;   
fi

echo "scan 'mytable', {'LIMIT' => 5};" | hbase shell; 

aws s3 rm 