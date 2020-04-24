#!/usr/bin/env bash
#创建hive表
hive -S -e "CREATE TABLE IF NOT EXISTS fsimage( \
path string, \
replication int, \
modificationtime string, \
accesstime string, \
preferredblocksize bigint, \
blockscount int, \
filesize bigint, \
nsquota int, \
dsquota int, \
permission string, \
username string, \
groupname string) \
ROW FORMAT DELIMITED \
FIELDS TERMINATED BY ',' \
STORED AS INPUTFORMAT \
'org.apache.hadoop.mapred.TextInputFormat' \
OUTPUTFORMAT \
'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'  \
location '/tmp/fsimage'"

#在NameNode节点，最好是standby NameNode解析fsimage文件
nn_paths=`hdfs getconf -confKey dfs.namenode.name.dir`
nn_path=${nn_paths##*,}
echo ${nn_path}
# 找到后缀名最大的那个fsimage文件，如：fsimage_0000000000157279038
fsimage_file=`find ${nn_path}/current -type f -name 'fsimage_*' | grep -v '.md5' | sort -n | tail -n1`
#解析fsimage文件成csv
hdfs oiv -p Delimited -delimiter "," -i ${fsimage_file} -o fsimage.csv
#上传到hive表，供后面做分析和删除文件使用
hadoop fs -put -f fsimage.csv /tmp/fsimage/

