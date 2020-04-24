# data-manager
# 自动化HDFS数据清理的终极方法

代码地址：https://github.com/lijufeng2016/data-manager

## 一、背景

hdfs集群从出生到成长，经历了各种各样业务和人的摧残，早已疲惫不堪，承受巨大压力。某天，你突然发现hdfs的空间超过80%的告警阈值，这时候，你的第一反应是找出那些比较占空间的目录，手动删除，或者是写个定时脚本每天清理固定的目录，随着业务和时间的不断摧残，小朋友，你是否有很多问号？

- 占大头的文件清理后还是发现hdfs占用空间大，不停地找要清理的目录，最后发现小文件加起来也占用大
- 同时很多人使用同一个集群，经过互联网的几番大洗礼后hdfs留下很多已离职人员的大量文件不知所措
- 哪些数据该清还是不该清毫无头绪，万一误删了呢
- 很多垃圾文件存放在hdfs上，但是分不清哪些到底是垃圾文件，不知道文件还有没有人用



针对上述问题，我们可不可以换一种清理hdfs的思路呢？不需要反复修改脚本去指定特定要删的目录呢？也不需要为了找哪些需要清理的目录而焦头烂额呢？来，接下来变魔术给你看。

## 二、原理

本文介绍一种方法，可以清hive数据，也可以清非hive表的hdfs数据。基本原理是通过解析hadoop fsimage文件获得hdfs全量的文件路径和所有文件最后的访问时间，请hive表数据则还需要加上hive的元数据信息。

### fsimage：

fsimage是hdfs的心脏，hdfs的全量的路径信息都存放在fsimage文件里面。我们在操作hdfs时，不论是增删改查，hadoop都会记录一条edit log，也就是hdfs的操作记录，edit log会定时merge生成fsimage文件，在HA模式下，fsimage文件由standby NameNode生成，单点模式下，由secondary NameNode生成。fsimage文件本身是二进制不可明文读取的，我们需要解析成可读的形式，比如csv。hadoop自带的命令`hdfs oiv`是专门用来解析fsimage文件，通过执行`hdfs getconf -confKey dfs.namenode.name.dir`命令可以知道fsimage的路径，在路径下默认会保存两个fsimage文件，都是fsimage_xxxxxxxxxxx的格式带一串时间戳，时间戳最大的那个就是由最新edit log合并解析生成的。

执行:

```shell
hdfs oiv -p Delimited -delimiter "," -i fsimage_xxxxxxxx -o fsimage.csv
```

解析fsimage生成csv文件，文件内容包含了hdfs所有文件和目录，csv包含如下列：

-  **Path**  目录路径
-  Replication  备份数
-  ModificationTime  最后修改时间
-  **AccessTime** 最后访问时间
-  PreferredBlockSize   首选块大小 byte
-  BlocksCount  块 数
-  FileSize 文件大小 byte
-  NSQUOTA 名称配额 限制指定目录下允许的文件和目录的数量。
-  DSQUOTA  空间配额 限制该目录下允许的字节数
-  Permission   权限
-  UserName    用户
-  GroupName 用户组

加粗的部分，是两个最重要的字段，**AccessTime**作为hdfs文件访问的最后时间，**可以根据它去确定哪些文件还在用，哪些已经很久没用，可以判定为垃圾文件或过期数据，达到清理的目的**。必须要开启`dfs.namenode.accesstime.precision`参数才会有AccessTime，默认开启值为1。但是在hdp集群是默认关闭的，注意要在hdfs-site.xml文件里面配置开启。

解析后的csv文件会上传到对应字段的建好的hive表，给后面清理逻辑使用

### hive元数据

一般在配置hive的时候，都会选用mysql作为元数据存储的介质，hive的元数据表很多，记录了表名、分区、路径、参数等等一切除了表数据之外的所有信息，我们在hive的元数据库里面需要知道表的**hdfs路径**和**分区**，清理hive数据的时候再根据上述的fsimage对应的hive表去做关联，把要清理的表或表分区关联出来

## 三、使用方法

代码地址：https://github.com/lijufeng2016/data-manager

主类：

`com.xkj.mlrc.fsimage.GenerateFsimageTable`：解析生成fsimage的csv文件并上传到hive

`com.xkj.mlrc.clean.table.HiveTableClean`：清理hive表的逻辑

`com.xkj.mlrc.clean.file.HdfsFileClean`：清理hdfs目录文件的逻辑，与上面清理hive的逻辑独立不冲突

args参数说明：

| 参数名          | 说明                                                         |
| --------------- | ------------------------------------------------------------ |
| -targetPath     | 指定的要删的目标路径，逗号隔开                               |
| -avoidPath      | 要避开删除的路径，不扫描的路径,逗号隔开                      |
| -avoidSuffix    | 要避开的包含后缀的文件,逗号隔开                              |
| -avoidPrefix    | 要避开的包含前缀的文件,逗号隔开                              |
| -avoidDbs       | 要避免删除的hive库，包含库下所有的表分区，逗号隔开           |
| -avoidTbls      | 要避免删除的hive表，包含表下所有的分区，逗号隔开             |
| -avoidTbls-file | 用要避免删除的表，用文件存放在hdfs，必须是“库.表名”的形式，包含表下所有的分区 |
| -expire         | 过期的数据时间，也就是清理多少天之前的数据，这是个参数很重要，必须大于0                                                   |
| -hdfsroot       | hdfs根路径，HA模式如 hdfs://bigdatacluster，单点模式如：hdfs://xxxx:50070 |

必要的准备

### 本地idea运行：

#### step1：准备工作

必须要把**hive-site.xml、core-site.xml、hdfs-site.xml**文件放在项目的resources下，否则运行不起来！然后按照自己的环境修改所有**config.properties**配置项。

#### step2:解析fsimage文件

执行主类**com.xkj.mlrc.fsimage.GenerateFsimageTable**，会远程ssh到NameNode执行一系列shell并解析fsimage文件上传到hdfs

#### step3:清理数据

根据自己的需要运行

`com.xkj.mlrc.clean.table.HiveTableClean`  或 `com.xkj.mlrc.clean.file.HdfsFileClean`清理hive表或hdfs数据，并根据上面的args参数说明列表传入自己需要的参数运行



### yarn运行：

#### step1：准备工作

照自己的环境修改所有**config.properties**配置项，maven打包项目生成data-manager.jar文件上传到集群机器上

#### step2:解析fsimage文件

执行项目run目录下的`ParseFsimageFile2Hive.sh`脚本，会远程ssh到NameNode执行一系列shell并解析fsimage文件上传到hdfs

#### step3:清理数据

根据自己的需要运行项目run目录下的`HdfsFileClean.sh`  或 `HiveTableClean.sh`脚本清理hive表或hdfs数据，根据自己的需要配置上面的args参数列表

## 四、总结

这种方法完美的利用了fsimage文件和hive元数据。传统删数据的方法需要用户知道哪些目录该删或不该删，用这种方法，你只需要关注多久没使用过的数据就删，比如有的文件连续超过7天未被读取，之后被读取的可能性也不大，就可以用上面的代码去做清理。代码里也特意做了安全机制，hdfs的java api中，直接删除的话不会放hdfs的回收站，这个项目里是把所有数据放入回收站，等到回收触发的时间才彻底删除，如果误删了数据也可以有时间恢复。
