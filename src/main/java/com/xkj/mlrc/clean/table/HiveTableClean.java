package com.xkj.mlrc.clean.table;

import com.xkj.mlrc.clean.domain.ParamOption;
import com.xkj.mlrc.clean.util.ArgsUtil;
import com.xkj.mlrc.clean.util.HdfsUtils;
import com.xkj.mlrc.clean.util.JdbcHelper;
import com.xkj.mlrc.clean.util.PropsUtil;
import com.xkj.mlrc.fsimage.GetFromFsImageInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;
import scala.reflect.ClassManifestFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * @author lijf@2345.com
 * @date 2020/4/21 16:30
 * @desc
 */
@Slf4j
public class HiveTableClean {
    private static SparkSession spark;
    private static Properties properties;
    private static ParamOption option;
    private static volatile Broadcast<List<String>> broadcast = null;


    public static void main(String[] args) throws IOException {
        init(args);
        loadTableInfoFromMysql();
        loadExpireDataByFsimage();
        cleanUnPartitiondTables();
        cleanPartitiondTables();
    }

    /**
     * 获取fsimage记录的所有过期的目录
     */
    public static void loadExpireDataByFsimage() {
        GetFromFsImageInfo fsImage = GetFromFsImageInfo.builder()
                .spark(spark)
                .avoidPrefix(option.avoidPrefix)
                .avoidSuffix(option.avoidSuffix)
                .expire(option.expire)
                .targetPath(option.targetPath)
                .hdfsroot(option.hdfsroot)
                .build();
        fsImage.getAllShouldDelDirsByLastAccesstime().createOrReplaceTempView("all_overdue_dirs");
    }

    /**
     * 初始化
     *
     * @param args 参数
     */
    private static void init(String[] args) {
        spark = getSparkSession();
        properties = new Properties();
        String url = PropsUtil.getProp("hive.meta.mysql.url");
        properties.put("url", url);
        properties.put("driver", PropsUtil.getProp("hive.meta.mysql.driver"));
        properties.put("user", PropsUtil.getProp("hive.meta.mysql.username"));
        properties.put("password", PropsUtil.getProp("hive.meta.mysql.password"));
        option = ArgsUtil.getOption(args);
    }

    /**
     * 清理非分区表
     */
    private static void cleanUnPartitiondTables() throws IOException {
        //查询出hive的所有表
        String sqlText = "select a.name as dbname," +
                "                b.tbl_name," +
                "                case when isnull(c.pkey_name) then 0 else 1 end as ispartition," +
                "                d.location " +
                "         from dbs a " +
                "         join tbls b on(a.db_id=b.db_id)" +
                "         left join partition_keys c on(b.tbl_id=c.tbl_id)" +
                "         join sds d on(b.sd_id=d.sd_id)";
        spark.sql(sqlText).createOrReplaceTempView("all_hive_tables");
        String distinctLines = "select dbname," +
                "                  tbl_name," +
                "                  ispartition," +
                "                  location " +
                "              from all_hive_tables " +
                "              where ispartition=0" +
                "              group by dbname,tbl_name,ispartition,location";
        // 去重记录
        spark.sql(distinctLines).createOrReplaceTempView("all_unpartitiond_tbs");
        // 获取所有要删的过期表，join fsimage的路径得到
        String getExpiredTbs = "select a.* from all_unpartitiond_tbs a join all_overdue_dirs b on(a.location=b.hdfs_abs_path)";
        Dataset<Row> allUnpartitiondTables = spark.sql(getExpiredTbs);
        // 过滤要排除掉的表
        allUnpartitiondTables = filterExclusionTables(allUnpartitiondTables);
        // 获取非分区表的库名、表名、路径
        allUnpartitiondTables
                .toJavaRDD()
                .map(row -> new Tuple3<>(row.getAs("dbname").toString(), row.getAs("tbl_name").toString(), row.getAs("location").toString()))
                .foreachPartition(partition -> {
                    JdbcHelper jdbcHelper = JdbcHelper.getHiveInstance();
                    while (partition.hasNext()){
                        Tuple3<String, String, String> tableLine = partition.next();
                        String dbname = tableLine._1();
                        String tblName = tableLine._2();
                        String location = tableLine._3();
                        //删除表
                        String table = dbname + "." + tblName;
                        String sqlTextDrop = "drop table  if exists " + table;
                        try {
                            jdbcHelper.execute(sqlTextDrop);
                            log.info("删除表成功:" + table);
                        } catch (Exception e) {
                            log.info("删除表失败:" + table);
                            e.printStackTrace();
                        }
                        //删除路径
                        try {
                            HdfsUtils.trashPath(location);
                            log.info("删除路径成功:" + location);
                        } catch (IOException e) {
                            if (e instanceof FileNotFoundException) {
                                log.info("删除路径成功:" + location);
                            } else {
                                log.error("删除路径失败:" + location);
                                e.printStackTrace();
                            }
                        }
                    }
                });
    }

    /**
     * 过滤掉要排除的库表
     *
     * @param tablesDataset tablesDataset
     * @return Dataset<Row>
     * @throws IOException IOException
     */
    private static Dataset<Row> filterExclusionTables(Dataset<Row> tablesDataset) throws IOException {
        String avoidTbls = option.avoidTbls;
        String avoidDb = option.avoidDb;
        String avoidTblsFile = option.avoidTblsFile;

        if (null != avoidDb) {
            tablesDataset = tablesDataset.filter(new FilterFunction<Row>() {
                @Override
                public boolean call(Row row) throws Exception {
                    return !avoidDb.equalsIgnoreCase(row.getAs("dbname").toString());
                }
            });
        }

        if (null != avoidTbls) {
            String[] tables = avoidTbls.split(",");
            for (int i = 0; i < tables.length; i++) {
                String table = tables[i];
                tablesDataset = tablesDataset.filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row row) throws Exception {
                        String dbname = row.getAs("dbname").toString();
                        String tblName = row.getAs("tbl_name").toString();
                        String tableName = dbname + "." + tblName;
                        return !table.equalsIgnoreCase(tableName);
                    }
                });
            }
        }
        if(null != avoidTblsFile){
            List<String> tables = HdfsUtils.readByLine(avoidTblsFile);
            broadcast = spark.sparkContext().broadcast(tables, ClassManifestFactory.classType(List.class));
            tablesDataset = tablesDataset.filter(new FilterFunction<Row>() {
                List<String> exclusionTablesValue = broadcast.value();
                @Override
                public boolean call(Row row) throws Exception {
                    String dbname = row.getAs("dbname").toString();
                    String tblName = row.getAs("tbl_name").toString();
                    String table = dbname + "." + tblName;
                    return !exclusionTablesValue.contains(table);
                }
            });
        }
        return tablesDataset;
    }

    /**
     * 清理分区表
     */
    private static void cleanPartitiondTables() throws IOException {
        String allPartitionTbs = "SELECT " +
                "                    a.name as dbname, " +
                "                    b.tbl_name, " +
                "                    c.location, " +
                "                    d.part_name," +
                "                    concat(location,'/',part_name) as part_location " +
                "                FROM " +
                "                    dbs a " +
                "                JOIN tbls b ON (a.db_id = b.db_id)  " +
                "                JOIN sds c ON (b.sd_id = c.sd_id) " +
                "                JOIN partitions d ON (b.tbl_id = d.tbl_id) ";
        // 获取所有分区表
        spark.sql(allPartitionTbs).createOrReplaceTempView("allPartitionTbs");
        String getExpiredParts = "select a.* from allPartitionTbs a join all_overdue_dirs b on(a.part_location=b.hdfs_abs_path)";
        Dataset<Row> partitiondTables = spark.sql(getExpiredParts);
        partitiondTables = filterExclusionTables(partitiondTables);
        partitiondTables.foreachPartition(parttition -> {
            JdbcHelper jdbcHelper = JdbcHelper.getHiveInstance();
            while (parttition.hasNext()){
                Row row = parttition.next();
                String dbName = row.getAs("dbname").toString();
                String tblName = row.getAs("tbl_name").toString();
                String partLocation = row.getAs("part_location").toString();
                String partName = row.getAs("part_name").toString();

                // 解析出分区名
                String[] split = partName.split("/");
                for (int j = 0; j < split.length; j++) {
                    String part = split[j];
                    split[j] = part.replace("=", "='") + "'";
                }
                String partNameFmt = StringUtils.join(split, ",");
                String tableName = dbName + "." + tblName;
                String dropPartitionSql = "ALTER TABLE " + tableName + " DROP IF EXISTS PARTITION (" + partNameFmt + ")";
                try {
                    jdbcHelper.execute(dropPartitionSql);
                    log.info("删除表分区成功！表名：{}，分区：{}", tableName, partNameFmt);
                } catch (Exception e) {
                    log.info("删除表分区失败！表名：{}，分区：{}", tableName, partNameFmt);
                    e.printStackTrace();
                }
                //删除路径
                try {
                    HdfsUtils.trashPath(partLocation);
                    log.info("删除分区路径成功！表名：{}，分区：{}，路径：{}", tableName, partNameFmt, partLocation);
                } catch (Exception e) {
                    if (e instanceof FileNotFoundException) {
                        log.info("删除分区路径成功！表名：{}，分区：{}，路径：{}", tableName, partNameFmt, partLocation);
                    } else {
                        log.info("删除分区路径失败！表名：{}，分区：{}，路径：{}", tableName, partNameFmt, partLocation);
                        e.printStackTrace();
                    }
                }

            }
        });

    }

    /**
     * 读取mysql中hive的元数据得到所有表信息
     */
    private static void loadTableInfoFromMysql() {
        spark.read().jdbc(properties.getProperty("url"), "DBS", properties).write().mode(SaveMode.Overwrite).saveAsTable("dbs");
        spark.read().jdbc(properties.getProperty("url"), "PARTITION_KEYS", properties).write().mode(SaveMode.Overwrite).saveAsTable("partition_keys");
        spark.read().jdbc(properties.getProperty("url"), "SDS", properties).write().mode(SaveMode.Overwrite).saveAsTable("sds");
        spark.read().jdbc(properties.getProperty("url"), "PARTITION_KEY_VALS", properties).write().mode(SaveMode.Overwrite).saveAsTable("partition_key_vals");
        spark.read().jdbc(properties.getProperty("url"), "PARTITIONS", properties).write().mode(SaveMode.Overwrite).saveAsTable("partitions");
        spark.read().jdbc(properties.getProperty("url"), "TBLS", properties).write().mode(SaveMode.Overwrite).saveAsTable("tbls");

    }

    private static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .master("local[2]")
                .appName(HiveTableClean.class.getSimpleName())
                .enableHiveSupport()
                .getOrCreate();
    }
}
