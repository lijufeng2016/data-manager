package com.xkj.mlrc.fsimage;

import com.xkj.mlrc.clean.util.DateUtil;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * @author lijf@2345.com
 * @date 2020/4/22 14:18
 * @desc 获取fsimage信息表
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder()
public class GetFromFsImageInfo implements Serializable {
    private SparkSession spark;
    // 目标路径，要删除的路径，包含所有子路径
    private String targetPath;
    // 要避开扫描的路径，包含所有子路径
    private String avoidPath;
    // 要过滤的带有前缀的文件
    private String avoidSuffix;
    // 要过滤的带有后缀的文件
    private String avoidPrefix;
    // 过滤多少天之前未读的数据
    private Integer expire;
    // hdfs根路径
    private String hdfsroot;

    /**
     * 获取要删除的所有文件
     *
     * @return Dataset<Row>
     * @throws NullPointerException 空指针异常
     */
    public Dataset<Row> getAllFiles() throws NullPointerException {
        if (expire == null || expire == 0) {
            throw new NullPointerException("expire必须大于0！！");
        }
        // 获取过期数据的日期
        String nDayFmtDAte = DateUtil.getNDayFmtDAte("yyyy-MM-dd HH:mm", 0 - expire);
        // 获取要删的文件
        String sqlText = "select * from fsimage where replication>0 and accesstime<'" + nDayFmtDAte + "'";
        Dataset<Row> fsImage = spark.sql(sqlText);

        // 以下根据传入的各个参数过滤数据
        if (null != targetPath) {
            fsImage = fsImage.filter(new FilterFunction<Row>() {
                @Override
                public boolean call(Row row) throws Exception {
                    String[] targetPaths = targetPath.split(",");
                    boolean contains = false;
                    for (int i = 0; i < targetPaths.length; i++) {
                        String path = targetPaths[i];
                        String fileAbsPath = row.getAs("path").toString();
                        if (fileAbsPath.startsWith(path)) {
                            contains = true;
                        }
                    }
                    return contains;
                }

            });
        }
        if (null != avoidPath) {
            String[] avoidPaths = avoidPath.split(",");
            for (int i = 0; i < avoidPaths.length; i++) {
                String path = avoidPaths[i];
                fsImage = fsImage.filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row row) throws Exception {
                        String fileAbsPath = row.getAs("path").toString();
                        return !fileAbsPath.startsWith(path);
                    }
                });
            }
        }

        if (null != avoidSuffix) {
            String[] avoidSuffixs = avoidSuffix.split(",");
            for (int i = 0; i < avoidSuffixs.length; i++) {
                String suffix = avoidSuffixs[i];
                fsImage = fsImage.filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row row) throws Exception {
                        String path = row.getAs("path").toString();
                        return !path.endsWith(suffix);
                    }
                });
            }
        }
        if (null != avoidPrefix) {
            String[] avoidPrefixs = avoidPrefix.split(",");
            for (int i = 0; i < avoidPrefixs.length; i++) {
                String prefix = avoidPrefixs[i];
                fsImage = fsImage.filter(new FilterFunction<Row>() {
                    @Override
                    public boolean call(Row row) throws Exception {
                        String pathName = row.getAs("path").toString();
                        String fileName = new Path(pathName).getName();
                        return !fileName.startsWith(prefix);
                    }
                });
            }
        }
        return fsImage;
    }


    /**
     * 获取所有文件的目录
     *
     * @return Dataset<Row>
     */
    public Dataset<Row> getAllFilesDir() {
        Dataset<Row> allFiles = getAllFiles();
        Dataset<Row> fsimage = allFiles.selectExpr("*", "SUBSTRING_INDEX(path,'/',size(split(path,'/'))-1) as dir");
        return fsimage;
    }

    /**
     * 根据目录下的访问时间最大的文件决定目录是否删除
     *
     * @return Dataset<Row>
     */
    public Dataset<Row> getAllShouldDelDirsByLastAccesstime() {
        getAllFilesDir().createOrReplaceTempView("fsimage_dirs");
        String sqlText = "select * from " +
                "          (" +
                "            select *," +
                "                  row_number() over (partition by dir  order by accesstime desc) rank " +
                "            from fsimage_dirs" +
                "          ) tmp where rank=1";
        Dataset<Row> fsimage = spark.sql(sqlText).selectExpr("*", "concat('" + hdfsroot + "',dir) as hdfs_abs_path");
        return fsimage;
    }
}
