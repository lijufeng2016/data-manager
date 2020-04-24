package com.xkj.mlrc.clean.file;

import com.xkj.mlrc.clean.domain.ParamOption;
import com.xkj.mlrc.clean.util.ArgsUtil;
import com.xkj.mlrc.clean.util.HdfsUtils;
import com.xkj.mlrc.fsimage.GetFromFsImageInfo;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.*;

import java.io.IOException;


/**
 * hdfs文件清理
 * @author lijf@2345.com
 * @date 2020/4/21 14:06
 * @desc
 */
@Slf4j
public class HdfsFileClean {
    public static void main(String[] args) {
        SparkSession spark = getSparkSession();
        ParamOption option = ArgsUtil.getOption(args);

        GetFromFsImageInfo fsImageInfo = GetFromFsImageInfo.builder()
                .spark(spark)
                .avoidPrefix(option.avoidPrefix)
                .avoidSuffix(option.avoidSuffix)
                .avoidPath(option.avoidPath)
                .expire(option.expire)
                .targetPath(option.targetPath)
                .hdfsroot(option.hdfsroot)
                .build();

        Dataset<Row> allFiles = fsImageInfo.getAllFiles();
        allFiles.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String path = row.getAs("path").toString();
                try {
                    HdfsUtils.trashPath(path);
                    log.info("删除路径成功:" + path);
                } catch (IOException e) {
                    log.info("删除路径失败:" + path);
                    e.printStackTrace();
                }
            }
        });


    }
    private static SparkSession getSparkSession() {
        return SparkSession
                .builder()
                .master("local[2]")
                .appName(HdfsFileClean.class.getSimpleName())
                .enableHiveSupport()
                .getOrCreate();
    }
}
