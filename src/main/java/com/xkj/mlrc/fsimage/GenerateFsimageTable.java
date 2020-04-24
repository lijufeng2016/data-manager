package com.xkj.mlrc.fsimage;

import com.jcraft.jsch.*;
import com.xkj.mlrc.clean.util.PropsUtil;
import com.xkj.mlrc.common.shell.Shell;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

/**
 * 远程ssh解析fsimage文件
 *
 * @author lijf@2345.com
 * @date 2020/4/23 12:38
 * @desc
 */
@Slf4j
public class GenerateFsimageTable {
    public static Shell shell;

    public static void main(String[] args) throws IOException, JSchException {
        // ssh远程登录NameNode所在的机器
        sshLoginNameNodeHost();
        // 解析fsimage文件并上传到hive表对应的路径
        generateFsimageCsv2Hive();
    }

    private static void generateFsimageCsv2Hive() throws IOException, JSchException {
        //获取存放fsimage文件的目录
        String cmd1 = "hdfs getconf -confKey dfs.namenode.name.dir";
        shell.execute(cmd1);
        ArrayList<String> list1 = shell.getStandardOutput();
        String fsimageDir = list1.get(list1.size() - 1).split(",")[0];

        //获取最新的fsimage文件的路径
        String cmd2 = "find ${fsimageDir}/current -type f -name 'fsimage_*' | grep -v '.md5' | sort -n | tail -n1";
        shell.execute(cmd2.replace("${fsimageDir}", fsimageDir));
        ArrayList<String> list2 = shell.getStandardOutput();
        String fsimageFile = list2.get(list2.size() - 1);
        //拷贝fsimage文件到ssh登录的HOME目录,并加上时间戳后缀
        String userHomeDir = "/home/" + shell.username;
        long timestamp = System.currentTimeMillis();
        String fsimageFileName = new File(fsimageFile).getName() + "_" + timestamp;
        String cmd3 = "cp ${fsimageFile} ${userhome}/${fsimageFileName}";
        cmd3 = cmd3.replace("${fsimageFile}", fsimageFile).replace("${userhome}", userHomeDir).replace("${fsimageFileName}", fsimageFileName);
        shell.execute(cmd3);

        //解析fsimage成csv文件
        String cmd4 = "hdfs oiv -p Delimited -delimiter ',' -i ${userhome}/${fsimageFileName} -o ${userhome}/fsimage.csv";
        cmd4 = cmd4.replace("${userhome}", userHomeDir).replace("${fsimageFileName}", fsimageFileName);
        shell.execute(cmd4);

        // 创建fsimage表
        String cmd5 = "hive -S -e \"CREATE TABLE IF NOT EXISTS fsimage( " +
                "path string, " +
                "replication int, " +
                "modificationtime string, " +
                "accesstime string, " +
                "preferredblocksize bigint, " +
                "blockscount int, " +
                "filesize bigint, " +
                "nsquota int, " +
                "dsquota int, " +
                "permission string, " +
                "username string, " +
                "groupname string) " +
                "ROW FORMAT DELIMITED " +
                "FIELDS TERMINATED BY ',' " +
                "STORED AS INPUTFORMAT " +
                "'org.apache.hadoop.mapred.TextInputFormat' " +
                "OUTPUTFORMAT " +
                "'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' " +
                "location '/tmp/fsimage'\"";
        shell.execute(cmd5);

        // 上传fsimage.csv文件到hive表，为后面统计分析做准备
        String cmd6 = "hadoop fs -put -f ${userhome}/fsimage.csv /tmp/fsimage/";
        cmd6 = cmd6.replace("${userhome}", userHomeDir);
        shell.execute(cmd6);

        // 删除无用的数据
        String cmd7 = "rm -rf ${userhome}/fsimage*";
        cmd7 = cmd7.replace("${userhome}", userHomeDir);
        shell.execute(cmd7);

    }

    /**
     * ssh登录namenode host
     */
    private static void sshLoginNameNodeHost() throws IOException, JSchException {
        String host = PropsUtil.getProp("ssh.namenode.host");
        String user = PropsUtil.getProp("ssh.namenode.user");
        String password = PropsUtil.getProp("ssh.namenode.password");
        shell = new Shell(host, user, password);
        int code = shell.execute("ls");
        System.out.println(code);
        if (code == 0) {
            log.info("用户：{} 登录host：{}成功！！", user, host);
        } else {
            log.error("用户：{} 登录host：{}失败！！", user, host);
            System.exit(-1);
        }
    }

}
