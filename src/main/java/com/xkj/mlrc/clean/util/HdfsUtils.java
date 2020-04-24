package com.xkj.mlrc.clean.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: lijf@2345.com
 * @Date: 2018/7/10 17:35
 * @Version: 1.0
 */
public class HdfsUtils {

    private HdfsUtils() {
    }

    private static FileSystem hdfs;
    static Logger logger = Logger.getLogger(HdfsUtils.class);
    private static final String PATH_DELIMER = "/";

    static {
        //获取FileSystem类的方法有很多种，这里只写一种
        Configuration config = new Configuration();
        try {
            String hdfsUri = PropsUtil.getProp("ad.hdfs.root.uri");
            // 第一位为uri，第二位为config，第三位是登录的用户
            hdfs = FileSystem.get(new URI(hdfsUri), config);

        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }

    /**
     * 检查文件或者文件夹是否存在
     *
     * @param filename
     * @return
     */
    public static boolean checkFileExist(String filename) {
        try {
            Path f = new Path(filename);
            return hdfs.exists(f);
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return false;
    }

    /**
     * 创建文件夹
     *
     * @param dirName
     * @return
     */
    public static boolean mkdir(String dirName) {
        if (checkFileExist(dirName)) {
            return true;
        }
        try {
            Path f = new Path(dirName);
            logger.info("Create and Write :" + f.getName() + " to hdfs");
            return hdfs.mkdirs(f);
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }

        return false;
    }

    /**
     * 创建一个空文件
     *
     * @param filePath 文件的完整路径名称
     * @return
     */
    public static boolean mkfile(String filePath) {
        try {
            Path f = new Path(filePath);
            if (hdfs.exists(f)) {
                return true;
            }
            FSDataOutputStream os = hdfs.create(f, false);
            os.close();
            return true;
        } catch (IllegalArgumentException | IOException e) {
            logger.error(e.toString(), e);
        }
        return false;
    }

    /**
     * 复制文件到指定目录
     *
     * @param srcfile srcfile
     * @param desfile desfile
     * @return boolean
     * @throws IOException IOException
     */
    public static boolean hdfsCopyUtils(String srcfile, String desfile) throws IOException {
        Configuration conf = new Configuration();
        Path src = new Path(srcfile);
        Path dst = new Path(desfile);
        FileUtil.copy(src.getFileSystem(conf), src,
                dst.getFileSystem(conf), dst, false, conf);

        return true;
    }

    /**
     * 移动文件或者文件夹
     *
     * @param src 初始路径
     * @param dst 移动结束路径
     * @throws Exception
     */
    public static void movefile(String src, String dst) throws IOException {
        Path p1 = new Path(src);
        Path p2 = new Path(dst);
        hdfs.rename(p1, p2);
    }

    /**
     * 删除文件或者文件夹
     *
     * @param src
     * @throws Exception
     */
    public static void delete(String src) throws IOException {
        Path p1 = new Path(src);
        if (hdfs.isDirectory(p1)) {
            hdfs.delete(p1, true);
            logger.info("删除文件夹成功: " + src);
        } else if (hdfs.isFile(p1)) {
            hdfs.delete(p1, false);
            logger.info("删除文件成功: " + src);
        }

    }

    /**
     * 读取本地文件到HDFS系统, 保证文件格式是utf-8
     *
     * @param localFilename
     * @param hdfsPath
     * @return
     */
    public static boolean copyLocalFileToHDFS(String localFilename, String hdfsPath) {
        // 如果路径不存在就创建文件夹
        mkdir(hdfsPath);

        File file = new File(localFilename);

        // 如果hdfs上已经存在文件，那么先删除该文件
        if (HdfsUtils.checkFileExist(hdfsPath + PATH_DELIMER + file.getName())) {
            try {
                delete(hdfsPath + PATH_DELIMER + file.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        Path f = new Path(hdfsPath + PATH_DELIMER + file.getName());
        try (
                FileInputStream is = new FileInputStream(file);
                FSDataOutputStream os = hdfs.create(f, true)
        ) {
            byte[] buffer = new byte[10240000];
            int nCount = 0;

            while (true) {
                int bytesRead = is.read(buffer);
                if (bytesRead <= 0) {
                    break;
                }

                os.write(buffer, 0, bytesRead);
                nCount++;
                if (nCount % (100) == 0) {
                    logger.info(" Have move " + nCount + " blocks");
                }
            }
            logger.info(" Write content of file " + file.getName()
                    + " to hdfs file " + f.getName() + " success");
            return true;
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return false;
    }

    /**
     * 复制本地文件夹到hdfs的文件
     *
     * @param localPath
     * @param hdfsPath
     * @return
     */
    public static boolean copyLocalDirTohdfs(String localPath, String hdfsPath) {
        try {
            File root = new File(localPath);
            File[] files = root.listFiles();

            for (File file : files) {
                if (file.isFile()) {
                    copyLocalFileToHDFS(file.getPath(), hdfsPath);

                } else if (file.isDirectory()) {
                    copyLocalDirTohdfs(localPath + "/" + file.getName(), hdfsPath + "/" + file.getName());
                }
            }
            return true;
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return false;
    }


    /**
     * 从hdfs下载
     *
     * @param hdfsFilename
     * @param localPath
     * @return
     */
    public static boolean downloadFileFromHdfs(String hdfsFilename, String localPath) {

        Path f = new Path(hdfsFilename);
        File file = new File(localPath + PATH_DELIMER + f.getName());
        try (
                FSDataInputStream dis = hdfs.open(f);
                FileOutputStream os = new FileOutputStream(file);
        ) {
            byte[] buffer = new byte[1024000];
            int length = 0;
            while ((length = dis.read(buffer)) > 0) {
                os.write(buffer, 0, length);
            }
            return true;
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return false;
    }

    /**
     * HDFS 到 HDFS 的合并
     * hdfs提供了一种FileUtil.copyMerge（）的方法， 注意下面的 false 这个，如果改为true，就会删除这个目录
     *
     * @param folder 需要合并的目录
     * @param file   要合并成的文件，完整路径名称
     */
    public static void copyMerge(String folder, String file) {
        Configuration conf = new Configuration();
        Path src = new Path(folder);
        Path dst = new Path(file);

        try {
            FileUtil.copyMerge(src.getFileSystem(conf), src,
                    dst.getFileSystem(conf), dst, false, conf, null);
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
    }


    /**
     * 列出所有DataNode的名字信息
     */
    public static void listDataNodeInfo() {
        try {
            DistributedFileSystem fs = null;
            fs = (DistributedFileSystem) hdfs;
            DatanodeInfo[] dataNodeStats = fs.getDataNodeStats();
            String[] names = new String[dataNodeStats.length];
            logger.info("List of all the datanode in the HDFS cluster:");
            for (int i = 0; i < names.length; i++) {
                names[i] = dataNodeStats[i].getHostName();
                logger.info(names[i]);
            }
            logger.info(hdfs.getUri().toString());
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
    }


    public static boolean mergeDirFiles(List<FileStatus> fileList, String tarPath, String rowTerminateFlag) {

        Path tarFile = new Path(tarPath);
        try (FSDataOutputStream tarFileOutputStream = hdfs.create(tarFile, true)) {
            byte[] buffer = new byte[1024000];
            int length = 0;
            long nTotalLength = 0;
            int nCount = 0;
            boolean bfirst = true;
            for (FileStatus file : fileList) {
                if (file.getPath().equals(tarFile)) {
                    continue;
                }
                logger.info(" merging file from  " + file.getPath() + " to " + tarPath);

                if (!bfirst) {
                    //添加换行符
                    tarFileOutputStream.write(rowTerminateFlag.getBytes(), 0, rowTerminateFlag.length());
                }
                try (
                        FSDataInputStream srcFileInputStream = hdfs.open(file.getPath(), buffer.length);
                ) {
                    while ((length = srcFileInputStream.read(buffer)) > 0) {
                        nCount++;
                        tarFileOutputStream.write(buffer, 0, length);
                        nTotalLength += length;
                        if (nCount % 1000 == 0) {
                            tarFileOutputStream.flush();
                            logger.info("Have move " + (nTotalLength / 1024000) + " MB");
                        }

                    }
                }
                bfirst = false;
            }

        } catch (Exception e) {
            logger.error(e.toString(), e);
            try {
                delete(tarPath);
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            return false;
        }
        return true;
    }


    /**
     * 将一个字符串写入某个路径
     *
     * @param text 要保存的字符串
     * @param path 要保存的路径
     */
    public static void writerString(String text, String path) {

        try {
            Path f = new Path(path);
            FSDataOutputStream os = hdfs.append(f);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "utf-8"));
            writer.write(text);
            writer.close();
            os.close();

        } catch (Exception e) {
            logger.error(e.toString(), e);
        }

    }

    /**
     * 按行读取文件内容，并且防止乱码
     *
     * @param hdfsFilename
     * @return
     */
    public static List<String> readByLine(String hdfsFilename) {
        List<String> list = new ArrayList<>();
        Path f = new Path(hdfsFilename);
        try (
                FSDataInputStream dis = hdfs.open(f);
                BufferedReader bf = new BufferedReader(new InputStreamReader(dis));) {
            String line = null;
            while ((line = bf.readLine()) != null) {
                list.add(new String(line.getBytes(), "utf-8"));
            }
            return list;
        } catch (Exception e) {
            logger.error(e.toString(), e);
            return list;
        }
    }

    /**
     * 按行读取文件内容，并且防止乱码
     *
     * @param hdfsDir
     * @return
     */
    public static List<String> listFiles(String hdfsDir) {
        List<String> listFiles = new ArrayList<>();
        try {
            Path path = new Path(hdfsDir);
            if (!hdfs.exists(path)) {
                return listFiles;
            }
            FileStatus[] fileStatuses = hdfs.listStatus(path);
            for (int i = 0; i < fileStatuses.length; i++) {
                FileStatus fileStatus = fileStatuses[i];
                String fileName = fileStatus.getPath().getName();
                listFiles.add(fileName);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return listFiles;
        }
        return listFiles;
    }

    /**
     * 获取子文件或文件的最后更新时间
     *
     * @param uri 路径地址
     * @return
     */
    public static Map<String, Long> getFilesModifyTime(String uri) {
        Map<String, Long> map = new HashMap<>();
        try {
            if (hdfs.isDirectory(new Path(uri))) {
                FileStatus[] fileStatuses = hdfs.listStatus(new Path(uri));
                for (int i = 0; i < fileStatuses.length; i++) {
                    FileStatus fileStatus = fileStatuses[i];
                    String name = fileStatus.getPath().toUri().toString();
                    long modificationTime = fileStatus.getModificationTime();
                    map.put(name, modificationTime);
                }
            } else {
                Path path = new Path(uri);
                if (hdfs.exists(path)) {
                    FileStatus fileStatus = hdfs.getFileStatus(path);
                    String name = fileStatus.getPath().toUri().toString();
                    long modificationTime = fileStatus.getModificationTime();
                    map.put(name, modificationTime);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return map;
        }
        return map;
    }


    /**
     * 把路径放入回收站
     *
     * @param src 目标路径
     * @return boolean
     * @throws IOException IOException
     */
    public static boolean trashPath(String src) throws IOException {
        Path path = new Path(src);
        Trash trashTmp = new Trash(hdfs, hdfs.getConf());
        if (hdfs.exists(path)) {
            if (trashTmp.moveToTrash(path)) {
                return true;
            }
        }
        return false;
    }

}
