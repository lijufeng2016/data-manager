package com.xkj.mlrc.clean.domain;

import org.kohsuke.args4j.Option;

public class ParamOption {

    @Option(name="-targetPath", usage="指定的要删的目标路径")
    public String targetPath;
    @Option(name="-avoidPath", usage="要避开的路径，不扫描的路径,逗号隔开")
    public String avoidPath;
    @Option(name="-avoidSuffix", usage="要避开的包含后缀的文件")
    public String avoidSuffix;
    @Option(name="-avoidPrefix", usage="要避开的包含前缀的文件")
    public String avoidPrefix;
    @Option(name="-avoidDbs", usage="要避免删除的数据库，包含库下所有的表分区，逗号隔开")
    public String avoidDb;
    @Option(name="-avoidTbls", usage="用要避免删除的表，包含表下所有的分区，逗号隔开")
    public String avoidTbls;
    @Option(name="-avoidTbls-file", usage="用要避免删除的表，用hdfs文件存放，必须是“库.表名”的形式，包含表下所有的分区")
    public String avoidTblsFile;
    @Option(name="-expire", usage="过期的数据",required = true)
    public Integer expire;
    @Option(name="-hdfsroot", usage="hdfs根路径,默认hdfs://cluster")
    public String hdfsroot = "hdfs://cluster";

}
