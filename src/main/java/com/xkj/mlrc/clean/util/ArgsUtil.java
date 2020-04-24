package com.xkj.mlrc.clean.util;

import com.xkj.mlrc.clean.domain.ParamOption;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: lijf@2345.com
 * @Date: 2018/8/22 18:33
 * @Version: 1.0
 */
public class ArgsUtil {
    private static Logger logger = LoggerFactory.getLogger(ArgsUtil.class);
    static CmdLineParser parser;
    private ArgsUtil() {}

    /**
     * 解析参数
     * @param args
     * @return
     */
    public static ParamOption getOption(String[] args){
        //开始解析命令参数
        ParamOption option = new ParamOption();
        parser = new CmdLineParser(option);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            logger.error(e.toString(),e);
        }

        return option;
    }

    /**
     * 参数说明
     */
    public static void showHelp(){
        parser.printUsage(System.out);
    }
}
