package com.bbd.dataplatform.mysql.service;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class ConfigService {

    private static Logger logger = LoggerFactory.getLogger(ConfigService.class);
    private static String mysqlUrl;
    private static String mysqlUsername;
    private static String mysqlPassword;
    private static String tableName;
    private static String sortColumn;
    private static Integer blockSize;
    private static List<String> fieldList;
    private static String filter;
    private static Integer limit;
    private static Integer executorNum;
    private static String format;
    private static String filePath;
    @Autowired
    private ApplicationArguments applicationArguments;

    private static Option getOption(String longOpt, boolean hasArg, String description, boolean isRequired) {
        Option option = new Option(longOpt, hasArg, description);
        option.setRequired(isRequired);

        return option;
    }

    @PostConstruct
    public void init() {
        String[] args = applicationArguments.getSourceArgs();

        Options options = new Options();
        options.addOption(getOption("help", false, "帮助", false));
        options.addOption(getOption("mysqlUrl", true, "mysql链接url", true));
        options.addOption(getOption("mysqlUsername", true, "mysql用户名", true));
        options.addOption(getOption("mysqlPassword", true, "mysql密码", true));
        options.addOption(getOption("tableName", true, "导出表名", true));
        options.addOption(getOption("sortColumn", true, "排序列名", true));
        options.addOption(getOption("blockSize", true, "分块大小", false));
        options.addOption(getOption("fieldList", true, "导出字段名", false));
        options.addOption(getOption("filter", true, "过滤条件", false));
        options.addOption(getOption("limit", true, "数据量", false));
        options.addOption(getOption("executorNum", true, "线程数", false));
        options.addOption(getOption("format", true, "格式", false));
        options.addOption(getOption("filePath", true, "输出文件路径", false));

        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine cmd = null;

        try {
            cmd = (new PosixParser()).parse(options, args);
        } catch (Exception e) {
            hf.printHelp("MysqlExporter", options, true);
            System.exit(1);
        }

        if (cmd.hasOption("help")) {
            hf.printHelp("MysqlExporter", options, true);
            System.exit(1);
        }

        mysqlUrl = cmd.getOptionValue("mysqlUrl").trim();
        logger.info("mysql url: " + mysqlUrl);
        mysqlUsername = cmd.getOptionValue("mysqlUsername").trim();
        logger.info("mysql username: " + mysqlUsername);
        mysqlPassword = cmd.getOptionValue("mysqlPassword").trim();
        logger.info("mysql password: " + mysqlPassword);
        tableName = cmd.getOptionValue("tableName").trim();
        logger.info("export table name: " + tableName);
        sortColumn = cmd.getOptionValue("sortColumn").trim();
        logger.info("export sort column: " + sortColumn);

        if (cmd.hasOption("blockSize")) {
            blockSize = Integer.valueOf(cmd.getOptionValue("blockSize").trim());
        } else {
            blockSize = 100;
        }
        logger.info("export block size: " + blockSize);


        if (cmd.hasOption("fieldList")) {
            fieldList = Arrays.asList(cmd.getOptionValue("fieldList").trim().split(","));
        } else {
            fieldList = new ArrayList<>();
        }
        if (fieldList.isEmpty()) {
            logger.info("export field list: all");
        } else {
            logger.info("export field list: " + String.join(",", fieldList));
        }

        if (cmd.hasOption("filter")) {
            filter = cmd.getOptionValue("filter").trim();
        } else {
            filter = "";
        }
        logger.info("export filter: " + filter);

        if (cmd.hasOption("limit")) {
            limit = Integer.valueOf(cmd.getOptionValue("limit").trim());
        } else {
            limit = 0;
        }
        logger.info("export limit: " + limit);

        if (cmd.hasOption("executorNum")) {
            executorNum = Integer.valueOf(cmd.getOptionValue("executorNum").trim());
        } else {
            executorNum = 100;
        }
        logger.info("export executor number: " + executorNum);

        if (cmd.hasOption("format")) {
            format = cmd.getOptionValue("format").trim();
        } else {
            format = "json";
        }
        logger.info("export format: " + format);

        if (cmd.hasOption("filePath")) {
            filePath = cmd.getOptionValue("filePath").trim();
        } else {
            filePath = tableName + "." + format;
        }
        logger.info("export output file path: " + filePath);

    }

    public String getMysqlUrl() {
        return mysqlUrl;
    }

    public String getMysqlUsername() {
        return mysqlUsername;
    }

    public String getMysqlPassword() {
        return mysqlPassword;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSortColumn() {
        return sortColumn;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public List<String> getFieldList() {
        return fieldList;
    }

    public String getFilter() {
        return filter;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getExecutorNum() {
        return executorNum;
    }

    public String getFormat() {
        return format;
    }

    public String getFilePath() {
        return filePath;
    }
}
