package com.bbd.dataplatform.mysql.service;

import com.bbd.dataplatform.mysql.repository.MysqlRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


@Service
public class ExtractService {

    private static Logger logger = LoggerFactory.getLogger(ExtractService.class);

    @Autowired
    private QueueService queueService;

    @Autowired
    private ConfigService configService;

    @Autowired
    private MysqlRepository mysqlRepository;

    @Autowired
    private TransactionService transactionServic;

    private static String buildStartSql(String tableName, String sortField) {
        return "SELECT " + sortField +
                " FROM " + tableName +
                " ORDER BY " + sortField + " ASC " +
                " LIMIT 1";
    }

    private static String buildQuerySql(String tableName, String sortField, String startField, List<String> fieldList, String filter, int blockSize) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("SELECT");
        if (fieldList.isEmpty()) {
            stringBuilder.append(" *");
        } else {
            stringBuilder.append(" " + String.join(",", fieldList));
        }
        stringBuilder.append(" FROM ").append(tableName).
                append(" WHERE ").append(sortField).append(" >= '").append(startField).append("'");

        if (filter != null && !filter.isEmpty()) {
            stringBuilder.append(" AND ").append(filter);
        }

        stringBuilder.append(" ORDER BY ").append(sortField).append(" ASC ").append(" LIMIT ").append(blockSize);
        logger.info(stringBuilder.toString());
        return stringBuilder.toString();
    }

    public void run() {

        Connection connection = mysqlRepository.getConnection();

        String startFieldValue = "";
        Integer totalCount = 0;
        try {
            Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//            ResultSet resultSet = statement.executeQuery(buildStartSql(configService.getTableName(), configService.getSortColumn()));
//            if (resultSet.next()) {
//                startFieldValue = resultSet.getObject(configService.getSortColumn()).toString();
//            } else {
//                logger.info("table [" + configService.getTableName() + "] no data, exit program");
//                System.exit(1);
//            }
//
//            if (startFieldValue == null || startFieldValue.isEmpty()) {
//                logger.info("table [" + configService.getTableName() + "] no data, exit program");
//            }

            while (!transactionServic.isExtractThreadDone()) {
                ResultSet resultSet = statement.executeQuery(buildQuerySql(configService.getTableName(),
                        configService.getSortColumn(), startFieldValue, configService.getFieldList(),
                        configService.getFilter(), configService.getBlockSize()));
                List<Map<String, Object>> results = new ArrayList<>();

                ResultSetMetaData rsmd = resultSet.getMetaData();
                int colCount = rsmd.getColumnCount();
                List<String> colNameList = new ArrayList<>();
                for (int i = 0; i < colCount; i++) {
                    colNameList.add(rsmd.getColumnName(i + 1));
                }
                while (resultSet.next()) {
                    Map map = new LinkedHashMap<String, Object>();
                    for (String colName : colNameList) {
                        Object value = resultSet.getObject(colName);
                        if (colName.equals(configService.getSortColumn())) {
                            startFieldValue = value.toString();
                        }
                        map.put(colName, value);
                    }
                    if (!map.isEmpty()) {
                        results.add(map);
                    }
                    totalCount = totalCount + 1;
                    if (configService.getLimit() != 0 && totalCount >= configService.getLimit()) {
                        break;
                    }
                }
                if (!results.isEmpty()) {
                    queueService.pushExtract(results);
                }
                resultSet.previous();
                if ((configService.getLimit() != 0 && totalCount >= configService.getLimit()) ||
                        resultSet.getRow() < configService.getBlockSize()) {
                    break;
                }
            }
        } catch (SQLException e) {
            logger.error("extract error: ", e);
        }

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
            mysqlRepository.discardConnection(connection);
        }
        transactionServic.extractThreadDone();
        logger.info("Extract data done");
    }
}
