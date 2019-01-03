package com.bbd.dataplatform.mysql.service;

import com.bbd.dataplatform.mysql.util.CsvUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class TransformService {

    private static Logger logger = LoggerFactory.getLogger(TransformService.class);

    @Autowired
    private QueueService queueService;

    @Autowired
    private ConfigService configService;

    @Autowired
    private TransactionService transactionService;

    private ObjectMapper om = new ObjectMapper();

    public void run() {
        transactionService.transformThreadIncrement();
        logger.info("transform thread start, remaining thread count: " + transactionService.getTransformThreadNum());
        try {
            while (true) {
                List<Map<String, Object>> extractList = queueService.getExtract();
                if (extractList == null || extractList.isEmpty()) {
                    if (transactionService.isExtractThreadDone()) {
                        break;
                    }
                    continue;
                }
                List<String> loadList = process(extractList);

                queueService.pushLoad(loadList);
            }
        } finally {
            transactionService.transformThreadDecrement();
            logger.info("transform thread done, remaining thread count: " + transactionService.getTransformThreadNum());
        }
    }

    public List<String> process(List<Map<String, Object>> extractList) {
        List<String> loadList = new ArrayList<>();
        for (Map<String, Object> extractData : extractList) {
            if ("csv".equals(configService.getFormat())) {
                loadList.add(CsvUtil.mapToCSVLine(extractData));
            } else {
                try {
                    loadList.add(om.writeValueAsString(extractData));
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                    continue;
                }
            }
        }
        return loadList;
    }
}
