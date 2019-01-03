package com.bbd.dataplatform.mysql.service;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

@Service
public class LoadService {

    private static Logger logger = LoggerFactory.getLogger(LoadService.class);

    @Autowired
    private QueueService queueService;

    @Autowired
    private ConfigService configService;

    @Autowired
    private TransactionService transactionService;

    private FileOutputStream fileOutputStream;

    @PostConstruct
    public void init() {
        try {
            fileOutputStream = new FileOutputStream(new File(configService.getFilePath()), true);
        } catch (FileNotFoundException e) {
            logger.error("write output file error, exit program. ", e);
            System.exit(1);
        }
    }

    public void run() {
        while (true) {
            List<String> loadList = queueService.getLoad();
            if (loadList == null || loadList.isEmpty()) {
                if (transactionService.isTransformThreadDone()) {
                    break;
                }
                continue;
            }
            writeFile(loadList);
        }

        logger.info("load thread done");
    }

    private void writeFile(List<String> loadList) {
        for (String loadString: loadList) {
            try {
                IOUtils.write(loadString + "\n", fileOutputStream, "utf-8");
            } catch (IOException e) {
                e.printStackTrace();
                logger.error("write line: "+ loadString + ", error: ", e);
            }
        }
    }
}
