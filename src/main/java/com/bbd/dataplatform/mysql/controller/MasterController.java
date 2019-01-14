package com.bbd.dataplatform.mysql.controller;

import com.bbd.dataplatform.mysql.service.ConfigService;
import com.bbd.dataplatform.mysql.service.ExtractService;
import com.bbd.dataplatform.mysql.service.LoadService;
import com.bbd.dataplatform.mysql.service.TransformService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.*;

@Service
public class MasterController {

    private static Logger logger = LoggerFactory.getLogger(MasterController.class);

    @Autowired
    private ConfigService configService;

    @Autowired
    private ExtractService extractService;

    @Autowired
    private TransformService transformService;

    @Autowired
    private LoadService loadService;

    public void start() {
        ExecutorService executor = new ThreadPoolExecutor(configService.getExecutorNum() + 5, configService.getExecutorNum() + 5, 5, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

        executor.execute(() -> extractService.run());
        for (int i = 0; i < configService.getExecutorNum(); i++) {
            executor.execute(() -> transformService.run());
        }
        executor.execute(() -> loadService.run());

        executor.shutdown();

        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("process interrupted, exit program: ", e);
        }

        logger.info("mysql export task complete, exit program");
        System.exit(1);
    }
}
