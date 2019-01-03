package com.bbd.dataplatform.mysql.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Service
public class QueueService {
    private static Logger logger = LoggerFactory.getLogger(QueueService.class);
    private BlockingQueue<List> extractQueue;
    private BlockingQueue<List> loadQueue;

    @PostConstruct
    public void init() {
        extractQueue = new LinkedBlockingQueue<>(100);
        loadQueue = new LinkedBlockingQueue<>(100);
        logger.info("queue component initialization is completed");
    }

    public List getExtract() {
        try {
            return extractQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean pushExtract(List e) {
        return extractQueue.offer(e);
    }

    public Integer sizeExtract() {
        return extractQueue.size();
    }

    public List getLoad() {
        try {
            return loadQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return null;
        }
    }

    public boolean pushLoad(List e) {
        return loadQueue.offer(e);
    }

    public Integer sizeLoad() {
        return loadQueue.size();
    }
}
