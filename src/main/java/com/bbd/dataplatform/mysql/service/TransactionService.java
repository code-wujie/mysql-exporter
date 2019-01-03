/**
 * BBD Service Inc
 * All Rights Reserved @2017
 */
package com.bbd.dataplatform.mysql.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class TransactionService {
    private static Logger logger = LoggerFactory.getLogger(TransactionService.class);

    private AtomicInteger transformThreadCount = new AtomicInteger(0);

    private boolean extractThreadHasDone = false;

    public void extractThreadDone() {
        extractThreadHasDone = true;
    }

    public boolean isExtractThreadDone() {
        return extractThreadHasDone;
    }

    public void transformThreadIncrement() {
        transformThreadCount.getAndIncrement();
    }

    public void transformThreadDecrement() {
        transformThreadCount.getAndDecrement();
    }

    public boolean isTransformThreadDone() {
        return transformThreadCount.get() == 0;
    }

    public int getTransformThreadNum() {
        return transformThreadCount.get();
    }
}