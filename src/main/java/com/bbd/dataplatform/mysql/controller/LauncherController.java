/**
 * BBD Service Inc
 * All Rights Reserved @2017
 */
package com.bbd.dataplatform.mysql.controller;

import com.bbd.dataplatform.mysql.repository.MysqlRepository;
import com.bbd.dataplatform.mysql.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;

import javax.annotation.PostConstruct;

/**
 * 启动器
 *
 * @author lvv
 * @version $Id: LauncherController.java, v0.1 ${DATA} 下午6:24 lvv Exp $$
 */
@Controller
public class LauncherController {

    @Autowired
    private ConfigService configService;

    @Autowired
    private MysqlRepository mysqlRepository;

    @Autowired
    private MasterController masterController;

    @PostConstruct
    public void start() {
        mysqlRepository.init(configService.getMysqlUrl(), configService.getMysqlUsername(), configService.getMysqlPassword());
        masterController.start();
    }
}
