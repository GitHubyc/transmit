package com.hebaibai.ctrt;

import com.alibaba.fastjson.JSONObject;
import com.hebaibai.ctrt.transmit.DataConfig;
import com.hebaibai.ctrt.transmit.config.CrtrConfig;
import com.hebaibai.ctrt.transmit.config.DbTypeConfig;
import com.hebaibai.ctrt.transmit.config.FileTypeConfig;
import com.hebaibai.ctrt.transmit.util.CrtrUtils;
import com.hebaibai.ctrt.transmit.verticle.DataBaseVerticle;
import com.hebaibai.ctrt.transmit.verticle.TransmitVerticle;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vertx.core.Context;
import io.vertx.core.Vertx;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 转发器启动器
 *
 * @author hjx
 */
public class CtrtLancher {

    private Vertx vertx = Vertx.vertx();

    private Context context = vertx.getOrCreateContext();

    private List<String> verticleIds = new ArrayList<>();

    /**
     * vert.x 部署 启动
     *
     * @param configFilePath 配置文件绝对路径
     * @throws Exception
     */
    public void start(String configFilePath) throws Exception {
        //获取数据库配置
        DataConfig dataConfig = getDataConfig(configFilePath);
        //数据库部署
        DataBaseVerticle dataBaseVerticle = new DataBaseVerticle();
        if (dataConfig != null) {
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setDriverClassName("com.mysql.jdbc.Driver");
            hikariConfig.setJdbcUrl(dataConfig.getUrl());
            hikariConfig.setUsername(dataConfig.getUsername());
            hikariConfig.setPassword(dataConfig.getPassword());
            hikariConfig.setMaximumPoolSize(Runtime.getRuntime().availableProcessors() * 2);
            hikariConfig.setPoolName("ctrt_dataSource");
            DataSource dataSource = new HikariDataSource(hikariConfig);
            dataBaseVerticle.setDataSource(dataSource);
        }
        dataBaseVerticle.init(vertx, this.context);
        vertx.deployVerticle(dataBaseVerticle, res -> {
            if (res.succeeded()) {
                String id = res.result();
                verticleIds.add(id);
            }
        });
        //接口转换部署
        TransmitVerticle transmitVerticle = new TransmitVerticle();
        CrtrConfig crtrConfig = crtrConfig(configFilePath);
        transmitVerticle.setCrtrConfig(crtrConfig);
        vertx.deployVerticle(transmitVerticle, res -> {
            if (res.succeeded()) {
                verticleIds.add(res.result());
            }
        });
    }

    private DataConfig getDataConfig(String configFilePath) throws IOException {
        String fileText = CrtrUtils.getFileText(configFilePath);
        JSONObject jsonObject = JSONObject.parseObject(fileText);
        //获取系统配置
        JSONObject configJson = jsonObject.getJSONObject("config");
        //配置日志数据库
        if (configJson.containsKey("db")) {
            DataConfig db = configJson.getObject("db", DataConfig.class);
            return db;
        } else {
            return null;
        }
    }

    public CrtrConfig crtrConfig(String configFilePath) throws Exception {
        String fileText = CrtrUtils.getFileText(configFilePath);
        JSONObject jsonObject = JSONObject.parseObject(fileText);
        //获取系统配置
        JSONObject configJson = jsonObject.getJSONObject("config");
        //使用数据库配置
        if (configJson.containsKey("dbConfig") && configJson.getBoolean("dbConfig")) {
            return new DbTypeConfig(vertx, configFilePath);
        }
        //使用配置文件配置
        else {
            return new FileTypeConfig(vertx, configFilePath);
        }
    }
}
