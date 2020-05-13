package com.hebaibai.ctrt.transmit;

import io.vertx.core.json.JsonObject;
import lombok.Data;

/**
 * 数据库配置
 *
 * @author hjx
 */
@Data
public class DataConfig {

    private String host;
    private int port;
    private String username;
    private String password;
    private String database;
    private String charset = "utf-8";
    private Integer maxPoolSize = Runtime.getRuntime().availableProcessors() * 2;
    private Integer queryTimeout = 5000;
    private Integer connectTimeout = 10000;

    public JsonObject getJson() {
        return new JsonObject()
                .put("host", host)
                .put("port", port)
                .put("username", username)
                .put("password", password)
                .put("database", database)
                .put("charset", charset)
                .put("maxPoolSize", maxPoolSize)
                .put("queryTimeout", queryTimeout)
                .put("connectTimeout", connectTimeout);
    }

}
