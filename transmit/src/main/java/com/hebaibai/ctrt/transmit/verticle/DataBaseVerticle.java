package com.hebaibai.ctrt.transmit.verticle;


import com.alibaba.fastjson.JSONObject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 没有配置sqlClient时，不做任何处理
 *
 * @author hjx
 */
@Slf4j
public class DataBaseVerticle extends AbstractVerticle {

    /**
     * 插入请求日志
     */
    public static String INSERT_LOG = "insert_log";

    /**
     * 更新请求日志
     */
    public static String UPDATE_LOG = "update_log";

    /**
     * 获取所有的配置项code
     */
    public static String FIND_ALL_CONFIG = "find_all_config";

    /**
     * 通过method和path查找配置
     */
    public static String FIND_CONFIG_BY_METHOD_AND_PATH = "find_config_by_method_and_path";

    private static String[] ADDRESS = {
            INSERT_LOG,
            UPDATE_LOG,
            FIND_ALL_CONFIG,
            FIND_CONFIG_BY_METHOD_AND_PATH
    };

    @Setter
    private MySQLPool mySQLPool;

    @Override
    public void start() throws Exception {
        EventBus eventBus = vertx.eventBus();
        if (mySQLPool == null) {
            for (String address : ADDRESS) {
                eventBus.consumer(address, this::log);
            }
        } else {
            eventBus.consumer(INSERT_LOG, this::insertLog);
            eventBus.consumer(UPDATE_LOG, this::updateLog);
            eventBus.consumer(FIND_ALL_CONFIG, this::findAllConfig);
            eventBus.consumer(FIND_CONFIG_BY_METHOD_AND_PATH, this::findConfigByMethodAndPath);
        }
    }

    @Override
    public void stop() throws Exception {
    }

    /**
     * 没有配置sqlClient时的处理
     *
     * @param jsonMsg
     */
    private void log(Message<String> jsonMsg) {
    }

    /**
     * 执行更新
     *
     * @param jsonMsg
     */
    public void updateLog(Message<String> jsonMsg) {
        JSONObject sqlParams = JSONObject.parseObject(jsonMsg.body());
        String sql = "UPDATE `api_log` SET `receive` = ?, `end_time` = now(), `status` = 1 WHERE `id` = ? ";
        log.debug(sql);
        mySQLPool.preparedQuery(sql, Tuple.of(
                sqlParams.getString("receive"),
                sqlParams.getString("id")
        ), event -> {
            if (event.succeeded()) {
                RowSet result = event.result();
                jsonMsg.reply(result.size());
            } else {
                jsonMsg.fail(500, event.cause().toString());
                event.cause().printStackTrace();
                log.error("update", event.cause());
            }
        });
    }

    /**
     * 执行新增
     *
     * @param jsonMsg
     */
    public void insertLog(Message<String> jsonMsg) {
        JSONObject sqlParams = JSONObject.parseObject(jsonMsg.body());
        String sql = "INSERT INTO `api_log` (`id`, `type_code`, `send_msg`, `create_time`, `status`) VALUES (?, ?, ?, now(), ?)";
        log.debug(sql);
        mySQLPool.preparedQuery(sql, Tuple.of(
                sqlParams.getString("id"),
                sqlParams.getString("type_code"),
                sqlParams.getString("send_msg"),
                0
        ), event -> {
            if (event.succeeded()) {
                RowSet result = event.result();
                jsonMsg.reply(result.size());
            } else {
                jsonMsg.fail(500, event.cause().toString());
                event.cause().printStackTrace();
                log.error("insert", event.cause());
            }
        });

    }

    /**
     * 获取所有的配置项的code
     *
     * @param jsonMsg
     */
    private void findAllConfig(Message<JsonArray> jsonMsg) {
        String sql = "select * from api_config where status = 1;";
        log.debug(sql);
        mySQLPool.query(sql, res -> {
            if (res.succeeded()) {
                RowSet result = res.result();
                JsonArray jsonArray = toJsonArray(result);
                jsonMsg.reply(jsonArray);
            } else {
                jsonMsg.fail(500, res.cause().toString());
                res.cause().printStackTrace();
                log.error("allConfigCode", res.cause());
            }
        });
    }

    /**
     * 通过method和path查找配置
     *
     * @param jsonMsg
     */
    private void findConfigByMethodAndPath(Message<String> jsonMsg) {
        String sql = "select * from api_config where method = ? and path = ? and status = 1;";
        log.debug(sql);
        JSONObject sqlParams = JSONObject.parseObject(jsonMsg.body());
        mySQLPool.preparedQuery(sql, Tuple.of(
                sqlParams.getString("method"),
                sqlParams.getString("path")
        ), res -> {
            if (res.succeeded()) {
                RowSet result = res.result();
                JsonArray jsonArray = toJsonArray(result);
                jsonMsg.reply(jsonArray);
            } else {
                jsonMsg.fail(500, res.cause().toString());
                res.cause().printStackTrace();
                log.error("allConfigCode", res.cause());
            }
        });
    }

    JsonArray toJsonArray(RowSet rowSet) {
        List<String> columnsNames = rowSet.columnsNames();
        RowIterator iterator = rowSet.iterator();
        JsonArray jsonArray = new JsonArray();
        while (iterator.hasNext()) {
            Row next = iterator.next();
            JsonObject jsonObject = new JsonObject();
            for (String columnsName : columnsNames) {
                Object value = next.getValue(columnsName);
                jsonObject.put(columnsName, value);
            }
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
