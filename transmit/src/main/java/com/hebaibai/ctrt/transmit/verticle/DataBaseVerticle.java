package com.hebaibai.ctrt.transmit.verticle;


import com.alibaba.fastjson.JSONObject;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.dbutils.QueryRunner;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
    private DataSource dataSource;

    @Override
    public void start() throws Exception {
        EventBus eventBus = vertx.eventBus();
        if (dataSource == null) {
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
        try {
            int update = new QueryRunner(dataSource).update(
                    sql,
                    sqlParams.getString("receive"),
                    sqlParams.getString("id")
            );
            jsonMsg.reply(update);
        } catch (SQLException e) {
            log.error("update", e);
            jsonMsg.fail(500, e.getMessage());
        }
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
        try {
            int update = new QueryRunner(dataSource).update(
                    sql,
                    sqlParams.getString("id"),
                    sqlParams.getString("type_code"),
                    sqlParams.getString("send_msg"),
                    0
            );
            jsonMsg.reply(update);
        } catch (SQLException e) {
            log.error("insert", e);
            jsonMsg.fail(500, e.getMessage());
        }
    }

    /**
     * 获取所有的配置项的code
     *
     * @param jsonMsg
     */
    private void findAllConfig(Message<JsonArray> jsonMsg) {
        String sql = "select * from api_config where status = 1;";
        log.debug(sql);
        try {
            JsonArray jsonArray = new QueryRunner(dataSource).query(sql, resultSet -> toJsonArray(resultSet));
            jsonMsg.reply(jsonArray);
        } catch (SQLException e) {
            log.error("allConfigCode", e);
            jsonMsg.fail(500, e.getMessage());
        }
    }

    /**
     * 通过method和path查找配置
     *
     * @param jsonMsg
     */
    private void findConfigByMethodAndPath(Message<String> jsonMsg) {
        String sql = "select * from api_config where method = ? and path = ? and status = 1;";
        JSONObject sqlParams = JSONObject.parseObject(jsonMsg.body());
        log.debug(sql);
        try {
            JsonArray jsonArray = new QueryRunner(dataSource).query(
                    sql,
                    resultSet -> toJsonArray(resultSet),
                    sqlParams.getString("method"),
                    sqlParams.getString("path")
            );
            jsonMsg.reply(jsonArray);
        } catch (SQLException e) {
            log.error("findConfigByMethodAndPath", e);
            jsonMsg.fail(500, e.getMessage());
        }
    }

    JsonArray toJsonArray(ResultSet resultSet) throws SQLException {
        JsonArray jsonArray = new JsonArray();
        ResultSetMetaData metaData = resultSet.getMetaData();
        while (resultSet.next()) {
            JsonObject jsonObject = new JsonObject();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object object = resultSet.getObject(columnName);
                jsonObject.put(columnName, object);
            }
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
