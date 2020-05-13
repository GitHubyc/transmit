package com.hebaibai.ctrt.transmit.config;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hebaibai.ctrt.transmit.DataType;
import com.hebaibai.ctrt.transmit.TransmitConfig;
import com.hebaibai.ctrt.transmit.util.CrtrUtils;
import com.hebaibai.ctrt.transmit.util.ext.Ext;
import com.hebaibai.ctrt.transmit.util.ext.Exts;
import com.hebaibai.ctrt.transmit.verticle.DataBaseVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author hjx
 */
@Slf4j
public class DbTypeConfig implements CrtrConfig {

    /**
     * vertx 实例
     */
    private Vertx vertx;


    /**
     * 启动时监听的端口
     */
    private int port;

    /**
     * 事件(调用数据库查询配置)
     */
    private EventBus eventBus;


    /**
     * 构造函数
     *
     * @param configFilePath
     * @throws IOException
     */
    public DbTypeConfig(Vertx vertx, String configFilePath) throws Exception {
        this.vertx = vertx;
        String fileText = CrtrUtils.getFileText(configFilePath);
        JSONObject jsonObject = JSONObject.parseObject(fileText);
        //获取系统配置
        JSONObject configJson = jsonObject.getJSONObject("config");
        this.eventBus = vertx.eventBus();
        initConfig(configJson);
    }

    /**
     * 获取 配置
     *
     * @param method
     * @param path
     * @return
     */
    @Override
    public Handler<Promise<TransmitConfig>> transmitConfig(HttpMethod method, String path) {
        JsonObject msg = new JsonObject();
        msg.put("path", path);
        msg.put("method", method.name());
        return event -> eventBus.request(
                DataBaseVerticle.FIND_CONFIG_BY_METHOD_AND_PATH,
                msg.toString(),
                (Handler<AsyncResult<Message<JsonArray>>>) result -> {
                    if (!result.succeeded()) {
                        event.fail("no config");
                        return;
                    }
                    JsonArray body = result.result().body();
                    if (body.size() != 1) {
                        event.fail("config size error");
                        return;
                    }
                    JsonObject one = body.getJsonObject(0);
                    try {
                        TransmitConfig transmitConfig = getTransmitConfig(one);
                        event.complete(transmitConfig);
                    } catch (Exception e) {
                        log.error("解析json异常: {}", e);
                        event.fail(e.getMessage());
                    }
                });
    }

    @Override
    public int getPort() {
        return port;
    }

    /**
     * 从数据库中获取 TransmitConfig
     *
     * @param transmitJson
     * @return
     * @throws IOException
     */
    private TransmitConfig getTransmitConfig(JsonObject transmitJson) throws Exception {
        TransmitConfig transmitConfig = new TransmitConfig();
        transmitConfig.setCode(transmitJson.getString("code"));
        //request 配置
        transmitConfig.setReqPath(getUrl(transmitJson.getString("path")));
        transmitConfig.setReqMethod(getHttpMethod(transmitJson.getString("method")));
        transmitConfig.setReqType(getDataType(transmitJson.getString("request_type")));
        transmitConfig.setResType(getDataType(transmitJson.getString("response_type")));
        //api 配置
        String url = transmitJson.getString("api_url");
        //没有配置url时, 认为不需要调用接口
        if (StringUtils.isNotBlank(url)) {
            transmitConfig.setApiPath(url);
            //接口调用超时时间， 默认3秒
            Integer timeout = transmitJson.getInteger("timeout", 3000);
            transmitConfig.setTimeout(timeout);
            transmitConfig.setApiMethod(getHttpMethod(transmitJson.getString("api_method")));
            transmitConfig.setApiReqType(getDataType(transmitJson.getString("api_request_type")));
            transmitConfig.setApiResType(getDataType(transmitJson.getString("api_response_type")));
            //请求转换模板文件
            transmitConfig.setApiReqFtlText(transmitJson.getString("request_ftl"));
        }
        //响应转换模板文件
        transmitConfig.setApiResFtlText(transmitJson.getString("response_ftl"));
        //插件编号
        String extCode = transmitJson.getString("ext_code");
        Class<? extends Ext> extClass = Exts.get(extCode);
        Ext ext = extClass.newInstance();
        //覆盖原有的property
        String property = transmitJson.getString("property");
        if (StringUtils.isBlank(property)) {
            property = "{}";
        }
        //将自定义配置混合进transmitJson
        JsonObject propertyJson = new JsonObject(property);
        for (String fieldName : propertyJson.fieldNames()) {
            transmitJson.put(fieldName, propertyJson.getValue(fieldName));
        }
        ext.init(this.vertx, transmitJson.getMap());
        transmitConfig.setExt(ext);
        return transmitConfig;
    }

    /**
     * 加载 配置文件 config 节点
     *
     * @param configJson
     */
    private void initConfig(JSONObject configJson) throws IOException {
        //获取系统端口配置
        this.port = configJson.getInteger("port");
        log.info("init port: {}", port);
        //插件加载
        if (!configJson.containsKey("ext")) {
            return;
        }
        JSONArray ext = configJson.getJSONArray("ext");
        for (int i = 0; i < ext.size(); i++) {
            String extClassName = ext.getString(i);
            try {
                Class.forName(extClassName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(0);
            }
        }
        for (String extCode : Exts.codes()) {
            log.info("load ext code {}", extCode);
        }
    }

    private DataType getDataType(String str) {
        DataType dataType = DataType.valueOf(str.toUpperCase());
        return dataType;
    }

    private HttpMethod getHttpMethod(String str) {
        HttpMethod httpMethod = HttpMethod.valueOf(str);
        if (httpMethod == HttpMethod.POST || httpMethod == HttpMethod.GET) {
            return httpMethod;
        }
        throw new UnsupportedOperationException("请求方式不支持: " + httpMethod);
    }

    private String getUrl(String url) {
        if (url == null) {
            return null;
        }
        String path = new File(url).getPath();
        return path;
    }
}
