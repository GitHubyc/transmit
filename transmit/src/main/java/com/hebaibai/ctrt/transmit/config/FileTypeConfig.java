package com.hebaibai.ctrt.transmit.config;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.hebaibai.ctrt.convert.FreeMarkerFtl;
import com.hebaibai.ctrt.convert.FreeMarkerUtils;
import com.hebaibai.ctrt.convert.reader.DataReader;
import com.hebaibai.ctrt.convert.reader.JsonDataReader;
import com.hebaibai.ctrt.transmit.DataType;
import com.hebaibai.ctrt.transmit.TransmitConfig;
import com.hebaibai.ctrt.transmit.util.CrtrUtils;
import com.hebaibai.ctrt.transmit.util.ext.Ext;
import com.hebaibai.ctrt.transmit.util.ext.Exts;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @author hjx
 */
@Slf4j
public class FileTypeConfig implements CrtrConfig {

    /**
     * vertx 实例
     */
    private Vertx vertx;

    /**
     * 配置文件地址
     */
    private String configFilePath;

    /**
     * 启动时监听的端口
     */
    private int port;

    /**
     * 是否缓{
     * 1: 存模板文件
     * 2: 转发配置
     * }
     */
    private boolean cache;

    /**
     * 导入的配置
     */
    private Set<String> imports = new HashSet();

    /**
     * 请求方式 对应数据转换
     */
    private Set<TransmitConfig> transmitConfigs = new HashSet();

    /**
     * 配置文件中的 TransmitConfig Json 数据
     */
    private Map<String, JSONObject> transmitConfigMap = new HashMap();

    /**
     * 配置中的属性
     */
    private Object prop;

    /**
     * 构造函数
     *
     * @param configFilePath
     * @throws IOException
     */
    public FileTypeConfig(Vertx vertx, String configFilePath) throws Exception {
        this.configFilePath = configFilePath;
        this.vertx = vertx;
        String fileText = CrtrUtils.getFileText(configFilePath);
        JSONObject jsonObject = JSONObject.parseObject(fileText);
        //获取系统配置
        JSONObject configJson = jsonObject.getJSONObject("config");
        initConfig(configJson);
        if (cache) {
            //加载prop
            initProp(configJson);
            //加载所有的TransmitConfig配置
            importAll(jsonObject);
            //将所有的 json 配置转换为 TransmitConfig
            for (Map.Entry<String, JSONObject> entry : transmitConfigMap.entrySet()) {
                TransmitConfig transmitConfig = initTransmitConfig(entry.getKey(), entry.getValue());
                transmitConfigs.add(transmitConfig);
            }
        }
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
        return event -> {
            //开启缓存
            if (cache) {
                Iterator<TransmitConfig> iterator = transmitConfigs.iterator();
                while (iterator.hasNext()) {
                    TransmitConfig next = iterator.next();
                    if (next.getReqMethod() == method && next.getReqPath().equals(path)) {
                        event.complete(next);
                    }
                }
            }
            //不使用缓存
            else {
                //重新读取配置文件
                try {
                    String fileText = CrtrUtils.getFileText(this.configFilePath);
                    JSONObject jsonObject = JSONObject.parseObject(fileText);
                    //获取系统配置
                    JSONObject configJson = jsonObject.getJSONObject("config");
                    //重新加载prop
                    initProp(configJson);
                    //清除之前加载的配置
                    imports = new HashSet();
                    transmitConfigs = new HashSet();
                    transmitConfigMap = new HashMap<>();
                    //加载所有的配置
                    importAll(jsonObject);
                    for (Map.Entry<String, JSONObject> entry : transmitConfigMap.entrySet()) {
                        JSONObject value = entry.getValue();
                        JSONObject request = value.getJSONObject("request");
                        if (request.containsKey("path") && request.containsKey("method")) {
                            String pathStr = request.getString("path");
                            String methodStr = request.getString("method");
                            if (method == HttpMethod.valueOf(methodStr) && path.equals(pathStr)) {
                                TransmitConfig transmitConfig = initTransmitConfig(entry.getKey(), value);
                                event.complete(transmitConfig);
                                return;
                            }
                        }
                    }
                } catch (Exception e) {
                    event.fail(e);
                }
            }
            event.fail("not find config  method: " + method + " path: " + path);
        };
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
        //是否缓存模板
        if (configJson.containsKey("cache")) {
            this.cache = configJson.getBoolean("cache");
        } else {
            this.cache = true;
        }

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

    /**
     * 加载 imports
     *
     * @param configJson
     */
    private void importAll(JSONObject configJson) throws Exception {
        if (configJson.containsKey("config")) {
            JSONObject config = configJson.getJSONObject("config");
            configJson.remove("config");
            if (config.containsKey("import")) {
                JSONArray importJsons = config.getJSONArray("import");
                for (int i = 0; i < importJsons.size(); i++) {
                    String importFilePath = importJsons.getString(i);
                    imports.add(importFilePath);
                    String fileText = CrtrUtils.getFileText(importFilePath);
                    JSONObject jsonObject = JSONObject.parseObject(fileText);
                    addTransmitJson(jsonObject);
                }
            }
        }
        addTransmitJson(configJson);
    }

    /**
     * 从配置文件中加载 TransmitJson
     *
     * @param configJson
     */
    private void addTransmitJson(JSONObject configJson) {
        Set<String> keys = configJson.keySet();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String code = iterator.next();
            JSONObject transmitJson = configJson.getJSONObject(code);
            transmitConfigMap.put(code, transmitJson);
        }
    }

    /**
     * 加载 prop
     *
     * @param configJson
     */
    private void initProp(JSONObject configJson) {
        //获取prop
        if (configJson.containsKey("prop")) {
            String propJson = configJson.getString("prop");
            JsonDataReader dataReader = new JsonDataReader();
            dataReader.read(propJson);
            Object prop = dataReader.getRequestData().get(DataReader.ROOT_NAME);
            this.prop = prop;
        }
    }

    /**
     * 从配置中获取 TransmitConfig
     *
     * @param code
     * @param transmitJson
     * @return
     * @throws IOException
     */
    private TransmitConfig initTransmitConfig(String code, JSONObject transmitJson) throws Exception {
        TransmitConfig transmitConfig = new TransmitConfig();
        transmitConfig.setCode(code);
        //request 配置
        JSONObject request = transmitJson.getJSONObject("request");
        //配置中路径去重
        String path = new File(request.getString("path")).getPath();
        transmitConfig.setReqMethod(getHttpMethod(request));
        transmitConfig.setReqType(getRequestType(request));
        transmitConfig.setResType(getResponseType(request));
        transmitConfig.setReqPath(getStringConfig(path));
        JSONObject api = transmitJson.getJSONObject("api");
        String url = api.getString("url");
        //没有配置url时, 认为不需要调用接口
        if (StringUtils.isNotBlank(url)) {
            transmitConfig.setApiPath(getStringConfig(url));
            //接口调用超时时间， 默认3秒
            int timeout = 3000;
            if (api.containsKey("timeout")) {
                timeout = api.getIntValue("timeout");
            }
            transmitConfig.setTimeout(timeout);
            transmitConfig.setApiMethod(getHttpMethod(api));
            transmitConfig.setApiReqType(getRequestType(api));
            transmitConfig.setApiResType(getResponseType(api));
            //请求转换模板文件
            String requestFtlPath = getStringConfig(api.getString("request-ftl"));
            transmitConfig.setApiReqFtlText(CrtrUtils.getFileText(requestFtlPath));
        }
        //响应转换模板文件
        String responseFtlPath = getStringConfig(api.getString("response-ftl"));
        transmitConfig.setApiResFtlText(CrtrUtils.getFileText(responseFtlPath));
        //插件编号
        String extCode = api.getString("extCode");
        Class<? extends Ext> extClass = Exts.get(extCode);
        Ext ext = extClass.newInstance();
        ext.init(this.vertx, transmitJson);
        transmitConfig.setExt(ext);
        return transmitConfig;
    }

    /**
     * 将prop转换进字符串配置中，使用freemarker
     * <p>
     * 转换的节点：
     * config.import
     * request.path
     * api.url
     * api.request-ftl
     * api.response-ftl
     *
     * @param value
     * @return
     * @throws Exception
     */
    private String getStringConfig(String value) throws Exception {
        Object prop = this.prop;
        if (prop == null) {
            return value;
        }
        if (StringUtils.isBlank(value)) {
            return "";
        }
        String name = "[" + value + "]";
        String format = FreeMarkerUtils.format(prop, new FreeMarkerFtl() {{
            setTemplateText(value);
            setTemplateName(name);
        }});
        return format;
    }


    /**
     * 获取配置 HttpMethod
     *
     * @param request
     * @return
     */
    private HttpMethod getHttpMethod(JSONObject request) {
        String method = request.getString("method");
        HttpMethod httpMethod = HttpMethod.valueOf(method.toUpperCase());
        if (httpMethod == HttpMethod.POST || httpMethod == HttpMethod.GET) {
            return httpMethod;
        }
        throw new UnsupportedOperationException("请求方式不支持: " + method);
    }

    /**
     * 获取配置 DataType
     *
     * @param request
     * @return
     */
    private DataType getRequestType(JSONObject request) {
        String requestType = request.getString("request-type");
        DataType dataType = DataType.valueOf(requestType.toUpperCase());
        return dataType;
    }

    /**
     * 获取配置 DataType
     *
     * @param request
     * @return
     */
    private DataType getResponseType(JSONObject request) {
        String responseType = request.getString("response-type");
        DataType dataType = DataType.valueOf(responseType.toUpperCase());
        return dataType;
    }


    @Override
    public int getPort() {
        return port;
    }

}
