package com.hebaibai.ctrt.transmit.verticle;

import com.hebaibai.ctrt.convert.reader.DataReader;
import com.hebaibai.ctrt.transmit.RouterVo;
import com.hebaibai.ctrt.transmit.TransmitConfig;
import com.hebaibai.ctrt.transmit.config.CrtrConfig;
import com.hebaibai.ctrt.transmit.util.Convert;
import com.hebaibai.ctrt.transmit.util.CrtrUtils;
import com.hebaibai.ctrt.transmit.util.Param;
import com.hebaibai.ctrt.transmit.util.Request;
import com.hebaibai.ctrt.transmit.util.ext.Ext;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.hebaibai.ctrt.transmit.util.CrtrUtils.CHARSET_NAME;

/**
 * 请求转发Verticle
 *
 * @author hjx
 */
@Slf4j
public class TransmitVerticle extends AbstractVerticle {

    /**
     * 启动配置
     */
    @Setter
    private CrtrConfig crtrConfig;

    /**
     * 接收请求
     */
    private HttpServer httpServer;

    /**
     * 执行http请求
     */
    private WebClient webClient;

    /**
     * 事件总线, 用于保存请求记录
     */
    private EventBus eventBus;

    /**
     * 执行ext中的阻塞方法
     */
    private WorkerExecutor extWorkerExecutor;

    /**
     * extWorkerExecutor线程池大小, 默认核心数量*2
     */
    private static int EXT_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    /**
     * extWorkerExecutor执行阻塞方法的超时时间 默认10秒
     */
    private static int EXT_WORKER_MAX_EXECUTE_TIME = 10;

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
        httpServer = vertx.createHttpServer();
        //适配https
        HttpClientOptions httpOptions = new HttpClientOptions();
        httpOptions.setSsl(true).setVerifyHost(false).setTrustAll(true);
        HttpClient httpClient = vertx.createHttpClient(httpOptions);
        this.webClient = WebClient.wrap(httpClient);
        extWorkerExecutor = vertx.createSharedWorkerExecutor(
                "ext-api-worker-executor",
                EXT_WORKER_POOL_SIZE,
                EXT_WORKER_MAX_EXECUTE_TIME,
                TimeUnit.SECONDS
        );
    }

    @Override
    public void start() {
        Router router = Router.router(vertx);
        //接收数据
        router.route().handler(this::requestBody);
        //根据请求,查找配置
        router.route().handler(this::requestConfig);
        //处理数据 或者 返回页面并结束
        router.route().handler(this::processData);
        //请求数据
        router.route().handler(this::request);
        //转换响应数据,并发返回
        router.route().handler(this::convertAndReturn);
        //开启路由
        httpServer.requestHandler(router).listen(crtrConfig.getPort());
        //事件总线
        eventBus = vertx.eventBus();
    }

    @Override
    public void stop() {
    }

    /**
     * 获取转换所需要的数据
     *
     * @param routingContext
     */
    private void requestBody(RoutingContext routingContext) {
        RouterVo routerVo = new RouterVo();
        HttpServerRequest request = routingContext.request();
        routerVo.setParams(request.params());
        routingContext.request().bodyHandler(event -> {
            String requestBody = event.toString(CHARSET_NAME);
            routerVo.setUuid(UUID.randomUUID().toString());
            routerVo.setBody(requestBody);
            log.info("request {} befor:\n{}", routerVo.getUuid(), routerVo.getBody());
            routingContext.put(RouterVo.class.getName(), routerVo);
            routingContext.next();
        });
    }

    /**
     * 根据请求获取 请求配置
     *
     * @param routingContext
     */
    private void requestConfig(RoutingContext routingContext) {
        RouterVo routerVo = routingContext.get(RouterVo.class.getName());
        HttpServerRequest request = routingContext.request();
        HttpMethod method = request.method();
        routerVo.setMethod(method);
        //去除重复的'/'符号
        String path = getPath(request.path());
        routerVo.setPath(path);
        Handler<Promise<TransmitConfig>> handler = crtrConfig.transmitConfig(method, path);
        extWorkerExecutor.executeBlocking(handler, event -> {
            if (!event.succeeded()) {
                routingContext.response().end(error(event.cause()), CHARSET_NAME);
                return;
            }
            TransmitConfig transmitConfig = event.result();
            //接受请求的参数
            Param param = CrtrUtils.param(method, transmitConfig.getReqType());
            if (param == null) {
                routingContext.response().end(error("not find param util"), CHARSET_NAME);
                return;
            }
            Map<String, Object> requestMap = null;
            try {
                requestMap = param.params(routerVo);
            } catch (Exception e) {
                routingContext.response().end(error("request error: " + e.getMessage()), CHARSET_NAME);
                return;
            }
            log.info("request {} requestMap:\n{}", routerVo.getUuid(), requestMap);
            routerVo.setRequestMap(requestMap);
            routerVo.setTransmitConfig(transmitConfig);
            routerVo.setTypeCode(transmitConfig.getCode());
            //保存请求记录
            eventBus.send(DataBaseVerticle.INSERT_LOG, routerVo.getInsertJsonStr());
            routingContext.put(RouterVo.class.getName(), routerVo);
            routingContext.next();
        });
    }

    /**
     * 转换请求数据
     *
     * @param routingContext
     */
    private void processData(RoutingContext routingContext) {
        RouterVo routerVo = routingContext.get(RouterVo.class.getName());
        TransmitConfig transmitConfig = routerVo.getTransmitConfig();
        //没有配置api.url, 直接跳过
        if (StringUtils.isBlank(transmitConfig.getApiPath())) {
            routingContext.next();
            return;
        }
        Convert convert = CrtrUtils.convert(transmitConfig.getReqType(), transmitConfig.getApiReqType());
        if (convert == null) {
            routingContext.response().end(error("not find convert util"), CHARSET_NAME);
            return;
        }
        Ext ext = transmitConfig.getExt();
        //经过插件处理后的,原始请求中的参数
        Handler<Promise<Map<String, Object>>> handler = ext.outRequestBodyMap(routerVo.getBody(), routerVo.getRequestMap());
        extWorkerExecutor.executeBlocking(handler, event -> {
            if (!event.succeeded()) {
                routingContext.response().end(error(event.cause()), CHARSET_NAME);
                return;
            }
            Map<String, Object> requestMap = event.result();
            //获取转换模板并转换数据
            String apiReqFtlText = transmitConfig.getApiReqFtlText();
            try {
                String value = convert.convert(requestMap, apiReqFtlText, transmitConfig.getCode() + "-REQ");
                routerVo.setBody(value);
                log.info("request {} after:\n{}", routerVo.getUuid(), value);
            } catch (Exception e) {
                log.error("request " + routerVo.getUuid() + " error", e);
                routingContext.response().end(error(e), CHARSET_NAME);
                return;
            }
            //更新body
            routingContext.put(RouterVo.class.getName(), routerVo);
            routingContext.next();

        });

    }

    /**
     * 发送请求
     *
     * @param routingContext
     */
    private void request(RoutingContext routingContext) {
        RouterVo routerVo = routingContext.get(RouterVo.class.getName());
        TransmitConfig transmitConfig = routerVo.getTransmitConfig();
        String value = routerVo.getBody();
        Ext ext = transmitConfig.getExt();
        try {
            //请求相应结果处理器
            Handler<AsyncResult<String>> handler = result -> {
                if (!result.succeeded()) {
                    routingContext.response().end(error(result.cause()), CHARSET_NAME);
                    return;
                } else {
                    String body = result.result();
                    log.info("response {} befor:\n{}", routerVo.getUuid(), body);
                    if (StringUtils.isBlank(body)) {
                        log.error("request " + routerVo.getUuid() + " error no response body");
                        routingContext.response().end(error("no response body"), CHARSET_NAME);
                    } else {
                        //更新body中的值, 设置为接口返回值
                        routerVo.setBody(body);
                        //保存接口返回参数
                        eventBus.send(DataBaseVerticle.UPDATE_LOG, routerVo.getUpdateJsonStr());
                        routingContext.next();
                    }
                }
            };
            Handler<Promise<String>> apiResult = ext.getApiResult(value);
            if (apiResult != null) {
                extWorkerExecutor.executeBlocking(apiResult, handler);
            } else {
                //没有配置api.url, 直接跳过
                if (StringUtils.isBlank(transmitConfig.getApiPath())) {
                    routingContext.next();
                    return;
                }
                //转发数据
                Request request = CrtrUtils.request(transmitConfig.getApiMethod(), transmitConfig.getApiReqType());
                if (request == null) {
                    routingContext.response().end(error("not find request util"), CHARSET_NAME);
                    return;
                }
                request.request(webClient, transmitConfig, value, handler);
            }
        } catch (Exception e) {
            log.error("request " + routerVo.getUuid() + " error", e);
            routingContext.response().end(error(e), CHARSET_NAME);
            return;
        }
    }

    /**
     * 转换响应数据,并发返回
     *
     * @param routingContext
     */
    private void convertAndReturn(RoutingContext routingContext) {
        RouterVo routerVo = routingContext.get(RouterVo.class.getName());
        TransmitConfig transmitConfig = routerVo.getTransmitConfig();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("REQUEST", routerVo.getRequestMap().get(DataReader.ROOT_NAME));
        //配置了api.url
        if (StringUtils.isNotBlank(transmitConfig.getApiPath())) {
            // 以POST方式, 和配置中的接口返回类型中获取参数
            Param param = CrtrUtils.param(HttpMethod.POST, transmitConfig.getApiResType());
            if (param == null) {
                routingContext.response().end(error("not find param util"), CHARSET_NAME);
                return;
            }
            //api响应结果
            Map<String, Object> responseMap = null;
            try {
                responseMap = param.params(routerVo);
            } catch (Exception e) {
                routingContext.response().end(error("response error: " + e.getMessage()), CHARSET_NAME);
                return;
            }
            log.info("request {} responseMap:\n{}", routerVo.getUuid(), responseMap);
            resultMap.put(DataReader.ROOT_NAME, responseMap.get(DataReader.ROOT_NAME));
        }
        //经过插件处理后的,api的响应结果
        Ext ext = transmitConfig.getExt();
        Handler<Promise<Map<String, Object>>> handler = ext.apiResponseBodyMap(routerVo.getBody(), resultMap);
        extWorkerExecutor.executeBlocking(handler, event -> {
            if (!event.succeeded()) {
                routingContext.response().end(error(event.cause()), CHARSET_NAME);
                return;
            }
            Convert convert = CrtrUtils.convert(transmitConfig.getApiResType(), transmitConfig.getResType());
            if (convert == null) {
                routingContext.response().end(error("not find convert util"), CHARSET_NAME);
                return;
            }
            Map<String, Object> result = event.result();
            //获取转换模板并转换数据
            String apiResFtlText = transmitConfig.getApiResFtlText();
            log.debug("response {} ftl:\n{}", routerVo.getUuid(), apiResFtlText);
            try {
                String value = convert.convert(result, apiResFtlText, transmitConfig.getCode() + "-RES");
                log.info("response {} after:\n{}", routerVo.getUuid(), value);
                //返回响应结果
                String ContentType = transmitConfig.getResType().val();
                routingContext.response().putHeader("Content-Type", ContentType);
                routingContext.response().end(value, CHARSET_NAME);
            } catch (Exception e) {
                log.error("request " + routerVo.getUuid() + " error", e);
                routingContext.response().end(error(e), CHARSET_NAME);
                return;
            }
        });
    }

    /**
     * 返回异常信息
     *
     * @param msg
     * @return
     */
    private String error(String msg) {
        return error(new RuntimeException(msg));
    }

    private String error(Throwable throwable) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("error_code", "0");
        if (throwable == null) {
            jsonObject.put("error_msg", "Internal Server Error");
        } else {
            jsonObject.put("error_msg", throwable.getMessage());
        }
        return jsonObject.toString();
    }


    /**
     * 去除路径中重复的 /
     *
     * @param path
     * @return
     */
    private String getPath(String path) {
        String[] split = StringUtils.split(path, "/");
        return "/" + StringUtils.join(split, "/");
    }
}
