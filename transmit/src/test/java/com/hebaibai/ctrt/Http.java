package com.hebaibai.ctrt;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

public class Http {

    @Test
    public void name() throws InterruptedException {
        Vertx vertx = Vertx.vertx();
        //HTTP请求配置
        HttpClientOptions httpOptions = new HttpClientOptions();
        httpOptions.setSsl(true).setVerifyHost(false).setTrustAll(true); //配置启用SSL
        HttpClient httpClient = vertx.createHttpClient(httpOptions); //获取HTTPClient
        WebClient client = WebClient.wrap(httpClient);
        client.requestAbs(HttpMethod.POST, "https://api.12313123.com:19090/apigateway/api")
                .putHeader("Content-Type", "application/json")
                .sendBuffer(Buffer.buffer("{}"), result -> {
                    if (result.succeeded()) {
                        System.out.println(result.result().bodyAsString("utf-8"));
                    } else {
                        result.cause().printStackTrace();
                    }
                });
    }

    @Test
    public void url() throws MalformedURLException {
        String url = "////asd////asda//asd///////asd";
        String[] split = StringUtils.split(url, "/");
        System.out.println(StringUtils.join(split, "/"));

    }
}
