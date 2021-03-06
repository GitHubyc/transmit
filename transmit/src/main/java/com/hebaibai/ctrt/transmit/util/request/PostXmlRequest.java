package com.hebaibai.ctrt.transmit.util.request;

import com.hebaibai.ctrt.transmit.DataType;
import com.hebaibai.ctrt.transmit.TransmitConfig;
import com.hebaibai.ctrt.transmit.util.CrtrUtils;
import com.hebaibai.ctrt.transmit.util.Request;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.WebClient;

public class PostXmlRequest implements Request {

    @Override
    public boolean support(HttpMethod method, DataType dataType) {
        return method == HttpMethod.POST && dataType == DataType.XML;
    }

    /**
     * @param webClient
     * @param transmitConfig
     * @param param
     * @param handler
     */
    @Override
    public void request(WebClient webClient, TransmitConfig transmitConfig, String param, Handler<AsyncResult<String>> handler) {
        webClient.requestAbs(HttpMethod.POST, transmitConfig.getApiPath())
                .putHeader(CONTENT_TYPE, "application/xml")
                .timeout(transmitConfig.getTimeout())
                .sendBuffer(Buffer.buffer(param), asyncResult -> {
                    if (asyncResult.succeeded()) {
                        handler.handle(Future.succeededFuture(asyncResult.result().bodyAsString(CrtrUtils.CHARSET_NAME)));
                    } else {
                        handler.handle(Future.failedFuture(asyncResult.cause()));
                    }
                });
    }
}
