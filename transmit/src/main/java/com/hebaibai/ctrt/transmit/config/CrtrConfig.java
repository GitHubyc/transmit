package com.hebaibai.ctrt.transmit.config;

import com.hebaibai.ctrt.transmit.TransmitConfig;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;

public interface CrtrConfig {

    Handler<Promise<TransmitConfig>> transmitConfig(HttpMethod method, String path);

    int getPort();
}
