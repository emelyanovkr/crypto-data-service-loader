package com.crypto.service.utils;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Mono;

import java.util.Properties;

public class ConnectionHandler {
    public static Mono<Connection> initConnection() {
        Properties properties = PropertiesLoader.loadProperties();

        ConnectionFactoryOptions options =
                ConnectionFactoryOptions.builder()
                        .option(ConnectionFactoryOptions.DRIVER, properties.getProperty("DRIVER"))
                        .option(ConnectionFactoryOptions.PROTOCOL, properties.getProperty("PROTOCOL"))
                        .option(ConnectionFactoryOptions.HOST, properties.getProperty("HOST"))
                        .option(ConnectionFactoryOptions.PORT, Integer.parseInt(properties.getProperty("PORT")))
                        .option(ConnectionFactoryOptions.USER, properties.getProperty("USER"))
                        .option(ConnectionFactoryOptions.PASSWORD, properties.getProperty("PASSWORD"))
                        .option(ConnectionFactoryOptions.DATABASE, properties.getProperty("DATABASE"))
                        .option(
                                ConnectionFactoryOptions.SSL, Boolean.parseBoolean(properties.getProperty("SSL")))
                        .build();

        return Mono.from(ConnectionFactories.get(options).create());
    }
}
