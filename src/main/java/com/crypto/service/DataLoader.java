package com.crypto.service;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

public class DataLoader {

  public static Properties loadProperties() {
    Properties properties = new Properties();
    try (InputStream input = new FileInputStream("src/main/resources/connection_data.properties")) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return properties;
  }

  public static void main(String[] args) {
    Properties properties = loadProperties();

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

    ConnectionFactory connectionFactory = ConnectionFactories.get(options);

    Mono.from(connectionFactory.create())
        .flatMapMany(connection -> connection.createStatement("SELECT * FROM test_table").execute())
        .flatMap(
            result ->
                result.map(
                    (row, rowMetadata) ->
                        String.format(
                            "id: %s,\nprice: %.2f, \ncount: %d, \ntime: %s, \nsome_value: %.2f\n",
                            row.get("id", String.class),
                            row.get("price", Float.class),
                            row.get("count", Integer.class),
                            row.get("time", LocalDateTime.class),
                            row.get("some_value", Float.class))))
        .doOnNext(System.out::println)
        .subscribe();

    Mono.from(connectionFactory.create())
        .flatMapMany(
            connection -> {
              return connection
                  .createStatement(
                      "INSERT INTO test_table VALUES (:id, :price, :count, :time, :some_value)")
                  .bind("id", UUID.randomUUID())
                  .bind("price", "5332.5")
                  .bind("count", "1011")
                  .bind("time", LocalDateTime.now())
                  .bind("some_value", "319.22")
                  .execute();
            })
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .subscribe();

    try {
      Thread.sleep(10000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
