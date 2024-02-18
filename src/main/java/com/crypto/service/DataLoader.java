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
        .flatMapMany(connection -> connection.createStatement("SELECT * FROM btc_data").execute())
        .flatMap(
            result ->
                result.map(
                    (row, rowMetadata) ->
                        String.format(
                            " open_time: %s\n open_price: %.2f\n high_price: %.2f\n low_price: %.2f\n"
                                + " close_price: %.2f\n volume: %.2f\n close_time: %s\n quote_volume: %.2f\n"
                                + " count: %d\n taker_buy_volume: %.2f\n taker_buy_quote_volume: %.2f\n ignore_column: %d\n",
                            row.get("open_time", String.class),
                            row.get("open_price", Float.class),
                            row.get("high_price", Float.class),
                            row.get("low_price", Float.class),
                            row.get("close_price", Float.class),
                            row.get("volume", Float.class),
                            row.get("close_time", LocalDateTime.class),
                            row.get("quote_volume", Float.class),
                            row.get("count", Integer.class),
                            row.get("taker_buy_volume", Float.class),
                            row.get("taker_buy_quote_volume", Float.class),
                            row.get("ignore_column", Integer.class))))
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .doOnNext(System.out::println)
        .subscribe();

    Mono.from(connectionFactory.create())
        .flatMapMany(
            connection -> {
              return connection
                  .createStatement(
                      "INSERT INTO btc_data VALUES (:open_time, :open_price, :high_price, :low_price, " +
                              ":close_price, :volume, :close_time, :quote_volume, :count, " +
                              ":taker_buy_volume, :taker_buy_quote_volume, :ignore_column)")
                  .bind("open_time", LocalDateTime.now())
                  .bind("open_price", "3353.21")
                  .bind("high_price", "3112.1")
                  .bind("low_price", "59931.11")
                  .bind("close_price", "99911.21")
                  .bind("volume", "21001.1")
                  .bind("close_time", LocalDateTime.now())
                  .bind("quote_volume", "111.11")
                  .bind("count", "990")
                  .bind("taker_buy_volume", "66.56")
                  .bind("taker_buy_quote_volume", "7761.11")
                  .bind("ignore_column", "0")
                  .execute();
            })
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .subscribe();

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
