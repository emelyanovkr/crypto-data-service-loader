package com.crypto.service;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

// TODO: SELECT OUTPUT DATA IN FILE OR SOMETHING ELSE
// Decomposition in several functions, maybe creating interface
// ACQUIRE LOCK
// implement nullaway?

public class DataLoader {

  private static ConnectionFactory connectionFactory;

  public static Properties loadProperties() {
    Properties properties = new Properties();
    try (InputStream input = new FileInputStream("src/main/resources/connection_data.properties")) {
      properties.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return properties;
  }

  public static void initConnectionFactory() {
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

    connectionFactory = ConnectionFactories.get(options);
  }

  public static void selectQuery() {
    Mono.from(connectionFactory.create())
        .flatMapMany(connection -> connection.createStatement("SELECT * FROM btc_data").execute())
        .flatMap(
            result ->
                result.map(
                    (row, rowMetadata) ->
                        String.format(
                            " open_time: %s\n open_price: %.2f\n high_price: %.2f\n low_price: %.2f\n"
                                + " close_price: %.2f\n volume: %.2f\n close_time: %s\n quote_volume: %.2f\n"
                                + " count: %d\n taker_buy_volume: %.2f\n taker_buy_quote_volume: %.2f\n "
                                + "ignore_column: %d\n",
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

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void insertQuery() {
    Mono.from(connectionFactory.create())
        .flatMapMany(
            connection ->
                connection
                    .createStatement(
                        "INSERT INTO btc_data VALUES (:open_time, :open_price, :high_price, :low_price, "
                            + ":close_price, :volume, :close_time, :quote_volume, :count, "
                            + ":taker_buy_volume, :taker_buy_quote_volume, :ignore_column)")
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
                    .execute())
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

  public static void readFromFile() {
    final List<String> data;

    try (Stream<String> stream =
        Files.lines(Path.of("src/main/resources/USDTRON-1m-2024-02-04.csv"))) {
      data = stream.toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Mono.from(connectionFactory.create())
        .flatMapMany(
            connection ->
                Flux.fromIterable(data)
                    .flatMap(
                        line -> {
                          String[] values = line.split(",");

                          LocalDateTime open_time =
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(Long.parseLong(values[0])), ZoneOffset.UTC);
                          LocalDateTime close_time =
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(Long.parseLong(values[6])), ZoneOffset.UTC);

                          return connection
                              .createStatement(
                                  "INSERT INTO btc_data VALUES (:open_time, :open_price, :high_price, :low_price, "
                                      + ":close_price, :volume, :close_time, :quote_volume, :count, "
                                      + ":taker_buy_volume, :taker_buy_quote_volume, :ignore_column)")
                              .bind("open_time", open_time)
                              .bind("open_price", values[1])
                              .bind("high_price", values[2])
                              .bind("low_price", values[3])
                              .bind("close_price", values[4])
                              .bind("volume", values[5])
                              .bind("close_time", close_time)
                              .bind("quote_volume", values[7])
                              .bind("count", values[8])
                              .bind("taker_buy_volume", values[9])
                              .bind("taker_buy_quote_volume", values[10])
                              .bind("ignore_column", values[11])
                              .execute();
                        }))
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .subscribe();
  }

  public static void main(String[] args) {
    initConnectionFactory();

    selectQuery();

    try {
      Thread.sleep(50000);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
