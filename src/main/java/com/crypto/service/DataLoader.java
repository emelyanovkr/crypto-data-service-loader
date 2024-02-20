package com.crypto.service;

import com.crypto.service.utils.ConnectionHandler;
import com.crypto.service.utils.SourceReader;
import com.google.common.collect.Lists;
import io.r2dbc.spi.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

public class DataLoader {
  private static Mono<Connection> connectionMono;

  // TODO: Output data in file or something else
  public static void selectQuery() {
    connectionMono
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
        .blockLast();
  }

  public static void simpleInsertData(List<String> data, Mono<Connection> connectionInstance) {

    connectionInstance
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
                                  "INSERT INTO btc_data SETTINGS async_insert=1, wait_for_async_insert=1 " +
                                          "VALUES (:open_time, :open_price, :high_price, :low_price, "
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
        .blockLast();
  }

  public static void batchInsertData(List<String> data, Mono<Connection> connectionInstance) {

    connectionInstance
        .flatMapMany(
            connection -> {
              Flux<String> dataFlux = Flux.fromIterable(data);

              return dataFlux
                  .buffer(150)
                  .flatMap(
                      batchData -> {
                        Batch batch = connection.createBatch();

                        for (String line : batchData) {
                          String[] values = line.split(",");

                          LocalDateTime open_time =
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(Long.parseLong(values[0])), ZoneOffset.UTC);
                          LocalDateTime close_time =
                              LocalDateTime.ofInstant(
                                  Instant.ofEpochMilli(Long.parseLong(values[6])), ZoneOffset.UTC);

                          String query =
                              String.format(
                                  "INSERT INTO btc_data SETTINGS async_insert=1, wait_for_async_insert=1,"
                                      + " async_insert_busy_timeout_ms=5, async_insert_max_data_size=2000000 "
                                      + "VALUES ('%s', %s, %s, %s, %s, %s, '%s', %s, %s, %s, %s, %s)",
                                  open_time,
                                  values[1],
                                  values[2],
                                  values[3],
                                  values[4],
                                  values[5],
                                  close_time,
                                  values[7],
                                  values[8],
                                  values[9],
                                  values[10],
                                  values[11]);

                          batch.add(query);
                        }

                        return batch.execute();
                      });
            })
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .blockLast();
  }

  public static void truncateTable() {
    connectionMono
        .flatMapMany(connection -> connection.createStatement("TRUNCATE btc_data").execute())
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .blockLast();
  }

  public static void countRecords() {
    connectionMono
        .flatMapMany(
            connection -> connection.createStatement("SELECT COUNT(*) FROM btc_data").execute())
        .flatMap(result -> result.map((row, rowMetadata) -> row.get(0, Integer.class)))
        .doOnNext(line -> System.out.println("CURRENT RECORDS IN DATA " + line))
        .onErrorResume(
            throwable -> {
              throwable.printStackTrace();
              return Mono.empty();
            })
        .blockLast();
  }

  public static void main(String[] args) {
    connectionMono = ConnectionHandler.initConnection();
    List<String> data = SourceReader.readFromFile();

    List<List<String>> partitions = Lists.partition(data, 16);
    try (ExecutorService executorService = Executors.newFixedThreadPool(8)) {
      for (List<String> subset : partitions) {
        Mono<Connection> connectionInstance = ConnectionHandler.initConnection();
        executorService.submit(() -> batchInsertData(subset, connectionInstance));
        countRecords();
      }
    }
  }
}
