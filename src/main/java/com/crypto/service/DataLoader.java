package com.crypto.service;

import com.crypto.service.utils.ConnectionHandler;
import io.r2dbc.spi.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Stream;

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

  public static void readFromFile() {
    final List<String> data;

    try (Stream<String> stream =
        Files.lines(Paths.get("src/main/resources/USDTRON-1m-2024-02-04.csv"))) {
      data = stream.toList();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    connectionMono
        .flatMapMany(
            connection -> {
              Flux<String> dataFlux =
                  Flux.fromIterable(data); // Преобразование списка данных в поток

              return dataFlux
                  .buffer(400) // Группируем данные по пакетам по 500 строк
                  .flatMap(
                      batchData -> {
                        Batch batch = connection.createBatch(); // Создаем новый батч

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
                                  "INSERT INTO btc_data VALUES ('%s', %s, %s, %s, %s, %s, '%s', %s, %s, %s, %s, %s)",
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

                          batch.add(query); // Добавляем запрос в пакет
                        }

                        return batch.execute(); // Выполняем пакет
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

    readFromFile();
  }
}
