package com.crypto.service;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseResponse;
import com.crypto.service.util.ConnectionHandler;

// TODO: Implement tests
public class Main {

  public void testMethod()
  {
    ClickHouseNode node = ConnectionHandler.initClickHouseConnection();
    ClickHouseClient client = ClickHouseClient.newInstance(node.getProtocol());
    System.out.println(node);

    while (true) {
      try {
        System.out.println("Waiting for connection...");

        try(ClickHouseResponse response =
              client.read(node).query("SELECT 1").executeAndWait())
        {
          System.out.println(response.firstRecord().getValue(0).asLong());
        }

        System.out.println("Received info");
        Thread.sleep(3000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}

