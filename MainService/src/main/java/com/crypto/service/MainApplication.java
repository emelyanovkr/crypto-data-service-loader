package com.crypto.service;

import com.crypto.service.util.TickersDataReader;

public class MainApplication {
  public static void main(String[] args){
    TickersDataReader tickersDataReader = new TickersDataReader();
    tickersDataReader.readExecutor();
  }
}
