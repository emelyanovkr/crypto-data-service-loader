package com.crypto.service;

import com.crypto.service.data.TickersDataLoader;

public class MainApplication {
  public static void main(String[] args) {
    TickersDataLoader dataLoader = new TickersDataLoader();
    dataLoader.uploadTickersData();
  }
}
