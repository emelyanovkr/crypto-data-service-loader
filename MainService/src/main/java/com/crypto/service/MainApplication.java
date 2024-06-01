package com.crypto.service;

import com.crypto.service.data.TickersDataLoader;

public class MainApplication {
  public static void main(String[] args) {
    Thread uploadTickersData = new Thread(new TickersDataLoader(), "UPLOAD-TICKERS-DATA-THREAD");
    uploadTickersData.start();
  }
}
