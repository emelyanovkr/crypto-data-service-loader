package com.crypto.service;

import com.crypto.service.util.TicketsDataReader;

public class LoaderApplication {

  public static void main(String[] args) {
    TicketsDataReader ticketsDataReader = new TicketsDataReader();
    ticketsDataReader.readExecutor();
  }
}