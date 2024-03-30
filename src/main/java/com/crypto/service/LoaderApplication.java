package com.crypto.service;

import com.crypto.service.config.LogServiceConfigurationFactory;
import com.crypto.service.util.TicketsDataReader;
import org.apache.logging.log4j.core.config.ConfigurationFactory;

public class LoaderApplication {

  public static void main(String[] args) {

    // Setup configuration provided from LogService... class
    ConfigurationFactory.setConfigurationFactory(new LogServiceConfigurationFactory());

    TicketsDataReader ticketsDataReader = new TicketsDataReader();
    ticketsDataReader.readExecutor();
  }
}
