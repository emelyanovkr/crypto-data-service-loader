package com.crypto.service.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

public class SourceReader {
    public static List<String> readFromFile()
    {
        try (Stream<String> stream =
                     Files.lines(Paths.get("src/main/resources/USDTRON-1m-2024-02-04.csv"))) {
            return stream.toList();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
