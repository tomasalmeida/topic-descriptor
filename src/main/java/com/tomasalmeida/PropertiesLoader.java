package com.tomasalmeida;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class PropertiesLoader {

    public static Properties load(final String configPath) throws IOException {

        if (!Files.exists(Paths.get(configPath))) {
            throw new IOException(configPath + " not found.");
        }
        final Properties cfg = new Properties();
        try (final InputStream inputStream = new FileInputStream(configPath)) {
            cfg.load(inputStream);
        }
        return cfg;
    }
}
