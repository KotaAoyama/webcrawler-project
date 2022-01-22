package com.udacity.webcrawler.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A static utility class that loads a JSON configuration file.
 */
public final class ConfigurationLoader {

    private final Path path;

    /**
     * Create a {@link ConfigurationLoader} that loads configuration from the given {@link Path}.
     */
    public ConfigurationLoader(Path path) {
        this.path = Objects.requireNonNull(path);
    }

    /**
     * Loads configuration from this {@link ConfigurationLoader}'s path
     *
     * @return the loaded {@link CrawlerConfiguration}.
     */
    public CrawlerConfiguration load() {
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            return read(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new CrawlerConfiguration.Builder().build();
    }

    /**
     * Loads crawler configuration from the given reader.
     *
     * @param reader a Reader pointing to a JSON string that contains crawler configuration.
     * @return a crawler configuration
     */
    public static CrawlerConfiguration read(Reader reader) {
        // This is here to get rid of the unused variable warning.
        Objects.requireNonNull(reader);

        ObjectMapper mapper = new ObjectMapper();
        mapper.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
        CrawlerConfiguration config = null;
        try {
            config = mapper.readValue(reader, CrawlerConfiguration.class);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (config == null) {
            return new CrawlerConfiguration.Builder().build();
        }

        return new CrawlerConfiguration.Builder()
                .addIgnoredWords(config.getIgnoredWords().stream().map(Pattern::toString).toArray(String[]::new))
                .addIgnoredUrls(config.getIgnoredUrls().stream().map(Pattern::toString).toArray(String[]::new))
                .addStartPages(config.getStartPages().toArray(String[]::new))
                .setImplementationOverride(config.getImplementationOverride())
                .setMaxDepth(config.getMaxDepth())
                .setParallelism(config.getParallelism())
                .setPopularWordCount(config.getPopularWordCount())
                .setProfileOutputPath(config.getProfileOutputPath())
                .setResultPath(config.getResultPath())
                .setTimeoutSeconds(config.getTimeout().toSecondsPart())
                .build();
    }
}
