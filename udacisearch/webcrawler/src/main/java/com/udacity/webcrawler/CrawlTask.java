package com.udacity.webcrawler;

import com.udacity.webcrawler.parser.PageParser;
import com.udacity.webcrawler.parser.PageParserFactory;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RecursiveTask;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class CrawlTask extends RecursiveTask<CrawlTask> {

    private final Clock clock;
    private final PageParserFactory parserFactory;
    private final String url;
    private final Instant deadLine;
    private final int maxDepth;
    private final Map<String, Integer> counts;
    private final Set<String> visitedUrls;
    private final List<Pattern> ignoredUrls;

    public CrawlTask(Clock clock,
                     PageParserFactory parserFactory,
                     String url,
                     Instant deadLine,
                     int maxDepth,
                     Map<String, Integer> counts,
                     Set<String> visitedUrls,
                     List<Pattern> ignoredUrls) {
        this.clock = clock;
        this.parserFactory = parserFactory;
        this.url = url;
        this.deadLine = deadLine;
        this.maxDepth = maxDepth;
        this.counts = counts;
        this.visitedUrls = visitedUrls;
        this.ignoredUrls = ignoredUrls;
    }

    public Clock getClock() {
        return clock;
    }

    public PageParserFactory getParserFactory() {
        return parserFactory;
    }

    public String getUrl() {
        return url;
    }

    public Instant getDeadLine() {
        return deadLine;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public Map<String, Integer> getCounts() {
        return counts;
    }

    public Set<String> getVisitedUrls() {
        return visitedUrls;
    }

    public List<Pattern> getIgnoredUrls() {
        return ignoredUrls;
    }

    public static final class Builder {
        private Clock clock = Clock.systemUTC();
        private PageParserFactory parserFactory = null;
        private String url = "";
        private Instant deadLine = Instant.EPOCH;
        private int maxDepth = 0;
        private Map<String, Integer> counts = null;
        private Set<String> visitedUrls = null;
        private List<Pattern> ignoredUrls = null;

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder parserFactory(PageParserFactory parserFactory) {
            this.parserFactory = parserFactory;
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public Builder deadLine(Instant deadLine) {
            this.deadLine = deadLine;
            return this;
        }

        public Builder maxDepth(int maxDepth) {
            this.maxDepth = maxDepth;
            return this;
        }

        public Builder counts(Map<String, Integer> counts) {
            this.counts = counts;
            return this;
        }

        public Builder visitedUrls(Set<String> visitedUrls) {
            this.visitedUrls = visitedUrls;
            return this;
        }

        public Builder ignoredUrls(List<Pattern> ignoredUrls) {
            this.ignoredUrls = ignoredUrls;
            return this;
        }

        public CrawlTask build() {
            return new CrawlTask(clock,
                    parserFactory,
                    url,
                    deadLine,
                    maxDepth,
                    counts,
                    visitedUrls,
                    ignoredUrls);
        }
    }

    @Override
    protected CrawlTask compute() {
        if (maxDepth == 0 || clock.instant().isAfter(deadLine)) {
            return null;
        }
        for (Pattern pattern : ignoredUrls) {
            if (pattern.matcher(url).matches()) {
                return null;
            }
        }

        synchronized (visitedUrls) {
            if (visitedUrls.contains(url)) {
                return null;
            }
            visitedUrls.add(url);
        }

        PageParser.Result result = parserFactory.get(url).parse();
        for (Map.Entry<String, Integer> e : result.getWordCounts().entrySet()) {
            synchronized (counts) {
                if (counts.containsKey(e.getKey())) {
                    counts.put(e.getKey(), e.getValue() + counts.get(e.getKey()));
                } else {
                    counts.put(e.getKey(), e.getValue());
                }
            }
        }
        List<CrawlTask> subTasks = result.getLinks().stream()
                .map(link -> new CrawlTask(clock,
                        parserFactory,
                        link,
                        deadLine,
                        maxDepth - 1,
                        counts,
                        visitedUrls,
                        ignoredUrls))
                .collect(Collectors.toList());
        invokeAll(subTasks);
        return new CrawlTask(clock, parserFactory, null, deadLine, 0, counts, visitedUrls, ignoredUrls);
    }
}
