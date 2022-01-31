package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;

/**
 * A concrete implementation of {@link WebCrawler} that runs multiple threads on a
 * {@link ForkJoinPool} to fetch and process multiple web pages in parallel.
 */
final class ParallelWebCrawler implements WebCrawler {
  private final Clock clock;
  private final Duration timeout;
  private final int popularWordCount;
  private final ForkJoinPool pool;
  private final PageParserFactory parserFactory;
  private final int maxDepth;
  private final List<Pattern> ignoredUrls;

  @Inject
  ParallelWebCrawler(
          Clock clock,
          PageParserFactory parserFactory,
          @Timeout Duration timeout,
          @PopularWordCount int popularWordCount,
          @TargetParallelism int threadCount,
          @MaxDepth int maxDepth,
          @IgnoredUrls List<Pattern> ignoredUrls) {
    this.clock = clock;
    this.parserFactory = parserFactory;
    this.timeout = timeout;
    this.popularWordCount = popularWordCount;
    this.pool = new ForkJoinPool(Math.min(threadCount, getMaxParallelism()));
    this.maxDepth = maxDepth;
    this.ignoredUrls = ignoredUrls;
  }

  @Override
  public CrawlResult crawl(List<String> startingUrls) {
    Instant deadLine = clock.instant().plus(timeout);
    Map<String, Integer> counts = Collections.synchronizedMap(new LinkedHashMap<>());
    Set<String> visitedUrls = new ConcurrentSkipListSet<>();

    for (String startingUrl : startingUrls) {
      pool.invoke(new CrawlInnerTask.Builder()
              .clock(clock)
              .counts(counts)
              .deadLine(deadLine)
              .ignoredUrls(ignoredUrls)
              .maxDepth(maxDepth)
              .parserFactory(parserFactory)
              .url(startingUrl)
              .visitedUrls(visitedUrls)
              .build());
    }

    if (counts.isEmpty()) {
      return new CrawlResult.Builder()
              .setUrlsVisited(visitedUrls.size())
              .setWordCounts(counts)
              .build();
    }

    return new CrawlResult.Builder()
            .setUrlsVisited(visitedUrls.size())
            .setWordCounts(WordCounts.sort(counts, popularWordCount))
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }

}
