package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParserFactory;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
    Map<String, Integer> counts = new HashMap<>();
    Set<String> visitedUrls = new HashSet<>();

    List<CrawlResult> totalCrawlResult = startingUrls
            .parallelStream()
            .map(
                    url -> pool.invoke(new CrawlTask.Builder()
                            .clock(clock)
                            .counts(counts)
                            .deadLine(deadLine)
                            .ignoredUrls(ignoredUrls)
                            .maxDepth(maxDepth)
                            .parserFactory(parserFactory)
                            .url(url)
                            .visitedUrls(visitedUrls)
                            .build()))
            .map(crawlTask -> new CrawlResult.Builder()
                    .setWordCounts(WordCounts.sort(crawlTask.getCounts(), popularWordCount))
                    .setUrlsVisited(crawlTask.getVisitedUrls().size())
                    .build())
            .collect(Collectors.toList());
    int totalVisitedUrls = totalCrawlResult.stream().mapToInt(CrawlResult::getUrlsVisited).sum();
    Map<String, Integer> totalCounts = new HashMap<>();
    List<Map<String, Integer>> countsList = totalCrawlResult.stream().map(CrawlResult::getWordCounts).collect(Collectors.toList());
    for (Map<String, Integer> countMap : countsList) {
      for (String key : countMap.keySet()) {
        if (totalCounts.containsKey(key)) {
          totalCounts.replace(key, totalCounts.get(key) + countMap.get(key));
        } else {
          totalCounts.put(key, countMap.get(key));
        }
      }
    }

    return new CrawlResult.Builder()
            .setUrlsVisited(totalVisitedUrls)
            .setWordCounts(totalCounts)
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
