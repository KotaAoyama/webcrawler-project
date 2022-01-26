package com.udacity.webcrawler;

import com.udacity.webcrawler.json.CrawlResult;
import com.udacity.webcrawler.parser.PageParserFactory;
import org.codehaus.plexus.util.CollectionUtils;

import javax.inject.Inject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
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

    List<CrawlResult> crawlResultList = startingUrls
            .parallelStream()
            .map(
                    startingUrl -> pool.invoke(new CrawlTask.Builder()
                            .clock(clock)
                            .counts(counts)
                            .deadLine(deadLine)
                            .ignoredUrls(ignoredUrls)
                            .maxDepth(maxDepth)
                            .parserFactory(parserFactory)
                            .url(startingUrl)
                            .visitedUrls(visitedUrls)
                            .build()))
            .collect(Collectors.toList());

    if (crawlResultList.size() == 1 && crawlResultList.get(0) == null) {
      return new CrawlResult.Builder()
              .setUrlsVisited(0)
              .setWordCounts(new HashMap<String, Integer>())
              .build();
    }

    int totalVisitedUrls = crawlResultList
            .stream()
            .mapToInt(CrawlResult::getUrlsVisited)
            .sum();

    Map<String, Integer> totalWordCounts = new HashMap<>();
    List<Map<String, Integer>> wordCountsList = crawlResultList
            .stream()
            .map(CrawlResult::getWordCounts)
            .collect(Collectors.toList());
    for (Map<String, Integer> wordCounts : wordCountsList) {
      for (Map.Entry<String, Integer> e : wordCounts.entrySet()) {
        if (totalWordCounts.containsKey(e.getKey())) {
          totalWordCounts.put(e.getKey(), e.getValue() + totalWordCounts.get(e.getKey()));
        } else {
          totalWordCounts.put(e.getKey(), e.getValue());
        }
      }
    }

    return new CrawlResult.Builder()
            .setUrlsVisited(totalVisitedUrls)
            .setWordCounts(WordCounts.sort(totalWordCounts, popularWordCount))
            .build();
  }

  @Override
  public int getMaxParallelism() {
    return Runtime.getRuntime().availableProcessors();
  }
}
