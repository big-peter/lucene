/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.search;

import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;

/**
 * A {@link Collector} implementation that collects the top-scoring hits, returning them as a {@link
 * TopDocs}. This is used by {@link IndexSearcher} to implement {@link TopDocs}-based search. Hits
 * are sorted by score descending and then (when the scores are tied) docID ascending. When you
 * create an instance of this collector you should know in advance whether documents are going to be
 * collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and {@link Float#NEGATIVE_INFINITY} are not valid
 * scores. This collector will not properly collect hits with such scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  /** Scorable leaf collector */
  public abstract static class ScorerLeafCollector implements LeafCollector {

    protected Scorable scorer;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      this.scorer = scorer;
    }
  }

  private static class SimpleTopScoreDocCollector extends TopScoreDocCollector {

    SimpleTopScoreDocCollector(
        int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      // 根据context更新docBase
      // reset the minimum competitive score
      docBase = context.docBase;
      minCompetitiveScore = 0f;

      return new ScorerLeafCollector() {
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          if (minScoreAcc == null) {
            updateMinCompetitiveScore(scorer);
          } else {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          // 打分。scorer - TermScorer,从postingenum中获取freq来计算分数
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          // 每1024次更新一次
          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          // 该文档分数比目前收集的文档的最小分还小,不收集
          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }

          // 否则, 移除堆顶文档,将该文档放入堆顶, 重新平衡
          // pqTop指向堆顶，通过更新堆顶对象的属性实现和移除元素一样的效果，同时避免创建新对象
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          // 下推，pqTop重新指向堆顶元素
          pqTop = pq.updateTop();
          updateMinCompetitiveScore(scorer);
        }
      };
    }
  }

  private static class PagingTopScoreDocCollector extends TopScoreDocCollector {

    private final ScoreDoc after;
    private int collectedHits;

    PagingTopScoreDocCollector(
        int numHits,
        ScoreDoc after,
        HitsThresholdChecker hitsThresholdChecker,
        MaxScoreAccumulator minScoreAcc) {
      super(numHits, hitsThresholdChecker, minScoreAcc);
      this.after = after;
      this.collectedHits = 0;
    }

    @Override
    protected int topDocsSize() {
      return collectedHits < pq.size() ? collectedHits : pq.size();
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
          ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
          : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      docBase = context.docBase;
      final int afterDoc = after.doc - context.docBase;
      minCompetitiveScore = 0f;

      return new ScorerLeafCollector() {
        @Override
        public void setScorer(Scorable scorer) throws IOException {
          super.setScorer(scorer);
          if (minScoreAcc == null) {
            updateMinCompetitiveScore(scorer);
          } else {
            updateGlobalMinCompetitiveScore(scorer);
          }
        }

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector relies on the fact that scorers produce positive values:
          assert score >= 0; // NOTE: false for NaN

          totalHits++;
          hitsThresholdChecker.incrementHitCount();

          if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
            updateGlobalMinCompetitiveScore(scorer);
          }

          // 前面的页已经收集过了
          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }
            return;
          }

          // 比topN的最小值还小，不收集
          if (score <= pqTop.score) {
            if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
              // we just reached totalHitsThreshold, we can start setting the min
              // competitive score now
              updateMinCompetitiveScore(scorer);
            }

            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          collectedHits++;
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          updateMinCompetitiveScore(scorer);
        }
      };
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to collect and the number
   * of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
   * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
   * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
   * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
   * but will also likely make query processing slower.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel objects.
   */
  public static TopScoreDocCollector create(int numHits, int totalHitsThreshold) {
    return create(numHits, null, totalHitsThreshold);
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to collect, the bottom of
   * the previous page, and the number of hits to count accurately.
   *
   * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
   * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
   * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
   * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
   * but will also likely make query processing slower.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {
    return create(
        numHits, after, HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)), null);
  }

  static TopScoreDocCollector create(
      int numHits,
      ScoreDoc after,
      HitsThresholdChecker hitsThresholdChecker,
      MaxScoreAccumulator minScoreAcc) {

    if (numHits <= 0) {
      throw new IllegalArgumentException(
          "numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (hitsThresholdChecker == null) {
      throw new IllegalArgumentException("hitsThresholdChecker must be non null");
    }

    if (after == null) {
      // 不分页
      return new SimpleTopScoreDocCollector(numHits, hitsThresholdChecker, minScoreAcc);
    } else {
      // 分页
      return new PagingTopScoreDocCollector(numHits, after, hitsThresholdChecker, minScoreAcc);
    }
  }

  /**
   * Create a CollectorManager which uses a shared hit counter to maintain number of hits and a
   * shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
   */
  public static CollectorManager<TopScoreDocCollector, TopDocs> createSharedManager(
      int numHits, ScoreDoc after, int totalHitsThreshold) {

    int totalHitsMax = Math.max(totalHitsThreshold, numHits);
    return new CollectorManager<>() {

      private final HitsThresholdChecker hitsThresholdChecker =
          HitsThresholdChecker.createShared(totalHitsMax);
      private final MaxScoreAccumulator minScoreAcc =
          totalHitsMax == Integer.MAX_VALUE ? null : new MaxScoreAccumulator();

      @Override
      public TopScoreDocCollector newCollector() throws IOException {
        return TopScoreDocCollector.create(numHits, after, hitsThresholdChecker, minScoreAcc);
      }

      @Override
      public TopDocs reduce(Collection<TopScoreDocCollector> collectors) throws IOException {
        final TopDocs[] topDocs = new TopDocs[collectors.size()];
        int i = 0;
        for (TopScoreDocCollector collector : collectors) {
          topDocs[i++] = collector.topDocs();
        }
        return TopDocs.merge(0, numHits, topDocs);
      }
    };
  }

  int docBase;
  ScoreDoc pqTop;
  final HitsThresholdChecker hitsThresholdChecker;
  final MaxScoreAccumulator minScoreAcc;
  float minCompetitiveScore;

  // prevents instantiation
  TopScoreDocCollector(
      int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
    super(new HitQueue(numHits, true));
    assert hitsThresholdChecker != null;

    // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
    // that at this point top() is already initialized.
    // pqTop一直指向堆顶
    pqTop = pq.top();
    this.hitsThresholdChecker = hitsThresholdChecker;
    this.minScoreAcc = minScoreAcc;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
  }

  @Override
  public ScoreMode scoreMode() {
    return hitsThresholdChecker.scoreMode();
  }

  // 下面两个update方法主要是更新scorer的min score，使随后低于score的文档不走collect流程。【score每次更新要不不变，要么增大】
  protected void updateGlobalMinCompetitiveScore(Scorable scorer) throws IOException {
    assert minScoreAcc != null;
    DocAndScore maxMinScore = minScoreAcc.get();
    if (maxMinScore != null) {
      // 如果当前docBase在maxMinScore之后会相等，只有在score更大时才会收集；否则，score相等时也会收集
      // since we tie-break on doc id and collect in doc id order we can require
      // the next float if the global minimum score is set on a document id that is
      // smaller than the ids in the current leaf
      float score =
          docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
      if (score > minCompetitiveScore) {
        assert hitsThresholdChecker.isThresholdReached();
        scorer.setMinCompetitiveScore(score);
        minCompetitiveScore = score;
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
      }
    }
  }

  protected void updateMinCompetitiveScore(Scorable scorer) throws IOException {
    // 仅当已经收集了n个doc，且pq已满时更新
    if (hitsThresholdChecker.isThresholdReached()
        && pqTop != null
        && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the score of sentinels

      // 获取更大的下一个score
      // since we tie-break on doc id and collect in doc id order, we can require
      // the next float
      float localMinScore = Math.nextUp(pqTop.score);
      if (localMinScore > minCompetitiveScore) {
        // 更新scorer的minScore，随后低于minScore的文档不再走收集流程【等于的会走】
        scorer.setMinCompetitiveScore(localMinScore);
        // 更新relation
        totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
        // 更新minCompetitiveScore为下一个更大的score
        minCompetitiveScore = localMinScore;
        if (minScoreAcc != null) {
          // 更新最小score记录器
          // we don't use the next float but we register the document
          // id so that other leaves can require it if they are after
          // the current maximum
          minScoreAcc.accumulate(docBase, pqTop.score);
        }
      }
    }
  }
}
