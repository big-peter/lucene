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

package org.apache.solr.cloud;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.cloud.Slice;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore // MRM-TEST TODO: harden, after recent speet up I see cant create config beause it defines
// lib and is not protected config set and A shard can only be split into 2 to 8 subshards in one split request. Provided numSubShards=1
public class SplitShardTest extends SolrCloudTestCase {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final String COLLECTION_NAME = "splitshardtest-collection";

  @BeforeClass
  public static void setupCluster() throws Exception {
    System.setProperty("solr.enableMetrics", "true");
    configureCluster(1)
        .addConfig("conf", SolrTestUtil.configset("cloud-minimal"))
        .configure();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @After
  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    // TODO: we should not do this when not needed, but why does it fail (timeout) in
    // these kinds of tests very rarely
    //cluster.deleteAllCollections();
  }

  @Test
  public void doTest() throws IOException, SolrServerException {
    CollectionAdminRequest
        .createCollection(COLLECTION_NAME, "conf", 2, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());
    
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setShardName("s1");
    splitShard.process(cluster.getSolrClient());
    cluster.waitForActiveCollection(COLLECTION_NAME, 6, 6);

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("s2").setNumSubShards(10);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards > 8");
    } catch (BaseHttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request."));
    }

    try {
      splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME).setShardName("s2").setNumSubShards(1);
      splitShard.process(cluster.getSolrClient());
      fail("SplitShard should throw an exception when numSubShards < 2");
    } catch (BaseHttpSolrClient.RemoteSolrException ex) {
      assertTrue(ex.getMessage().contains("A shard can only be split into 2 to 8 subshards in one split request. Provided numSubShards=1"));
    }
  }

  @Test
  public void multipleOptionsSplitTest() throws IOException, SolrServerException {
    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(COLLECTION_NAME)
        .setNumSubShards(5)
        .setRanges("0-c,d-7fffffff")
        .setShardName("s1");
    boolean expectedException = false;
    try {
      splitShard.process(cluster.getSolrClient());
      fail("An exception should have been thrown");
    } catch (BaseHttpSolrClient.RemoteSolrException ex) {
      expectedException = true;
    }
    assertTrue("Expected SolrException but it didn't happen", expectedException);
  }

  @Test
  public void testSplitFuzz() throws Exception {
    String collectionName = "splitFuzzCollection";
    CollectionAdminRequest
        .createCollection(collectionName, "conf", 2, 1)
        .setMaxShardsPerNode(100)
        .process(cluster.getSolrClient());

    CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
        .setSplitFuzz(0.5f)
        .setShardName("s1");
    splitShard.process(cluster.getSolrClient());
    DocCollection coll = cluster.getSolrClient().getZkStateReader().getClusterState().getCollection(collectionName);
    Slice s1_0 = coll.getSlice("s1_0");
    Slice s1_1 = coll.getSlice("s1_1");
    long fuzz = ((long)Integer.MAX_VALUE >> 3) + 1L;
    long delta0 = s1_0.getRange().max - s1_0.getRange().min;
    long delta1 = s1_1.getRange().max - s1_1.getRange().min;
    long expected0 = (Integer.MAX_VALUE >> 1) + fuzz;
    long expected1 = (Integer.MAX_VALUE >> 1) - fuzz;
    assertEquals("wrong range in s1_0", expected0, delta0);
    assertEquals("wrong range in s1_1", expected1, delta1);
  }


  CloudHttp2SolrClient createCollection(String collectionName, int repFactor) throws Exception {

      CollectionAdminRequest
          .createCollection(collectionName, "conf", 1, repFactor)
          .setMaxShardsPerNode(100)
          .process(cluster.getSolrClient());

    CloudHttp2SolrClient client = cluster.getSolrClient();
    client.setDefaultCollection(collectionName);
    return client;
  }


  long getNumDocs(CloudHttp2SolrClient client) throws Exception {
    String collectionName = client.getDefaultCollection();
    DocCollection collection = client.getZkStateReader().getClusterState().getCollection(collectionName);
    Collection<Slice> slices = collection.getSlices();

    long totCount = 0;
    for (Slice slice : slices) {
      if (!slice.getState().equals(Slice.State.ACTIVE)) continue;
      long lastReplicaCount = -1;
      for (Replica replica : slice.getReplicas()) {
        SolrClient replicaClient = SolrTestCaseJ4.getHttpSolrClient(replica.getBaseUrl() + "/" + replica.getName());
        long numFound = 0;
        try {
          numFound = replicaClient.query(params("q", "*:*", "distrib", "false")).getResults().getNumFound();
          log.info("Replica count={} for {}", numFound, replica);
        } finally {
          replicaClient.close();
        }
        if (lastReplicaCount >= 0) {
          assertEquals("Replica doc count for " + replica, lastReplicaCount, numFound);
        }
        lastReplicaCount = numFound;
      }
      totCount += lastReplicaCount;
    }


    long cloudClientDocs = client.query(new SolrQuery("*:*")).getResults().getNumFound();
    assertEquals("Sum of shard count should equal distrib query doc count", totCount, cloudClientDocs);
    return totCount;
  }

  void doLiveSplitShard(String collectionName, int repFactor, int nThreads) throws Exception {
    final CloudHttp2SolrClient client = createCollection(collectionName, repFactor);
    List<Future> futures = new ArrayList<>();
    final ConcurrentHashMap<String,Long> model = new ConcurrentHashMap<>();  // what the index should contain
    final AtomicBoolean doIndex = new AtomicBoolean(true);
    final AtomicInteger docsIndexed = new AtomicInteger();
    Callable[] indexThreads = new Callable[nThreads];
    try {

      for (int i=0; i<nThreads; i++) {
        indexThreads[i] = new Callable() {
          @Override
          public Object call() throws Exception {
            while (doIndex.get()) {
              try {
                // Thread.sleep(10);  // cap indexing rate at 100 docs per second per thread
                int currDoc = docsIndexed.incrementAndGet();
                String docId = "doc_" + currDoc;

                // Try all docs in the same update request
                UpdateRequest updateReq = new UpdateRequest();
                updateReq.add(SolrTestCaseJ4.sdoc("id", docId));
                // UpdateResponse ursp = updateReq.commit(client, collectionName);  // uncomment this if you want a commit each time
                UpdateResponse ursp = updateReq.process(client, collectionName);
                assertEquals(0, ursp.getStatus());  // for now, don't accept any failures
                if (ursp.getStatus() == 0) {
                  model.put(docId, 1L);  // in the future, keep track of a version per document and reuse ids to keep index from growing too large
                }
              } catch (Exception e) {
                fail(e.getMessage());
                break;
              }
            }
            return null;
          }
        };
      }

      for (Callable thread : indexThreads) {
        futures.add(getTestExecutor().submit(thread));
      }
      Thread.sleep(350);  // wait for a few docs to be indexed before invoking split
      int docCount = model.size();

      CollectionAdminRequest.SplitShard splitShard = CollectionAdminRequest.splitShard(collectionName)
          .setShardName("s1");
      splitShard.process(client);

      // make sure that docs were able to be indexed during the split
      assertTrue(model.size() > docCount);

      Thread.sleep(250);  // wait for a few more docs to be indexed after split

    } finally {
      // shut down the indexers
      doIndex.set(false);
      for (Future future : futures) {
        future.get();
      }
    }

    client.commit();  // final commit is needed for visibility

    long numDocs = getNumDocs(client);
    if (numDocs != model.size()) {
      SolrDocumentList results = client.query(new SolrQuery("q","*:*", "fl","id", "rows", Integer.toString(model.size()) )).getResults();
      Map<String,Long> leftover = new HashMap<>(model);
      for (SolrDocument doc : results) {
        String id = (String) doc.get("id");
        leftover.remove(id);
      }
      log.error("MISSING DOCUMENTS: {}", leftover);
    }

    assertEquals("Documents are missing!", docsIndexed.get(), numDocs);
    log.info("Number of documents indexed and queried : {}", numDocs);
  }



  @Test
  public void testLiveSplit() throws Exception {
    // Debugging tips: if this fails, it may be easier to debug by lowering the number fo threads to 1 and looping the test
    // until you get another failure.
    // You may need to further instrument things like DistributedZkUpdateProcessor to display the cluster state for the collection, etc.
    // Using more threads increases the chance to hit a concurrency bug, but too many threads can overwhelm single-threaded buffering
    // replay after the low level index split and result in subShard leaders that can't catch up and
    // become active (a known issue that still needs to be resolved.)
    doLiveSplitShard("livesplit1", 1, 1);
  }


}
