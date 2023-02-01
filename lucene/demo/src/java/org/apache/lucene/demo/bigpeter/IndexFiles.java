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
package org.apache.lucene.demo.bigpeter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.DemoEmbeddings;
import org.apache.lucene.demo.knn.KnnVectorDict;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Index all text files under a directory.
 *
 * <p>This is a command-line application demonstrating simple Lucene indexing. Run it with no
 * command-line arguments for usage information.
 */
public class IndexFiles implements AutoCloseable {
  static final String KNN_DICT = "knn-dict";

  // Calculates embedding vectors for KnnVector search
  private final DemoEmbeddings demoEmbeddings;
  private final KnnVectorDict vectorDict;

  private IndexFiles(KnnVectorDict vectorDict) throws IOException {
    if (vectorDict != null) {
      this.vectorDict = vectorDict;
      demoEmbeddings = new DemoEmbeddings(vectorDict);
    } else {
      this.vectorDict = null;
      demoEmbeddings = null;
    }
  }

  /** Index all text files under a directory. */
  public static void main(String[] args) throws Exception {
    String indexPath = "./resources/dir_index";
    String docsPath = "./lucene";

    String vectorDictSource = null;
    boolean create = true;

    final Path docDir = Paths.get(docsPath);
    if (!Files.isReadable(docDir)) {
      System.out.println(
          "Document directory '"
              + docDir.toAbsolutePath()
              + "' does not exist or is not readable, please check the path");
      System.exit(1);
    }

    long start = System.currentTimeMillis();
    try {
      System.out.println("Indexing to directory '" + indexPath + "'...");

      // purge old index file
      Directory dir = FSDirectory.open(Paths.get(indexPath));
      for (String fileName : dir.listAll()) {
        dir.deleteFile(fileName);
      }

      Analyzer analyzer = new StandardAnalyzer();
      IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

      iwc.setUseCompoundFile(false);
      // keep all commit checkpoint files
      iwc.setIndexDeletionPolicy(NoDeletionPolicy.INSTANCE);
      // merge policy
      iwc.setMergePolicy(new TieredMergePolicy());

      if (create) {
        iwc.setOpenMode(OpenMode.CREATE);
      } else {
        iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
      }

      // Optional: for better indexing performance, if you
      // are indexing many documents, increase the RAM
      // buffer.  But if you do this, increase the max heap
      // size to the JVM (eg add -Xmx512m or -Xmx1g):
      //
      iwc.setRAMBufferSizeMB(32.0);

      KnnVectorDict vectorDictInstance = null;
      long vectorDictSize = 0;
      if (vectorDictSource != null) {
        KnnVectorDict.build(Paths.get(vectorDictSource), dir, KNN_DICT);
        vectorDictInstance = new KnnVectorDict(dir, KNN_DICT);
        vectorDictSize = vectorDictInstance.ramBytesUsed();
      }

      // create IndexWriter
      IndexWriter writer = new IndexWriter(dir, iwc);
      ScheduledExecutorService flushAndCommitExecutor = new ScheduledThreadPoolExecutor(2);
      ExecutorService indexExecutor = new ThreadPoolExecutor(2, 2, 0, TimeUnit.SECONDS, new LinkedBlockingDeque<>(100));

      FlushTask flushTask = new FlushTask(writer);
      CommitTask commitTask = new CommitTask(writer);

      try {
        // schedule flush action
        flushAndCommitExecutor.scheduleWithFixedDelay(flushTask, 0, 1, TimeUnit.SECONDS);

        // schedule commit action
        flushAndCommitExecutor.scheduleWithFixedDelay(commitTask, 0, 3, TimeUnit.SECONDS);

        // start index files
        try (IndexFiles indexFiles = new IndexFiles(vectorDictInstance)) {
          Directory sourceDir = FSDirectory.open(Paths.get(docsPath));
          List<Future<?>> futures = new ArrayList<>();
          for (String subDir : sourceDir.listAll()) {
            Future<?> future = indexExecutor.submit(() -> {
              try {
                indexFiles.indexDocs(writer, Paths.get(docsPath, subDir));
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });

            futures.add(future);
          }

          for (Future<?> f : futures) {
            f.get();
          }

          // NOTE: if you want to maximize search performance,
          // you can optionally call forceMerge here.  This can be
          // a terribly costly operation, so generally it's only
          // worth it when your index is relatively static (ie
          // you're done adding documents to it):
          //
          // writer.forceMerge(1);
        } finally {
          IOUtils.close(vectorDictInstance);
        }
      } finally {
        // wait docs that last indexed but not flushed and committed to flush and commit
        TimeUnit.SECONDS.sleep(5);
        indexExecutor.shutdownNow();
        indexExecutor.awaitTermination(10, TimeUnit.SECONDS);
        flushAndCommitExecutor.shutdownNow();
        flushAndCommitExecutor.awaitTermination(10, TimeUnit.SECONDS);
        writer.close();
      }

      long end = System.currentTimeMillis();
      try (IndexReader reader = DirectoryReader.open(dir)) {
        // commit sequence may larger than indexed doc num. Because will advance flush queue when flush, this will use threadPool
        // size as offset, and not always all thread is indexing doc.
        System.out.println("Active flush count: " + flushTask.getActiveFlushCount() +"; commit count: " + commitTask.getCommitCount());

        System.out.println(
                "Indexed "
                        + reader.numDocs()
                        + " documents in "
                        + (end - start)
                        + " milliseconds");
        if (reader.numDocs() > 100
                && vectorDictSize < 1_000_000
                && System.getProperty("smoketester") == null) {
//          throw new RuntimeException(
//              "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
        }
      }
    } catch (IOException e) {
      System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
    }
  }

  void indexDocs(final IndexWriter writer, Path path) throws IOException {
    if (Files.isDirectory(path)) {
      Files.walkFileTree(
          path,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              try {
                indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
              } catch (
                  @SuppressWarnings("unused")
                  IOException ignore) {
                ignore.printStackTrace(System.err);
                // don't index files that can't be read.
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } else {
      indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
    }
  }

  void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
    if (file.toString().endsWith(".class")) {
      return;
    }

    try (InputStream stream = Files.newInputStream(file)) {
      // make a new, empty document
      Document doc = new Document();

      Field pathField = new StringField("path", file.toString(), Field.Store.YES);
      doc.add(pathField);

      doc.add(new LongPoint("modified", lastModified));

      doc.add(
          new TextField(
              "contents",
              new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

      if (demoEmbeddings != null) {
        try (InputStream in = Files.newInputStream(file)) {
          float[] vector =
              demoEmbeddings.computeEmbedding(
                  new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8)));
          doc.add(
              new KnnVectorField("contents-vector", vector, VectorSimilarityFunction.DOT_PRODUCT));
        }
      }

      if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
        // New index, so we just add the document (no old document can be there):
//        System.out.println("adding " + file);
        writer.addDocument(doc);
      } else {
//        System.out.println("updating " + file);
        writer.updateDocument(new Term("path", file.toString()), doc);
      }
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorDict);
  }

  private static class FlushTask implements Runnable {

    private int activeFlushCount;  // guard by synchronized
    private final IndexWriter indexWriter;

    FlushTask(IndexWriter indexWriter) {
      this.indexWriter = indexWriter;
    }

    @Override
    public synchronized void run() {
      try {
        System.out.println("Do flush action.");
        indexWriter.flush();
        activeFlushCount+=1;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public int getActiveFlushCount() {
      return activeFlushCount;
    }
  }

  private static class CommitTask implements Runnable {

    private int commitCount;  // guard by synchronized
    private final IndexWriter indexWriter;

    CommitTask(IndexWriter indexWriter) {
      this.indexWriter = indexWriter;
    }

    @Override
    public synchronized void run() {
      try {
        System.out.println("Do commit action.");
        long sequenceNumber = indexWriter.commit();
        commitCount +=1;
        System.out.println("Commit sequence number: " + sequenceNumber);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public int getCommitCount() {
      return commitCount;
    }
  }
}
