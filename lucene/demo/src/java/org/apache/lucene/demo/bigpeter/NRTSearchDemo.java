package org.apache.lucene.demo.bigpeter;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NRTSearchDemo {

    static class Indexer {

        private IndexWriter indexWriter;

        public Indexer(IndexWriter indexWriter) {
            this.indexWriter = indexWriter;
        }

        public void index(Document doc) throws IOException {
            indexWriter.addDocuments(Collections.singletonList(doc));
        }

    }

    static class Searcher {

        private static final int TOP_N = 10;

        private SearcherManager searcherManager;

        public Searcher(SearcherManager searcherManager) {
            this.searcherManager = searcherManager;
        }

        public TopDocs search(String rawQuery) throws IOException {
            Query query = null;
            IndexSearcher searcher = searcherManager.acquire();
            try {
                return searcher.search(query, TOP_N);
            } finally {
                searcherManager.release(searcher);
            }
        }

    }

    static class IndexRefresher {

        private SearcherManager searcherManager;
        private ScheduledExecutorService scheduledExecutorService = new ScheduledThreadPoolExecutor(1);

        public IndexRefresher(SearcherManager searcherManager) {
            this.searcherManager = searcherManager;
        }

        public void start() {
            scheduledExecutorService.scheduleWithFixedDelay(() -> {
                try {
                    searcherManager.maybeRefresh();
                } catch (IOException e) {
                    System.out.println("Refresh index happened error." + e);
                }
            }, 0, 1, TimeUnit.SECONDS);
        }

    }

    static class Admin {

        private IndexWriter indexWriter;
        private SearcherManager searcherManager;

        public Admin() throws IOException {
            Directory directory = FSDirectory.open(Paths.get("./resources/nrt_search"));
            IndexWriterConfig iwc = new IndexWriterConfig();
            indexWriter = new IndexWriter(directory, iwc);

            searcherManager = new SearcherManager(indexWriter, new SearcherFactory());
        }

        public IndexWriter getIndexWriter() {
            return indexWriter;
        }

        public SearcherManager getSearcherManager() {
            return searcherManager;
        }
    }

    static class TransportService {
        private Indexer indexer;
        private Searcher searcher;

        public TransportService(Indexer indexer, Searcher searcher) {
            this.indexer = indexer;
            this.searcher = searcher;
        }

        public void start() {

        }

        public void index(String indexRequest) throws IOException {
            Document doc = new Document();
            doc.add(new TextField("content", indexRequest, Field.Store.YES));
            indexer.index(doc);
        }

        public TopDocs search(String searchRequest) throws IOException {
            return searcher.search(searchRequest);
        }
    }

    static class ServerBootstrap {

        public void start() throws IOException {
            Admin admin = new Admin();
            IndexRefresher indexRefresher = new IndexRefresher(admin.getSearcherManager());
            indexRefresher.start();

            Indexer indexer = new Indexer(admin.getIndexWriter());
            Searcher searcher = new Searcher(admin.getSearcherManager());

            TransportService transportService = new TransportService(indexer, searcher);
            transportService.start();
        }

    }

    public static void main(String[] args) throws IOException {
        new ServerBootstrap().start();
    }

}
