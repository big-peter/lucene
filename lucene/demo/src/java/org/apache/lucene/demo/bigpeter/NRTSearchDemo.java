package org.apache.lucene.demo.bigpeter;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.*;

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
        private QueryParser queryParser;  // text search

        public Searcher(SearcherManager searcherManager) {
            this.searcherManager = searcherManager;
            this.queryParser = new QueryParser("key", new StandardAnalyzer());
        }

        public TopDocs search(String rawQuery) throws IOException, ParseException {
//            Query query = queryParser.parse(rawQuery);
            Term term = new Term("key", rawQuery);
            Query query = new TermQuery(term);
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
                    System.out.println("Start fresh...");
                    searcherManager.maybeRefresh();
                    System.out.println("Finish fresh...");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }, 0, 10, TimeUnit.SECONDS);
        }

    }

    static class Admin {

        private IndexWriter indexWriter;
        private SearcherManager searcherManager;

        public Admin() throws IOException {
            Directory directory = FSDirectory.open(Paths.get("./resources/nrt_search"));
            IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
            iwc.setUseCompoundFile(false);
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
            doc.add(new StringField("key", indexRequest, Field.Store.YES));
            indexer.index(doc);
        }

        public TopDocs search(String searchRequest) throws IOException, ParseException {
            return searcher.search(searchRequest);
        }
    }

    static class ServerBootstrap {

        private TransportService transportService;

        public void start() throws IOException {
            Admin admin = new Admin();
            IndexRefresher indexRefresher = new IndexRefresher(admin.getSearcherManager());
            indexRefresher.start();

            Indexer indexer = new Indexer(admin.getIndexWriter());
            Searcher searcher = new Searcher(admin.getSearcherManager());

            this.transportService = new TransportService(indexer, searcher);
            this.transportService.start();
        }

        public TransportService getTransportService() {
            return transportService;
        }
    }

    public static void main(String[] args) throws IOException, ParseException {
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        serverBootstrap.start();

        TransportService transportService = serverBootstrap.getTransportService();

        ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new SynchronousQueue<>());
        executorService.execute(() -> {
            int i = 0;
            while (! Thread.currentThread().isInterrupted()) {
                try {
                    System.out.println("Start index key: " + i);
                    transportService.index(String.valueOf(i++));
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        while (true) {
            System.out.println("Please input query:");
            String line = in.readLine();

            TopDocs topDocs = transportService.search(line);
            System.out.println(topDocs.totalHits);
        }
    }

}
