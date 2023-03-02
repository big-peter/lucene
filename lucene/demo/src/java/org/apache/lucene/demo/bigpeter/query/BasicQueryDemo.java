package org.apache.lucene.demo.bigpeter.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;

public class BasicQueryDemo {

    private static void index() throws IOException {
        try (Directory dir = FSDirectory.open(Paths.get("./resources/search_demo"));
             Analyzer analyzer = new WhitespaceAnalyzer()) {
            for (String fileName : dir.listAll()) {
                dir.deleteFile(fileName);
            }

            IndexWriterConfig iwf = new IndexWriterConfig(analyzer);
            IndexWriter indexWriter = new IndexWriter(dir, iwf);

            Document doc = new Document();
            doc.add(new StringField("title", "doc0", Field.Store.YES));
            doc.add(new TextField("content", "a b", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", 5));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc1", Field.Store.YES));
            doc.add(new TextField("content", "a c d", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", 4));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc2", Field.Store.YES));
            doc.add(new TextField("content", "d e", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", 3));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc3", Field.Store.YES));
            doc.add(new TextField("content", "b f", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", 2));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc4", Field.Store.YES));
            doc.add(new TextField("content", "g", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", 1));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc5", Field.Store.YES));
            doc.add(new TextField("content", "k", Field.Store.YES));
            indexWriter.addDocument(doc);

            doc = new Document();
            doc.add(new StringField("title", "doc6", Field.Store.YES));
            doc.add(new TextField("content", "e g", Field.Store.YES));
            doc.add(new NumericDocValuesField("orderByNumber", -1));
            indexWriter.addDocument(doc);

            indexWriter.commit();
        }
    }

    private static void search() throws IOException {
        try (Directory dir = FSDirectory.open(Paths.get("./resources/search_demo"))) {
            DirectoryReader reader = DirectoryReader.open(dir);
            IndexSearcher indexSearcher = new IndexSearcher(reader);

            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new TermQuery(new Term("content", "a")), BooleanClause.Occur.SHOULD);
            builder.add(new TermQuery(new Term("content", "b")), BooleanClause.Occur.SHOULD);
            builder.add(new TermQuery(new Term("content", "c")), BooleanClause.Occur.MUST_NOT);
            builder.setMinimumNumberShouldMatch(1);
            Query booleanQuery = builder.build();

            SortField sortField = new SortedNumericSortField("orderByNumber", SortField.Type.LONG);
            Sort sort = new Sort(sortField);
            TopFieldDocs topDocs = indexSearcher.search(booleanQuery, 10, sort);
            System.out.println(topDocs);
        }
    }

    public static void main(String[] args) throws IOException {
        index();
        search();
    }

}
