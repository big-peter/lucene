package org.apache.lucene.demo;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

/**
 * @author BigPeter
 * @date 2021-03-30 13:52
 */
public class MyIndex {

    public static void main(String[] args) throws IOException {
        String[] rawDocs = new String[]{
                "book book is",
                "book",
                "book that",
                ""
        };

        String indexPath = "./resources/my/index";
        Directory directory = FSDirectory.open(Paths.get(indexPath));

        for (String fileName : directory.listAll()) {
            directory.deleteFile(fileName);
        }

        PayloadAnalyzer analyzer = new PayloadAnalyzer();
        analyzer.setPayloadData("content", "hi".getBytes(StandardCharsets.UTF_8), 0, 2);

        IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setUseCompoundFile(false);

        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        fieldType.setTokenized(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        fieldType.freeze();

        try (IndexWriter indexWriter = new IndexWriter(directory, iwc)) {
            for (int i=0; i < rawDocs.length; i+=2) {
                Document document = new Document();
                document.add(new Field("content", rawDocs[i], fieldType));
                document.add(new Field("title", rawDocs[i + 1], fieldType));

                indexWriter.addDocument(document);
            }

            indexWriter.commit();

            System.out.println("wait...");
        }

        System.out.println("wait...");
    }

}
