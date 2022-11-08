package org.apache.lucene.demo;

import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.*;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * @author BigPeter
 * @date 2021-03-30 13:52
 */
public class MyIndex {

    public static void osDemo(String[] args) throws IOException {
        // raw java
        FileOutputStream fos = new FileOutputStream("./resources/my/osdemo/file1.txt");
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        bos.write("Hello World".getBytes(StandardCharsets.UTF_8), 0, 11);
        bos.flush();
        fos.getFD().sync();
        // fos.getChannel().force(true);  或者该方法
        bos.close();

        // lucene
        String indexPath = "./resources/my/osdemo";
        Directory directory = new TrackingDirectoryWrapper(FSDirectory.open(Paths.get(indexPath)));
        IndexOutput indexOutput = directory.createOutput("file.txt", IOContext.DEFAULT);
        indexOutput.writeBytes("Hello World".getBytes(StandardCharsets.UTF_8), 11);
        indexOutput.close();    // indexOutput.os.flush()，只保证flush到操作系统缓存，不保证写到磁盘
        directory.sync(Collections.singleton(indexOutput.getName()));  // sync 到磁盘，内部会调用FileChannel.force方法
    }

    public static void main(String[] args) throws IOException {
        String[] rawDocs = new String[]{
                "book book is",
                "book",
                "book that",
                "apple",
                "learning lucene and elasticsearch",
                "topic"
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
        iwc.setCodec(new SimpleTextCodec());

        FieldType fieldType = new FieldType();
        fieldType.setOmitNorms(false);

        fieldType.setStored(true);

        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setStoreTermVectorPayloads(true);
        fieldType.setStoreTermVectorOffsets(true);

//        fieldType.setDocValuesType(DocValuesType.NUMERIC);
        fieldType.setTokenized(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        fieldType.freeze();

        FieldType sortType = new FieldType();
        sortType.setOmitNorms(false);

        sortType.setStored(true);

        sortType.setStoreTermVectors(true);
        sortType.setStoreTermVectorPositions(true);
        sortType.setStoreTermVectorPayloads(true);
        sortType.setStoreTermVectorOffsets(true);

        sortType.setDocValuesType(DocValuesType.BINARY);
        sortType.setTokenized(false);
        sortType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        sortType.freeze();



        try (IndexWriter indexWriter = new IndexWriter(directory, iwc)) {
            List<CompletableFuture<Void>> allFutures = new ArrayList<>(2);
            for (int i=0; i < rawDocs.length; i+=2) {
                Document document = new Document();
                document.add(new Field("content", rawDocs[i], fieldType));
                document.add(new Field("title", rawDocs[i + 1].getBytes(StandardCharsets.UTF_8), sortType));

//                allFutures.add(CompletableFuture.runAsync(() -> {
//                    try {
//                        indexWriter.addDocument(document);
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                }));

                indexWriter.addDocument(document);
            }

            CompletableFuture.allOf(allFutures.toArray(new CompletableFuture[0])).join();
            indexWriter.flush();

            indexWriter.commit();

            System.out.println("wait...");
        }

        System.out.println("wait...");
    }

}
