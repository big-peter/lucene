package org.apache.lucene.demo;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;

import java.io.IOException;
import java.util.Comparator;

/**
 * @Author: kolt.wang@zoom.us
 * @ModuleOwner: harris.yang@zoom.us
 * @Date: 10/13/2022
 * @Description:
 */
public class FSTDemo {

    public static void officialDemo() throws IOException {
        // Input values (keys). These must be provided to Builder in Unicode code point (UTF8 or UTF32) sorted order.
        // Note that sorting by Java's String.compareTo, which is UTF16 sorted order, is not correct and can lead to
        // exceptions while building the FST:
        String inputValues[] = {"cat", "dog", "dogs"};
        long outputValues[] = {5, 7, 12};

        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        FSTCompiler<Long> fstCompiler = new FSTCompiler<Long>(FST.INPUT_TYPE.BYTE1, outputs);
        BytesRefBuilder scratchBytes = new BytesRefBuilder();
        IntsRefBuilder scratchInts = new IntsRefBuilder();
        for (int i = 0; i < inputValues.length; i++) {
            scratchBytes.copyChars(inputValues[i]);
            fstCompiler.add(Util.toIntsRef(scratchBytes.toBytesRef(), scratchInts), outputValues[i]);
        }
        FST<Long> fst = fstCompiler.compile();

        // Retrieval by key:
        Long value = Util.get(fst, new BytesRef("dog"));
        System.out.println(value); // 7

        // Retrieval by value: [deprecated]
        // Only works because outputs are also in sorted order
        // IntsRef key = Util.getByOutput(fst, 12);
        // System.out.println(Util.toBytesRef(key, scratchBytes).utf8ToString()); // dogs

        // Iterate over key-value pairs in sorted order:
        // Like TermsEnum, this also supports seeking (advance)
        BytesRefFSTEnum<Long> iterator = new BytesRefFSTEnum<Long>(fst);
        while (iterator.next() != null) {
            BytesRefFSTEnum.InputOutput<Long> mapEntry = iterator.current();
            System.out.println(mapEntry.input.utf8ToString());
            System.out.println(mapEntry.output);
        }

        // N-shortest paths by weight:
        Comparator<Long> comparator = new Comparator<Long>() {
            public int compare(Long left, Long right) {
                return left.compareTo(right);
            }
        };
        FST.Arc<Long> firstArc = fst.getFirstArc(new FST.Arc<Long>());
        Util.TopResults<Long> paths = Util.shortestPaths(fst, firstArc, fst.outputs.getNoOutput(), comparator, 3, true);
        System.out.println(Util.toBytesRef(paths.topN.get(0).input, scratchBytes).utf8ToString()); // cat
        System.out.println(paths.topN.get(0).output); // 5
        System.out.println(Util.toBytesRef(paths.topN.get(1).input, scratchBytes).utf8ToString()); // dog
        System.out.println(paths.topN.get(1).output); // 7
    }

    public static void myDemo() throws IOException {
        String[] inputValues = {"mo", "moth", "pop", "star", "stop", "top"};
        long[] outputValues = {100, 91, 72, 83, 54, 55};

        PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
        FSTCompiler<Long> fstCompiler = new FSTCompiler<Long>(FST.INPUT_TYPE.BYTE1, outputs);
        BytesRefBuilder scratchBytes = new BytesRefBuilder();
        IntsRefBuilder scratchInts = new IntsRefBuilder();
        for (int i = 0; i < inputValues.length; i++) {
            scratchBytes.copyChars(inputValues[i]);
            fstCompiler.add(Util.toIntsRef(scratchBytes.toBytesRef(), scratchInts), outputValues[i]);
        }
        FST<Long> fst = fstCompiler.compile();

        System.out.println(Util.get(fst, new BytesRef("star")));
    }

    public static void main(String[] args) throws IOException {
//        officialDemo();
        myDemo();
    }

}
