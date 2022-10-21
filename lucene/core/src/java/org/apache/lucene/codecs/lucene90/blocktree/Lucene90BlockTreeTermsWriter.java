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
package org.apache.lucene.codecs.lucene90.blocktree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.compress.LowercaseAsciiCompression;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.Util;

/*
  TODO:

    - Currently there is a one-to-one mapping of indexed
      term to term block, but we could decouple the two, ie,
      put more terms into the index than there are blocks.
      The index would take up more RAM but then it'd be able
      to avoid seeking more often and could make PK/FuzzyQ
      faster if the additional indexed terms could store
      the offset into the terms block.

    - The blocks are not written in true depth-first
      order, meaning if you just next() the file pointer will
      sometimes jump backwards.  For example, block foo* will
      be written before block f* because it finished before.
      This could possibly hurt performance if the terms dict is
      not hot, since OSs anticipate sequential file access.  We
      could fix the writer to re-order the blocks as a 2nd
      pass.

    - Each block encodes the term suffixes packed
      sequentially using a separate vInt per term, which is
      1) wasteful and 2) slow (must linear scan to find a
      particular suffix).  We should instead 1) make
      random-access array so we can directly access the Nth
      suffix, and 2) bulk-encode this array using bulk int[]
      codecs; then at search time we can binary search when
      we seek a particular term.
*/

/**
 * Block-based terms index and dictionary writer.
 *
 * <p>Writes terms dict and index, block-encoding (column stride) each term's metadata for each set
 * of terms between two index terms.
 *
 * <p>Files:
 *
 * <ul>
 *   <li><code>.tim</code>: <a href="#Termdictionary">Term Dictionary</a>
 *   <li><code>.tmd</code>: <a href="#Termmetadata">Term Metadata</a>
 *   <li><code>.tip</code>: <a href="#Termindex">Term Index</a>
 * </ul>
 *
 * <p><a id="Termdictionary"></a>
 *
 * <h2>Term Dictionary</h2>
 *
 * <p>The .tim file contains the list of terms in each field along with per-term statistics (such as
 * docfreq) and per-term metadata (typically pointers to the postings list for that term in the
 * inverted index).
 *
 * <p>The .tim is arranged in blocks: with blocks containing a variable number of entries (by
 * default 25-48), where each entry is either a term or a reference to a sub-block.
 *
 * <p>NOTE: The term dictionary can plug into different postings implementations: the postings
 * writer/reader are actually responsible for encoding and decoding the Postings Metadata and Term
 * Metadata sections.
 *
 * <ul>
 *   <li>TermsDict (.tim) --&gt; Header, FieldDict<sup>NumFields</sup>, Footer
 *   <li>FieldDict --&gt; <i>PostingsHeader</i>, NodeBlock<sup>NumBlocks</sup>
 *   <li>NodeBlock --&gt; (OuterNode | InnerNode)
 *   <li>OuterNode --&gt; EntryCount, SuffixLength, Byte<sup>SuffixLength</sup>, StatsLength, &lt;
 *       TermStats &gt;<sup>EntryCount</sup>, MetaLength,
 *       &lt;<i>TermMetadata</i>&gt;<sup>EntryCount</sup>
 *   <li>InnerNode --&gt; EntryCount, SuffixLength[,Sub?], Byte<sup>SuffixLength</sup>, StatsLength,
 *       &lt; TermStats ? &gt;<sup>EntryCount</sup>, MetaLength, &lt;<i>TermMetadata ?
 *       </i>&gt;<sup>EntryCount</sup>
 *   <li>TermStats --&gt; DocFreq, TotalTermFreq
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *   <li>EntryCount,SuffixLength,StatsLength,DocFreq,MetaLength --&gt; {@link DataOutput#writeVInt
 *       VInt}
 *   <li>TotalTermFreq --&gt; {@link DataOutput#writeVLong VLong}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>Header is a {@link CodecUtil#writeHeader CodecHeader} storing the version information for
 *       the BlockTree implementation.
 *   <li>DocFreq is the count of documents which contain the term.
 *   <li>TotalTermFreq is the total number of occurrences of the term. This is encoded as the
 *       difference between the total number of occurrences and the DocFreq.
 *   <li>PostingsHeader and TermMetadata are plugged into by the specific postings implementation:
 *       these contain arbitrary per-file data (such as parameters or versioning information) and
 *       per-term data (such as pointers to inverted files).
 *   <li>For inner nodes of the tree, every entry will steal one bit to mark whether it points to
 *       child nodes(sub-block). If so, the corresponding TermStats and TermMetaData are omitted.
 * </ul>
 *
 * <p><a id="Termmetadata"></a>
 *
 * <h2>Term Metadata</h2>
 *
 * <p>The .tmd file contains the list of term metadata (such as FST index metadata) and field level
 * statistics (such as sum of total term freq).
 *
 * <ul>
 *   <li>TermsMeta (.tmd) --&gt; Header, NumFields, &lt;FieldStats&gt;<sup>NumFields</sup>,
 *       TermIndexLength, TermDictLength, Footer
 *   <li>FieldStats --&gt; FieldNumber, NumTerms, RootCodeLength, Byte<sup>RootCodeLength</sup>,
 *       SumTotalTermFreq?, SumDocFreq, DocCount, MinTerm, MaxTerm, IndexStartFP, FSTHeader,
 *       <i>FSTMetadata</i>
 *   <li>Header,FSTHeader --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *   <li>TermIndexLength, TermDictLength --&gt; {@link DataOutput#writeLong Uint64}
 *   <li>MinTerm,MaxTerm --&gt; {@link DataOutput#writeVInt VInt} length followed by the byte[]
 *   <li>NumFields,FieldNumber,RootCodeLength,DocCount --&gt; {@link DataOutput#writeVInt VInt}
 *   <li>NumTerms,SumTotalTermFreq,SumDocFreq,IndexStartFP --&gt; {@link DataOutput#writeVLong
 *       VLong}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>FieldNumber is the fields number from {@link FieldInfos}. (.fnm)
 *   <li>NumTerms is the number of unique terms for the field.
 *   <li>RootCode points to the root block for the field.
 *   <li>SumDocFreq is the total number of postings, the number of term-document pairs across the
 *       entire field.
 *   <li>DocCount is the number of documents that have at least one posting for this field.
 *   <li>MinTerm, MaxTerm are the lowest and highest term in this field.
 * </ul>
 *
 * <a id="Termindex"></a>
 *
 * <h2>Term Index</h2>
 *
 * <p>The .tip file contains an index into the term dictionary, so that it can be accessed randomly.
 * The index is also used to determine when a given term cannot exist on disk (in the .tim file),
 * saving a disk seek.
 *
 * <ul>
 *   <li>TermsIndex (.tip) --&gt; Header, FSTIndex<sup>NumFields</sup>Footer
 *   <li>Header --&gt; {@link CodecUtil#writeHeader CodecHeader}
 *       <!-- TODO: better describe FST output here -->
 *   <li>FSTIndex --&gt; {@link FST FST&lt;byte[]&gt;}
 *   <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 * </ul>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>The .tip file contains a separate FST for each field. The FST maps a term prefix to the
 *       on-disk block that holds all terms starting with that prefix. Each field's IndexStartFP
 *       points to its FST.
 *   <li>It's possible that an on-disk block would contain too many terms (more than the allowed
 *       maximum (default: 48)). When this happens, the block is sub-divided into new blocks (called
 *       "floor blocks"), and then the output in the FST for the block's prefix encodes the leading
 *       byte of each sub-block, and its file pointer.
 * </ul>
 *
 * @see Lucene90BlockTreeTermsReader
 * @lucene.experimental
 */
public final class Lucene90BlockTreeTermsWriter extends FieldsConsumer {

  /**
   * Suggested default value for the {@code minItemsInBlock} parameter to {@link
   * #Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MIN_BLOCK_SIZE = 25;

  /**
   * Suggested default value for the {@code maxItemsInBlock} parameter to {@link
   * #Lucene90BlockTreeTermsWriter(SegmentWriteState,PostingsWriterBase,int,int)}.
   */
  public static final int DEFAULT_MAX_BLOCK_SIZE = 48;

  // public static boolean DEBUG = false;
  // public static boolean DEBUG2 = false;

  // private final static boolean SAVE_DOT_FILES = false;

  private final IndexOutput metaOut;  // terms meta file, .tmd
  private final IndexOutput termsOut; // terms file, .tim
  private final IndexOutput indexOut;  // term index file, .tip
  final int maxDoc;
  final int minItemsInBlock;
  final int maxItemsInBlock;

  final PostingsWriterBase postingsWriter;
  final FieldInfos fieldInfos;

  private final List<ByteBuffersDataOutput> fields = new ArrayList<>();

  /**
   * Create a new writer. The number of items (terms or sub-blocks) per block will aim to be between
   * minItemsPerBlock and maxItemsPerBlock, though in some cases the blocks may be smaller than the
   * min.
   */
  public Lucene90BlockTreeTermsWriter(
      SegmentWriteState state,
      PostingsWriterBase postingsWriter,
      int minItemsInBlock,
      int maxItemsInBlock)
      throws IOException {
    validateSettings(minItemsInBlock, maxItemsInBlock);

    this.minItemsInBlock = minItemsInBlock;
    this.maxItemsInBlock = maxItemsInBlock;

    this.maxDoc = state.segmentInfo.maxDoc();
    this.fieldInfos = state.fieldInfos;
    this.postingsWriter = postingsWriter;

    final String termsName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene90BlockTreeTermsReader.TERMS_EXTENSION);
    termsOut = state.directory.createOutput(termsName, state.context);
    boolean success = false;
    IndexOutput metaOut = null, indexOut = null;
    try {
      CodecUtil.writeIndexHeader(
          termsOut,
          Lucene90BlockTreeTermsReader.TERMS_CODEC_NAME,
          Lucene90BlockTreeTermsReader.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      final String indexName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name,
              state.segmentSuffix,
              Lucene90BlockTreeTermsReader.TERMS_INDEX_EXTENSION);
      indexOut = state.directory.createOutput(indexName, state.context);
      CodecUtil.writeIndexHeader(
          indexOut,
          Lucene90BlockTreeTermsReader.TERMS_INDEX_CODEC_NAME,
          Lucene90BlockTreeTermsReader.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      // segment = state.segmentInfo.name;

      final String metaName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name,
              state.segmentSuffix,
              Lucene90BlockTreeTermsReader.TERMS_META_EXTENSION);
      metaOut = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          metaOut,
          Lucene90BlockTreeTermsReader.TERMS_META_CODEC_NAME,
          Lucene90BlockTreeTermsReader.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);

      postingsWriter.init(metaOut, state); // have consumer write its format/header

      this.metaOut = metaOut;
      this.indexOut = indexOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut);
      }
    }
  }

  /** Throws {@code IllegalArgumentException} if any of these settings is invalid. */
  public static void validateSettings(int minItemsInBlock, int maxItemsInBlock) {
    if (minItemsInBlock <= 1) {
      throw new IllegalArgumentException("minItemsInBlock must be >= 2; got " + minItemsInBlock);
    }
    if (minItemsInBlock > maxItemsInBlock) {
      throw new IllegalArgumentException(
          "maxItemsInBlock must be >= minItemsInBlock; got maxItemsInBlock="
              + maxItemsInBlock
              + " minItemsInBlock="
              + minItemsInBlock);
    }
    if (2 * (minItemsInBlock - 1) > maxItemsInBlock) {
      throw new IllegalArgumentException(
          "maxItemsInBlock must be at least 2*(minItemsInBlock-1); got maxItemsInBlock="
              + maxItemsInBlock
              + " minItemsInBlock="
              + minItemsInBlock);
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    // if (DEBUG) System.out.println("\nBTTW.write seg=" + segment);

    String lastField = null;
    for (String field : fields) {
      assert lastField == null || lastField.compareTo(field) < 0;
      lastField = field;

      // if (DEBUG) System.out.println("\nBTTW.write seg=" + segment + " field=" + field);
      Terms terms = fields.terms(field);
      if (terms == null) {
        continue;
      }

      // 将该Field的所有term的信息依次写入.doc, .pos, .pay文件
      TermsEnum termsEnum = terms.iterator();
      TermsWriter termsWriter = new TermsWriter(fieldInfos.fieldInfo(field));
      while (true) {
        BytesRef term = termsEnum.next();
        // if (DEBUG) System.out.println("BTTW: next term " + term);

        if (term == null) {
          break;
        }

        // if (DEBUG) System.out.println("write field=" + fieldInfo.name + " term=" +
        // brToString(term));
        termsWriter.write(term, termsEnum, norms);
      }

      termsWriter.finish();  // 写入该field中的所有词项信息，直接写.tim,.tip文件，将tmd文件的信息保存在termsWriter的fields列表中，close的时候会写入.tmd文件

      // if (DEBUG) System.out.println("\nBTTW.write done seg=" + segment + " field=" + field);
    }
  }

  static long encodeOutput(long fp, boolean hasTerms, boolean isFloor) {
    assert fp < (1L << 62);
    return (fp << 2)
        | (hasTerms ? Lucene90BlockTreeTermsReader.OUTPUT_FLAG_HAS_TERMS : 0)
        | (isFloor ? Lucene90BlockTreeTermsReader.OUTPUT_FLAG_IS_FLOOR : 0);
  }

  private static class PendingEntry {
    public final boolean isTerm;

    protected PendingEntry(boolean isTerm) {
      this.isTerm = isTerm;
    }
  }

  private static final class PendingTerm extends PendingEntry {
    public final byte[] termBytes;
    // stats + metadata
    public final BlockTermState state;

    public PendingTerm(BytesRef term, BlockTermState state) {
      super(true);
      this.termBytes = new byte[term.length];
      System.arraycopy(term.bytes, term.offset, termBytes, 0, term.length);
      this.state = state;
    }

    @Override
    public String toString() {
      return "TERM: " + brToString(termBytes);
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(BytesRef b) {
    if (b == null) {
      return "(null)";
    } else {
      try {
        return b.utf8ToString() + " " + b;
      } catch (Throwable t) {
        // If BytesRef isn't actually UTF8, or it's eg a
        // prefix of UTF8 that ends mid-unicode-char, we
        // fallback to hex:
        return b.toString();
      }
    }
  }

  // for debugging
  @SuppressWarnings("unused")
  static String brToString(byte[] b) {
    return brToString(new BytesRef(b));
  }

  private static final class PendingBlock extends PendingEntry {
    public final BytesRef prefix;
    public final long fp;
    public FST<BytesRef> index;
    public List<FST<BytesRef>> subIndices;
    public final boolean hasTerms;
    public final boolean isFloor;
    public final int floorLeadByte;

    public PendingBlock(
        BytesRef prefix,
        long fp,
        boolean hasTerms,
        boolean isFloor,
        int floorLeadByte,
        List<FST<BytesRef>> subIndices) {
      super(false);
      this.prefix = prefix;
      this.fp = fp;
      this.hasTerms = hasTerms;
      this.isFloor = isFloor;
      this.floorLeadByte = floorLeadByte;
      this.subIndices = subIndices;
    }

    @Override
    public String toString() {
      return "BLOCK: prefix=" + brToString(prefix);
    }

    public void compileIndex(
        List<PendingBlock> blocks,
        ByteBuffersDataOutput scratchBytes,
        IntsRefBuilder scratchIntsRef)
        throws IOException {

      assert (isFloor && blocks.size() > 1) || (isFloor == false && blocks.size() == 1)
          : "isFloor=" + isFloor + " blocks=" + blocks;
      assert this == blocks.get(0);

      assert scratchBytes.size() == 0;

      // TODO: try writing the leading vLong in MSB order
      // (opposite of what Lucene does today), for better
      // outputs sharing in the FST
      scratchBytes.writeVLong(encodeOutput(fp, hasTerms, isFloor));
      if (isFloor) {
        scratchBytes.writeVInt(blocks.size() - 1);
        for (int i = 1; i < blocks.size(); i++) {
          PendingBlock sub = blocks.get(i);
          assert sub.floorLeadByte != -1;
          // if (DEBUG) {
          //  System.out.println("    write floorLeadByte=" +
          // Integer.toHexString(sub.floorLeadByte&0xff));
          // }
          scratchBytes.writeByte((byte) sub.floorLeadByte);
          assert sub.fp > fp;
          scratchBytes.writeVLong((sub.fp - fp) << 1 | (sub.hasTerms ? 1 : 0));
        }
      }

      final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
      final FSTCompiler<BytesRef> fstCompiler =
          new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs)
              .shouldShareNonSingletonNodes(false)
              .build();
      // if (DEBUG) {
      //  System.out.println("  compile index for prefix=" + prefix);
      // }
      // indexBuilder.DEBUG = false;
      final byte[] bytes = scratchBytes.toArrayCopy();
      assert bytes.length > 0;
      fstCompiler.add(Util.toIntsRef(prefix, scratchIntsRef), new BytesRef(bytes, 0, bytes.length));  // 添加映射 prefix -> [fp, hasTerms, isFloor]
      scratchBytes.reset();

      // Copy over index for all sub-blocks
      for (PendingBlock block : blocks) {  // 如果block有子切片，添加block的子切片
        if (block.subIndices != null) {
          for (FST<BytesRef> subIndex : block.subIndices) {
            append(fstCompiler, subIndex, scratchIntsRef);
          }
          block.subIndices = null;
        }
      }

      index = fstCompiler.compile();  // FST构造完成

      assert subIndices == null;

      /*
      Writer w = new OutputStreamWriter(new FileOutputStream("out.dot"));
      Util.toDot(index, w, false, false);
      System.out.println("SAVED to out.dot");
      w.close();
      */
    }

    // TODO: maybe we could add bulk-add method to
    // Builder?  Takes FST and unions it w/ current
    // FST.
    private void append(
        FSTCompiler<BytesRef> fstCompiler, FST<BytesRef> subIndex, IntsRefBuilder scratchIntsRef)
        throws IOException {
      final BytesRefFSTEnum<BytesRef> subIndexEnum = new BytesRefFSTEnum<>(subIndex);
      BytesRefFSTEnum.InputOutput<BytesRef> indexEnt;
      while ((indexEnt = subIndexEnum.next()) != null) {
        // if (DEBUG) {
        //  System.out.println("      add sub=" + indexEnt.input + " " + indexEnt.input + " output="
        // + indexEnt.output);
        // }
        fstCompiler.add(Util.toIntsRef(indexEnt.input, scratchIntsRef), indexEnt.output);
      }
    }
  }

  private final ByteBuffersDataOutput scratchBytes = ByteBuffersDataOutput.newResettableInstance();
  private final IntsRefBuilder scratchIntsRef = new IntsRefBuilder();

  static final BytesRef EMPTY_BYTES_REF = new BytesRef();

  private static class StatsWriter {

    private final DataOutput out;
    private final boolean hasFreqs;
    private int singletonCount;

    StatsWriter(DataOutput out, boolean hasFreqs) {
      this.out = out;
      this.hasFreqs = hasFreqs;
    }

    void add(int df, long ttf) throws IOException {
      // Singletons (DF==1, TTF==1) are run-length encoded
      if (df == 1 && (hasFreqs == false || ttf == 1)) {
        singletonCount++;
      } else {
        finish();
        out.writeVInt(df << 1);
        if (hasFreqs) {
          out.writeVLong(ttf - df);
        }
      }
    }

    void finish() throws IOException {
      if (singletonCount > 0) {
        out.writeVInt(((singletonCount - 1) << 1) | 1);
        singletonCount = 0;
      }
    }
  }

  // 每个Field一个TermsWriter对象
  class TermsWriter {
    private final FieldInfo fieldInfo;
    private long numTerms;
    final FixedBitSet docsSeen;
    long sumTotalTermFreq;
    long sumDocFreq;

    // Records index into pending where the current prefix at that
    // length "started"; for example, if current term starts with 't',
    // startsByPrefix[0] is the index into pending for the first
    // term/sub-block starting with 't'.  We use this to figure out when
    // to write a new block:
    private final BytesRefBuilder lastTerm = new BytesRefBuilder();
    private int[] prefixStarts = new int[8];

    // Pending stack of terms and blocks.  As terms arrive (in sorted order)
    // we append to this stack, and once the top of the stack has enough
    // terms starting with a common prefix, we write a new block with
    // those terms and replace those terms in the stack with a new block:
    private final List<PendingEntry> pending = new ArrayList<>();

    // Reused in writeBlocks:
    private final List<PendingBlock> newBlocks = new ArrayList<>();

    private PendingTerm firstPendingTerm;
    private PendingTerm lastPendingTerm;

    // 生成NodeBlock。可能会生成多个PendingBlock
    /** Writes the top count entries in pending, using prevTerm to compute the prefix. */
    void writeBlocks(int prefixLength, int count) throws IOException {

      assert count > 0;

      // if (DEBUG2) {
      //  BytesRef br = new BytesRef(lastTerm.bytes());
      //  br.length = prefixLength;
      //  System.out.println("writeBlocks: seg=" + segment + " prefix=" + brToString(br) + " count="
      // + count);
      // }

      // Root block better write all remaining pending entries:
      assert prefixLength > 0 || count == pending.size();

      int lastSuffixLeadLabel = -1;

      // True if we saw at least one term in this block (we record if a block
      // only points to sub-blocks in the terms index so we can avoid seeking
      // to it when we are looking for a term):
      boolean hasTerms = false;
      boolean hasSubBlocks = false;

      int start = pending.size() - count;
      int end = pending.size();
      int nextBlockStart = start;
      int nextFloorLeadLabel = -1;

      // 生成PendingBlock
      for (int i = start; i < end; i++) {

        PendingEntry ent = pending.get(i);

        int suffixLeadLabel;

        if (ent.isTerm) {
          PendingTerm term = (PendingTerm) ent;
          if (term.termBytes.length == prefixLength) {
            // Suffix is 0, i.e. prefix 'foo' and term is
            // 'foo' so the term has empty string suffix
            // in this block
            assert lastSuffixLeadLabel == -1
                : "i=" + i + " lastSuffixLeadLabel=" + lastSuffixLeadLabel;
            suffixLeadLabel = -1;
          } else {
            suffixLeadLabel = term.termBytes[prefixLength] & 0xff;  // 拿到后缀开始的字节
          }
        } else {
          PendingBlock block = (PendingBlock) ent;
          assert block.prefix.length > prefixLength;
          suffixLeadLabel = block.prefix.bytes[block.prefix.offset + prefixLength] & 0xff;
        }
        // if (DEBUG) System.out.println("  i=" + i + " ent=" + ent + " suffixLeadLabel=" +
        // suffixLeadLabel);

        if (suffixLeadLabel != lastSuffixLeadLabel) {
          int itemsInBlock = i - nextBlockStart;
          if (itemsInBlock >= minItemsInBlock && end - nextBlockStart > maxItemsInBlock) {
            // The count is too large for one block, so we must break it into "floor" blocks, where
            // we record
            // the leading label of the suffix of the first term in each floor block, so at search
            // time we can
            // jump to the right floor block.  We just use a naive greedy segmenter here: make a new
            // floor
            // block as soon as we have at least minItemsInBlock.  This is not always best: it often
            // produces
            // a too-small block as the final block:
            // 太多了，划分成多个floor block
            boolean isFloor = itemsInBlock < count;
            // 合并成PendingBlock，写.tim文件，并将合并的block加入newBlocks列表
            newBlocks.add(
                writeBlock(
                    prefixLength,
                    isFloor,
                    nextFloorLeadLabel,
                    nextBlockStart,
                    i,
                    hasTerms,
                    hasSubBlocks));

            hasTerms = false;
            hasSubBlocks = false;
            nextFloorLeadLabel = suffixLeadLabel;
            nextBlockStart = i;
          }

          lastSuffixLeadLabel = suffixLeadLabel;
        }

        if (ent.isTerm) {
          hasTerms = true;
        } else {
          hasSubBlocks = true;
        }
      }

      // Write last block, if any:
      if (nextBlockStart < end) {  // 写入最后剩余的数据
        int itemsInBlock = end - nextBlockStart;
        boolean isFloor = itemsInBlock < count;
        newBlocks.add(
            writeBlock(
                prefixLength,
                isFloor,
                nextFloorLeadLabel,
                nextBlockStart,
                end,
                hasTerms,
                hasSubBlocks));
      }

      assert newBlocks.isEmpty() == false;

      PendingBlock firstBlock = newBlocks.get(0);

      assert firstBlock.isFloor || newBlocks.size() == 1;

      // 将生成的newBlocks构建成FST
      firstBlock.compileIndex(newBlocks, scratchBytes, scratchIntsRef);  // 构建该block的FST

      // 将已合并的PendingEntry从pending stack中移除
      // Remove slice from the top of the pending stack, that we just wrote:
      pending.subList(pending.size() - count, pending.size()).clear();

      // 将创建的block加入pending stack
      // Append new block
      pending.add(firstBlock);

      newBlocks.clear();
    }

    private boolean allEqual(byte[] b, int startOffset, int endOffset, byte value) {
      Objects.checkFromToIndex(startOffset, endOffset, b.length);
      for (int i = startOffset; i < endOffset; ++i) {
        if (b[i] != value) {
          return false;
        }
      }
      return true;
    }


    /**
     * Writes the specified slice (start is inclusive, end is exclusive) from pending stack as a new
     * block. If isFloor is true, there were too many (more than maxItemsInBlock) entries sharing
     * the same prefix, and so we broke it into multiple floor blocks where we record the starting
     * label of the suffix of each floor block.
     */
    // 写.tim文件，并生成PendingBlock
    private PendingBlock writeBlock(
        int prefixLength,
        boolean isFloor,
        int floorLeadLabel,
        int start,
        int end,
        boolean hasTerms,
        boolean hasSubBlocks)
        throws IOException {

      assert end > start;

      long startFP = termsOut.getFilePointer();

      boolean hasFloorLeadLabel = isFloor && floorLeadLabel != -1;

      final BytesRef prefix = new BytesRef(prefixLength + (hasFloorLeadLabel ? 1 : 0));
      System.arraycopy(lastTerm.get().bytes, 0, prefix.bytes, 0, prefixLength);
      prefix.length = prefixLength;

      // if (DEBUG2) System.out.println("    writeBlock field=" + fieldInfo.name + " prefix=" +
      // brToString(prefix) + " fp=" + startFP + " isFloor=" + isFloor + " isLastInFloor=" + (end ==
      // pending.size()) + " floorLeadLabel=" + floorLeadLabel + " start=" + start + " end=" + end +
      // " hasTerms=" + hasTerms + " hasSubBlocks=" + hasSubBlocks);

      // Write block header:
      int numEntries = end - start;
      int code = numEntries << 1;
      if (end == pending.size()) {
        // Last block:
        code |= 1;
      }

      // numEntriesCode
      termsOut.writeVInt(code);  // termsOut，.tim文件输出流

      /*
      if (DEBUG) {
        System.out.println("  writeBlock " + (isFloor ? "(floor) " : "") + "seg=" + segment + " pending.size()=" + pending.size() + " prefixLength=" + prefixLength + " indexPrefix=" + brToString(prefix) + " entCount=" + (end-start+1) + " startFP=" + startFP + (isFloor ? (" floorLeadLabel=" + Integer.toHexString(floorLeadLabel)) : ""));
      }
      */

      // 1st pass: pack term suffix bytes into byte[] blob
      // TODO: cutover to bulk int codec... simple64?

      // We optimize the leaf block case (block has only terms), writing a more
      // compact format in this case:
      boolean isLeafBlock = hasSubBlocks == false;

      // System.out.println("  isLeaf=" + isLeafBlock);

      final List<FST<BytesRef>> subIndices;

      boolean absolute = true;

      // 1. 先将整合的数据保存在内存中
      if (isLeafBlock) {
        // Block contains only ordinary terms:
        // 只包含PendingTerm
        subIndices = null;

        // 用来写docFreq，totalTermFreq
        // statsWriter类型是ByteBuffersDataOutput
        StatsWriter statsWriter =
            new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          assert ent.isTerm : "i=" + i;

          PendingTerm term = (PendingTerm) ent;

          assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
          BlockTermState state = term.state;
          final int suffix = term.termBytes.length - prefixLength;  // 后缀长度
          // if (DEBUG2) {
          //  BytesRef suffixBytes = new BytesRef(suffix);
          //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
          //  suffixBytes.length = suffix;
          //  System.out.println("    write term suffix=" + brToString(suffixBytes));
          // }

          // For leaf block we write suffix straight
          suffixLengthsWriter.writeVInt(suffix);  // ByteBuffersDataOutput，常见TermsWriter对象时创建
          suffixWriter.append(term.termBytes, prefixLength, suffix);
          assert floorLeadLabel == -1 || (term.termBytes[prefixLength] & 0xff) >= floorLeadLabel;

          // Write term stats, to separate byte[] blob:
          statsWriter.add(state.docFreq, state.totalTermFreq);

          // Write term meta data
          // metaWriter类型是ByteBuffersDataOutput
          postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);
          absolute = false;
        }

        // 如果singletonCount大于0，写入this.statsWriter
        statsWriter.finish();
      } else {
        // 至少有一个PendingBlock
        // Block has at least one prefix term or a sub block:
        subIndices = new ArrayList<>();
        StatsWriter statsWriter =
            new StatsWriter(this.statsWriter, fieldInfo.getIndexOptions() != IndexOptions.DOCS);
        for (int i = start; i < end; i++) {
          PendingEntry ent = pending.get(i);
          if (ent.isTerm) {
            // 如果是PendingTerm，处理逻辑和上面一样
            PendingTerm term = (PendingTerm) ent;

            assert StringHelper.startsWith(term.termBytes, prefix) : term + " prefix=" + prefix;
            BlockTermState state = term.state;
            final int suffix = term.termBytes.length - prefixLength;
            // if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(term.termBytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write term suffix=" + brToString(suffixBytes));
            // }

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block, and 1 bit to record if
            // it's a prefix term.  Terms cannot be larger than ~32 KB
            // so we won't run out of bits:

            suffixLengthsWriter.writeVInt(suffix << 1);  // 保存后缀长度
            suffixWriter.append(term.termBytes, prefixLength, suffix);  // 保存后缀内容

            // Write term stats, to separate byte[] blob:
            statsWriter.add(state.docFreq, state.totalTermFreq);  // 内存中保存文档频率，词频信息。如果只出现一次即文档频率，词频都为1，则特殊处理

            // TODO: now that terms dict "sees" these longs,
            // we can explore better column-stride encodings
            // to encode all long[0]s for this block at
            // once, all long[1]s, etc., e.g. using
            // Simple64.  Alternatively, we could interleave
            // stats + meta ... no reason to have them
            // separate anymore:

            // Write term meta data
            postingsWriter.encodeTerm(metaWriter, fieldInfo, state, absolute);  // 将.doc, .pay, .pos文件的偏移量信息写入到metaWriter内存中
            absolute = false;
          } else {
            // 如果是PendingBlock
            PendingBlock block = (PendingBlock) ent;
            assert StringHelper.startsWith(block.prefix, prefix);
            final int suffix = block.prefix.length - prefixLength;
            assert StringHelper.startsWith(block.prefix, prefix);

            assert suffix > 0;

            // For non-leaf block we borrow 1 bit to record
            // if entry is term or sub-block:f
            suffixLengthsWriter.writeVInt((suffix << 1) | 1);
            suffixWriter.append(block.prefix.bytes, prefixLength, suffix);

            // if (DEBUG2) {
            //  BytesRef suffixBytes = new BytesRef(suffix);
            //  System.arraycopy(block.prefix.bytes, prefixLength, suffixBytes.bytes, 0, suffix);
            //  suffixBytes.length = suffix;
            //  System.out.println("      write sub-block suffix=" + brToString(suffixBytes) + "
            // subFP=" + block.fp + " subCode=" + (startFP-block.fp) + " floor=" + block.isFloor);
            // }

            assert floorLeadLabel == -1
                    || (block.prefix.bytes[prefixLength] & 0xff) >= floorLeadLabel
                : "floorLeadLabel="
                    + floorLeadLabel
                    + " suffixLead="
                    + (block.prefix.bytes[prefixLength] & 0xff);
            assert block.fp < startFP;

            // startFP delta
            suffixLengthsWriter.writeVLong(startFP - block.fp);
            // 添加block.index
            subIndices.add(block.index);
          }
        }
        statsWriter.finish();

        assert subIndices.size() != 0;
      }

      // 2. 压缩
      // Write suffixes byte[] blob to terms dict output, either uncompressed, compressed with LZ4
      // or with LowercaseAsciiCompression.
      CompressionAlgorithm compressionAlg = CompressionAlgorithm.NO_COMPRESSION;
      // If there are 2 suffix bytes or less per term, then we don't bother compressing as suffix
      // are unlikely what
      // makes the terms dictionary large, and it also tends to be frequently the case for dense IDs
      // like
      // auto-increment IDs, so not compressing in that case helps not hurt ID lookups by too much.
      // We also only start compressing when the prefix length is greater than 2 since blocks whose
      // prefix length is
      // 1 or 2 always all get visited when running a fuzzy query whose max number of edits is 2.
      if (suffixWriter.length() > 2L * numEntries && prefixLength > 2) {
        // LZ4 inserts references whenever it sees duplicate strings of 4 chars or more, so only try
        // it out if the
        // average suffix length is greater than 6.
        if (suffixWriter.length() > 6L * numEntries) {
          if (compressionHashTable == null) {
            compressionHashTable = new LZ4.HighCompressionHashTable();
          }
          LZ4.compress(
              suffixWriter.bytes(), 0, suffixWriter.length(), spareWriter, compressionHashTable);
          if (spareWriter.size() < suffixWriter.length() - (suffixWriter.length() >>> 2)) {
            // LZ4 saved more than 25%, go for it
            compressionAlg = CompressionAlgorithm.LZ4;
          }
        }
        if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
          spareWriter.reset();
          if (spareBytes.length < suffixWriter.length()) {
            spareBytes = new byte[ArrayUtil.oversize(suffixWriter.length(), 1)];
          }
          if (LowercaseAsciiCompression.compress(
              suffixWriter.bytes(), suffixWriter.length(), spareBytes, spareWriter)) {
            compressionAlg = CompressionAlgorithm.LOWERCASE_ASCII;
          }
        }
      }

      // 3. 开始写.tim文件
      // 3.1 写token：suffixLength, isLeafBlock, compressionAlg
      long token = ((long) suffixWriter.length()) << 3;
      if (isLeafBlock) {
        token |= 0x04;
      }
      token |= compressionAlg.code;
      termsOut.writeVLong(token);

      // 3.2 写suffix数据
      if (compressionAlg == CompressionAlgorithm.NO_COMPRESSION) {
        termsOut.writeBytes(suffixWriter.bytes(), suffixWriter.length());
      } else {
        spareWriter.copyTo(termsOut);
      }
      suffixWriter.setLength(0);
      spareWriter.reset();

      // 3.3 写suffixLength数据
      // Write suffix lengths
      final int numSuffixBytes = Math.toIntExact(suffixLengthsWriter.size());
      spareBytes = ArrayUtil.growNoCopy(spareBytes, numSuffixBytes);
      suffixLengthsWriter.copyTo(new ByteArrayDataOutput(spareBytes));
      suffixLengthsWriter.reset();
      if (allEqual(spareBytes, 1, numSuffixBytes, spareBytes[0])) {
        // Structured fields like IDs often have most values of the same length
        termsOut.writeVInt((numSuffixBytes << 1) | 1);
        termsOut.writeByte(spareBytes[0]);
      } else {
        termsOut.writeVInt(numSuffixBytes << 1);
        termsOut.writeBytes(spareBytes, numSuffixBytes);
      }

      // 3.4 写stats数据：docFrq，totalTermFreq
      // Stats
      final int numStatsBytes = Math.toIntExact(statsWriter.size());
      termsOut.writeVInt(numStatsBytes);
      statsWriter.copyTo(termsOut);
      statsWriter.reset();

      // 3.5 写termMeta数据：docStartFP，posStartFP，payStartFP，lastPosBlockOffset, skipOffset
      // Write term meta data byte[] blob
      termsOut.writeVInt((int) metaWriter.size());
      metaWriter.copyTo(termsOut);
      metaWriter.reset();

      // if (DEBUG) {
      //   System.out.println("      fpEnd=" + out.getFilePointer());
      // }

      if (hasFloorLeadLabel) {
        // We already allocated to length+1 above:
        prefix.bytes[prefix.length++] = (byte) floorLeadLabel;
      }

      return new PendingBlock(prefix, startFP, hasTerms, isFloor, floorLeadLabel, subIndices);
    }

    TermsWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
      assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
      docsSeen = new FixedBitSet(maxDoc);
      postingsWriter.setField(fieldInfo);
    }

    /** Writes one term's worth of postings. */
    public void write(BytesRef text, TermsEnum termsEnum, NormsProducer norms) throws IOException {
      /*
      if (DEBUG) {
        int[] tmp = new int[lastTerm.length];
        System.arraycopy(prefixStarts, 0, tmp, 0, tmp.length);
        System.out.println("BTTW: write term=" + brToString(text) + " prefixStarts=" + Arrays.toString(tmp) + " pending.size()=" + pending.size());
      }
      */

      // 将docId,freq,pos,payload,offset信息写到.doc, .pos(包含offset数据), .pay文件中
      BlockTermState state = postingsWriter.writeTerm(text, termsEnum, docsSeen, norms);

      /*
      state包含的信息如下：
        singletonDocID：该值如果不为-1，说明只有一篇文档包含当前term，那么singletonDocID的值为对应的文档号，singletonDocID的存在会影响索引文件的数据结构，在生成InnerNode流程点会介绍该值的影响
        lastPosBlockOffset：如果该值为-1，说明term在所有文档中的词频没有达到128，即没有生成一个block，如果至少存在一个block，那么该值描述的是VIntBlocks在索引文件.pos中的起始位置
        docStartFP：当前term的文档号docId、词频信息frequency在索引文件.doc的起始位置
        posStartFP：当前term的位置信息position在索引文件.pos的起始位置
        payStartFP：当前term的偏移位置offset，payload在索引文件.pay的起始位置
        skipOffset：当前term的跳表信息在索引文件.doc的起始位置

        docFreq：包含当前term的文档数量
        totalTermFreq：当前term在所有文档中出现的词频和值
       */
      if (state != null) {

        assert state.docFreq != 0;
        assert fieldInfo.getIndexOptions() == IndexOptions.DOCS
                || state.totalTermFreq >= state.docFreq
            : "postingsWriter=" + postingsWriter;

        // 如果相同前缀的term满足条件，构建PendingBlock。并更新prefixStarts，注意prefixStarts[i]表示以lastTerm[0:i]为前缀的词在pending stack中最下面的下标
        pushTerm(text);

        PendingTerm term = new PendingTerm(text, state);

        // pending即存放term的栈。pending栈中存放两种类型元素：PendingTerm，PendingBlock
        pending.add(term);
        // if (DEBUG) System.out.println("    add pending term = " + text + " pending.size()=" +
        // pending.size());

        // 统计当前域每个词项的文档总频率和总词频，term数量
        sumDocFreq += state.docFreq;
        sumTotalTermFreq += state.totalTermFreq;
        numTerms++;
        if (firstPendingTerm == null) {
          firstPendingTerm = term;
        }
        lastPendingTerm = term;
      }
    }

    /** Pushes the new term to the top of the stack, and writes new blocks. */
    private void pushTerm(BytesRef text) throws IOException {
      // Find common prefix between last term and current term:
      int prefixLength =
          Arrays.mismatch(
              lastTerm.bytes(),
              0,
              lastTerm.length(),
              text.bytes,
              text.offset,
              text.offset + text.length);  // 找到lastTerm和text第一个不同的byte的下标，即相同的前缀长度
      if (prefixLength == -1) { // Only happens for the first term, if it is empty
        assert lastTerm.length() == 0;
        prefixLength = 0;
      }

      // if (DEBUG) System.out.println("  shared=" + pos + "  lastTerm.length=" + lastTerm.length);

      // Close the "abandoned" suffix now:
      for (int i = lastTerm.length() - 1; i >= prefixLength; i--) {

        // 统计term栈中的prefixTopSize
        // term栈即存放term的栈，prefixTopSize是跟栈顶term有相同前缀的term数量
        // pending.size即栈的大小，prefixStarts[i]表示以lastTerm[0:i]为前缀的词在pending stack中最下面的下标
        // pending.size() - prefixStarts[i]即表示以lastTerm[0:i]为前缀的词的个数
        // How many items on top of the stack share the current suffix
        // we are closing:
        int prefixTopSize = pending.size() - prefixStarts[i];
        if (prefixTopSize >= minItemsInBlock) {
          // if (DEBUG) System.out.println("pushTerm i=" + i + " prefixTopSize=" + prefixTopSize + "
          // minItemsInBlock=" + minItemsInBlock);
          // 具有相同前缀的term个数达到阈值，生成NodeBlock
          // 注意：在该循环中，可能会调用多次writeBlocks
          writeBlocks(i + 1, prefixTopSize);
          prefixStarts[i] -= prefixTopSize - 1;
        }
      }

      if (prefixStarts.length < text.length) {
        prefixStarts = ArrayUtil.grow(prefixStarts, text.length);
      }

      // Init new tail:
      for (int i = prefixLength; i < text.length; i++) {
        prefixStarts[i] = pending.size();
      }

      lastTerm.copyBytes(text);
    }

    // Finishes all terms in this field
    public void finish() throws IOException {
      if (numTerms > 0) {
        // if (DEBUG) System.out.println("BTTW: finish prefixStarts=" +
        // Arrays.toString(prefixStarts));

        // Add empty term to force closing of all final blocks:
        pushTerm(new BytesRef());

        // TODO: if pending.size() is already 1 with a non-zero prefix length
        // we can save writing a "degenerate" root block, but we have to
        // fix all the places that assume the root block's prefix is the empty string:
        // 将term栈中剩余未处理的PendingEntry生成NodeBlock。至此所有的PendingEntry会生成一个NodeBlock
        pushTerm(new BytesRef());
        writeBlocks(0, pending.size());

        // We better have one final "root" block:
        assert pending.size() == 1 && !pending.get(0).isTerm
            : "pending.size()=" + pending.size() + " pending=" + pending;
        final PendingBlock root = (PendingBlock) pending.get(0);
        assert root.prefix.length == 0;
        final BytesRef rootCode = root.index.getEmptyOutput();  // root.index类型是FST
        assert rootCode != null;

        ByteBuffersDataOutput metaOut = new ByteBuffersDataOutput();
        // 将信息保存到fields中，后续会写到.tmd文件. Fields保存FieldSummary数据，即terms meta data。
        fields.add(metaOut);

        // 内存中保存tmd的信息：字段序号，词项个数，FST的rootCode信息，总词项频率，总文档频率，总文档数，第一个词项和最后一个词项内容
        metaOut.writeVInt(fieldInfo.number);
        metaOut.writeVLong(numTerms);
        metaOut.writeVInt(rootCode.length);
        metaOut.writeBytes(rootCode.bytes, rootCode.offset, rootCode.length);
        assert fieldInfo.getIndexOptions() != IndexOptions.NONE;
        if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
          metaOut.writeVLong(sumTotalTermFreq);
        }
        metaOut.writeVLong(sumDocFreq);
        metaOut.writeVInt(docsSeen.cardinality());
        writeBytesRef(metaOut, new BytesRef(firstPendingTerm.termBytes));  // minTerm
        writeBytesRef(metaOut, new BytesRef(lastPendingTerm.termBytes));  // maxTerm
        metaOut.writeVLong(indexOut.getFilePointer());

        // 将tip信息写入到.tip文件
        // Write FST to index
        root.index.save(metaOut, indexOut);
        // System.out.println("  write FST " + indexStartFP + " field=" + fieldInfo.name);

        /*
        if (DEBUG) {
          final String dotFileName = segment + "_" + fieldInfo.name + ".dot";
          Writer w = new OutputStreamWriter(new FileOutputStream(dotFileName));
          Util.toDot(root.index, w, false, false);
          System.out.println("SAVED to " + dotFileName);
          w.close();
        }
        */

      } else {
        assert sumTotalTermFreq == 0
            || fieldInfo.getIndexOptions() == IndexOptions.DOCS && sumTotalTermFreq == -1;
        assert sumDocFreq == 0;
        assert docsSeen.cardinality() == 0;
      }
    }

    private final ByteBuffersDataOutput suffixLengthsWriter =
        ByteBuffersDataOutput.newResettableInstance();  // 用来内存中保存suffix len
    private final BytesRefBuilder suffixWriter = new BytesRefBuilder(); // 用来内存中保存suffix
    private final ByteBuffersDataOutput statsWriter = ByteBuffersDataOutput.newResettableInstance(); // 用来保存docFrq，totalTermFreq
    private final ByteBuffersDataOutput metaWriter = ByteBuffersDataOutput.newResettableInstance();  // 用来保存docStartFP，posStartFP，payStartFP，lastPosBlockOffset，skipOffset
    private final ByteBuffersDataOutput spareWriter = ByteBuffersDataOutput.newResettableInstance(); // 用来保存压缩后的数据
    private byte[] spareBytes = BytesRef.EMPTY_BYTES;
    private LZ4.HighCompressionHashTable compressionHashTable;
  }

  private boolean closed;

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    boolean success = false;
    try {
      metaOut.writeVInt(fields.size());
      for (ByteBuffersDataOutput fieldMeta : fields) {
        fieldMeta.copyTo(metaOut);
      }
      CodecUtil.writeFooter(indexOut);
      metaOut.writeLong(indexOut.getFilePointer());
      CodecUtil.writeFooter(termsOut);
      metaOut.writeLong(termsOut.getFilePointer());
      CodecUtil.writeFooter(metaOut);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(metaOut, termsOut, indexOut, postingsWriter); // write到文件
      } else {
        IOUtils.closeWhileHandlingException(metaOut, termsOut, indexOut, postingsWriter);
      }
    }
  }

  private static void writeBytesRef(DataOutput out, BytesRef bytes) throws IOException {
    out.writeVInt(bytes.length);
    out.writeBytes(bytes.bytes, bytes.offset, bytes.length);
  }
}
