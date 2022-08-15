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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class stores streams of information per term without knowing the size of the stream ahead of
 * time. Each stream typically encodes one level of information like term frequency per document or
 * term proximity. Internally this class allocates a linked list of slices that can be read by a
 * {@link ByteSliceReader} for each term. Terms are first deduplicated in a {@link BytesRefHash}
 * once this is done internal data-structures point to the current offset of each stream that can be
 * written to.
 *
 * PerField -> TermsHashPerField
 */
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {
  private static final int HASH_INIT_SIZE = 4;

  private final TermsHashPerField nextPerField;
  private final IntBlockPool intPool;
  final ByteBlockPool bytePool;
  // for each term we store an integer per stream that points into the bytePool above
  // the address is updated once data is written to the stream to point to the next free offset
  // in the terms stream. The start address for the stream is stored in
  // postingsArray.byteStarts[termId]
  // This is initialized in the #addTerm method, either to a brand new per term stream if the term
  // is new or
  // to the addresses where the term stream was written to when we saw it the last time.
  private int[] termStreamAddressBuffer;
  private int streamAddressOffset;
  private final int streamCount;
  private final String fieldName;
  final IndexOptions indexOptions;
  /* This stores the actual term bytes for postings and offsets into the parent hash in the case that this
   * TermsHashPerField is hashing term vectors.*/
  private final BytesRefHash bytesHash;

  ParallelPostingsArray postingsArray;
  private int lastDocID; // only with assert

  /**
   * streamCount: how many streams this field stores per term. E.g. doc(+freq) is 1 stream,
   * prox+offset is a second.
   */
  TermsHashPerField(
      int streamCount,
      IntBlockPool intPool,
      ByteBlockPool bytePool,
      ByteBlockPool termBytePool,
      Counter bytesUsed,
      TermsHashPerField nextPerField,
      String fieldName,
      IndexOptions indexOptions) {
    this.intPool = intPool;
    this.bytePool = bytePool;
    this.streamCount = streamCount;
    this.fieldName = fieldName;
    this.nextPerField = nextPerField;
    assert indexOptions != IndexOptions.NONE;
    this.indexOptions = indexOptions;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
  }

  void reset() {
    bytesHash.clear(false);
    sortedTermIDs = null;
    if (nextPerField != null) {
      nextPerField.reset();
    }
  }

  final void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int streamStartOffset = postingsArray.addressOffset[termID];
    final int[] streamAddressBuffer =
        intPool.buffers[streamStartOffset >> IntBlockPool.INT_BLOCK_SHIFT];
    final int offsetInAddressBuffer = streamStartOffset & IntBlockPool.INT_BLOCK_MASK;
    reader.init(
        bytePool,
        postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE,
        streamAddressBuffer[offsetInAddressBuffer + stream]);
  }

  private int[] sortedTermIDs;

  /**
   * Collapse the hash table and sort in-place; also sets this.sortedTermIDs to the results This
   * method must not be called twice unless {@link #reset()} or {@link #reinitHash()} was called.
   */
  final void sortTerms() {
    assert sortedTermIDs == null;
    sortedTermIDs = bytesHash.sort();
  }

  /** Returns the sorted term IDs. {@link #sortTerms()} must be called before */
  final int[] getSortedTermIDs() {
    assert sortedTermIDs != null;
    return sortedTermIDs;
  }

  final void reinitHash() {
    sortedTermIDs = null;
    bytesHash.reinit();
  }

  private boolean doNextCall;

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  //
  // 注意和方法void add(BytesRef termBytes, final int docID)签名不同
  // 写入词向量的方法入口。
  private void add(int textStart, final int docID) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart);
    if (termID >= 0) { // New posting
      // First time we are seeing this token since we last
      // flushed the hash.
      initStreamSlices(termID, docID);
    } else {
      positionStreamSlice(termID, docID);
    }
  }

  /**
   * Called when we first encounter a new term. We must allocate slies to store the postings (vInt
   * compressed doc/freq/prox), and also the int pointers to where (in our ByteBlockPool storage)
   * the postings for this term begin.
   */
  private void initStreamSlices(int termID, int docID) throws IOException {
    // Init stream slices
    // 需要初始化streamCount个slice，每个slice在intPool中需要一个item保存
    // 检查是否需要扩容
    if (streamCount + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
      // not enough space remaining in this buffer -- jump to next buffer and lose this remaining
      // piece
      intPool.nextBuffer();
    }

    // bytePool是否需要扩容  TODO wj why * 2
    if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto
        < (2 * streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE) {
      // can we fit at least one byte per stream in the current buffer, if not allocate a new one
      bytePool.nextBuffer();
    }

    termStreamAddressBuffer = intPool.buffer;
    streamAddressOffset = intPool.intUpto;  // 新term在intPool开始写入的下标
    intPool.intUpto += streamCount; // advance the pool to reserve the N streams for this term

    // 保存该term到intPool的下标映射
    postingsArray.addressOffset[termID] = streamAddressOffset + intPool.intOffset;  // intOffset是前面所有buffer保存的item个数，这样相当于一维数组的下标

    for (int i = 0; i < streamCount; i++) {
      // initialize each stream with a slice we start with ByteBlockPool.FIRST_LEVEL_SIZE)
      // and grow as we need more space. see ByteBlockPool.LEVEL_SIZE_ARRAY
      final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE); // 在bytePool上新分配slice
      termStreamAddressBuffer[streamAddressOffset + i] = upto + bytePool.byteOffset;  // 保存该stream在bytePool上开始写入的下标
    }
    postingsArray.byteStarts[termID] = termStreamAddressBuffer[streamAddressOffset];  // 保存该term到bytePool的docId&Frequency开始写入的下标映射

    // 将未见过的term的统计信息写入内存中
    newTerm(termID, docID);
  }

  private boolean assertDocId(int docId) {
    assert docId >= lastDocID : "docID must be >= " + lastDocID + " but was: " + docId;
    lastDocID = docId;
    return true;
  }

  /**
   * Called once per inverted token. This is the primary entry point (for first TermsHash); postings
   * use this API.
   */
  void add(BytesRef termBytes, final int docID) throws IOException {
    assert assertDocId(docID);
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    // 如果该segment中还未遇到该词项，保存并返回termId。否则，返回之前的termId
    int termID = bytesHash.add(termBytes);
    // System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + "
    // termID=" + termID);
    if (termID >= 0) { // New posting
      // Init stream slices
      // 初始化新的slice，保存该term的索引数据
      initStreamSlices(termID, docID);
    } else {
      // 从原来的slice中继续写
      termID = positionStreamSlice(termID, docID);
    }

    // 判断下个处理器是否需要处理
    if (doNextCall) {
      nextPerField.add(postingsArray.textStarts[termID], docID);
    }
  }

  private int positionStreamSlice(int termID, final int docID) throws IOException {
    termID = (-termID) - 1;
    int intStart = postingsArray.addressOffset[termID];
    termStreamAddressBuffer = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    streamAddressOffset = intStart & IntBlockPool.INT_BLOCK_MASK;

    // 将已经见过的term的统计信息写入内存中
    addTerm(termID, docID);
    return termID;
  }

  final void writeByte(int stream, byte b) {
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) {
      // End of slice; allocate a new one
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      termStreamAddressBuffer[streamAddress] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (termStreamAddressBuffer[streamAddress])++;
  }

  final void writeBytes(int stream, byte[] b, int offset, int len) {
    final int end = offset + len;
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] slice = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert slice != null;
    int sliceOffset = upto & ByteBlockPool.BYTE_BLOCK_MASK;

    while (slice[sliceOffset] == 0 && offset < end) {
      slice[sliceOffset++] = b[offset++];
      (termStreamAddressBuffer[streamAddress])++;
    }

    while (offset < end) {
      int offsetAndLength = bytePool.allocKnownSizeSlice(slice, sliceOffset);
      sliceOffset = offsetAndLength >> 8;
      int sliceLength = offsetAndLength & 0xff;
      slice = bytePool.buffer;
      int writeLength = Math.min(sliceLength - 1, end - offset);
      System.arraycopy(b, offset, slice, sliceOffset, writeLength);
      sliceOffset += writeLength;
      offset += writeLength;
      termStreamAddressBuffer[streamAddress] = sliceOffset + bytePool.byteOffset;
    }
  }

  // 保存变长整数
  final void writeVInt(int stream, int i) {
    assert stream < streamCount;
    // 变长数字保存
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  final TermsHashPerField getNextPerField() {
    return nextPerField;
  }

  final String getFieldName() {
    return fieldName;
  }

  /**
   * 可以认为是ParallelPostingsArray的封装类，freq和termvector分别有自己的实现
   */
  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray();
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow();
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(
            -(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray();
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public final int compareTo(TermsHashPerField other) {
    return fieldName.compareTo(other.fieldName);
  }

  /** Finish adding all instances of this field to the current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish();
    }
  }

  final int getNumTerms() {
    return bytesHash.size();
  }

  /**
   * Start adding a new field instance; first is true if this is the first time this field name was
   * seen in the document.
   */
  boolean start(IndexableField field, boolean first) {
    if (nextPerField != null) {
      doNextCall = nextPerField.start(field, first);
    }
    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID, final int docID) throws IOException;

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID, final int docID) throws IOException;

  /** Called when the postings array is initialized or resized. */
  abstract void newPostingsArray();

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
