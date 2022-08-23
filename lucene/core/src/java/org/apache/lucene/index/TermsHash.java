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
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class is passed each token produced by the analyzer on each field during indexing, and it
 * stores these tokens in a hash table, and allocates separate byte streams per token. Consumers of
 * this class, eg {@link FreqProxTermsWriter} and {@link TermVectorsConsumer}, write their own byte
 * streams under each term.
 *
 * 内存中保存索引数据。IndexChain与TermsHash一对一的关系，PerField.termsHashPerField是在TermsHash的基础上生成的，内部共用相同的数据。所以
 * 一个DWPT一个TermsHash，即一个segment一个TermsHash。
 *
 * 子类FreqProxTermsWriter负责保存频次位置信息，TermVectorsConsumer负责保存词向量信息。
 */
abstract class TermsHash {

  /**
   * 链式保存，目前是 FreqProxTermsWriter -> TermVectorsConsumer
   */
  final TermsHash nextTermsHash;

  /**
   * 保存term在bytePool中docId+frequency和position+payload+offset+len开始写入的下标。目前一个term对应两项。
   * 词项i对应的下标由ParallelPostingsArray.addressOffset[i]保存，即addressOffset[i],addressOffset[i+1]
   */
  final IntBlockPool intPool;
  /**
   * 保存term,docId,frequency,position,payload,offset,len数据。底层实现是连续内存实现链表
   */
  final ByteBlockPool bytePool;
  /**
   * 保存词项内容。目前和bytePool共用一个ByteBlockPool
   */
  ByteBlockPool termBytePool;

  final Counter bytesUsed;

  TermsHash(
      final IntBlockPool.Allocator intBlockAllocator,
      final ByteBlockPool.Allocator byteBlockAllocator,
      Counter bytesUsed,
      TermsHash nextTermsHash) {
    this.nextTermsHash = nextTermsHash;
    this.bytesUsed = bytesUsed;
    intPool = new IntBlockPool(intBlockAllocator);
    bytePool = new ByteBlockPool(byteBlockAllocator);

    if (nextTermsHash != null) {
      // We are primary
      // termBytePool和bytePool共用一个ByteBlockPool。即存储term和docId，frequency,position是在一个ByteBlockPool中
      // termVector和freq使用同一个termBytePool.
      termBytePool = bytePool;
      nextTermsHash.termBytePool = bytePool;
    }
  }

  public void abort() {
    try {
      reset();
    } finally {
      if (nextTermsHash != null) {
        nextTermsHash.abort();
      }
    }
  }

  // Clear all state
  void reset() {
    // we don't reuse so we drop everything and don't fill with 0
    intPool.reset(false, false);
    bytePool.reset(false, false);
  }

  void flush(
      Map<String, TermsHashPerField> fieldsToFlush,
      final SegmentWriteState state,
      Sorter.DocMap sortMap,
      NormsProducer norms)
      throws IOException {
    if (nextTermsHash != null) {
      Map<String, TermsHashPerField> nextChildFields = new HashMap<>();
      for (final Map.Entry<String, TermsHashPerField> entry : fieldsToFlush.entrySet()) {
        nextChildFields.put(entry.getKey(), entry.getValue().getNextPerField());
      }
      nextTermsHash.flush(nextChildFields, state, sortMap, norms);
    }
  }

  /**
   * (该segment中)第一次遇到某个Field时调用，创建用于该Field写入索引的类TermsHashPerField。
   * @param fieldInvertState
   * @param fieldInfo
   * @return
   */
  abstract TermsHashPerField addField(FieldInvertState fieldInvertState, FieldInfo fieldInfo);

  void finishDocument(int docID) throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.finishDocument(docID);
    }
  }

  void startDocument() throws IOException {
    if (nextTermsHash != null) {
      nextTermsHash.startDocument();
    }
  }
}
