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
package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Extension of {@link PostingsWriterBase}, adding a push API for writing each element of the
 * postings. This API is somewhat analogous to an XML SAX API, while {@link PostingsWriterBase} is
 * more like an XML DOM API.
 *
 * @see PostingsReaderBase
 * @lucene.experimental
 */
// TODO: find a better name; this defines the API that the
// terms dict impls use to talk to a postings impl.
// TermsDict + PostingsReader/WriterBase == PostingsConsumer/Producer
public abstract class PushPostingsWriterBase extends PostingsWriterBase {

  // Reused in writeTerm
  private PostingsEnum postingsEnum;
  private int enumFlags;

  /** {@link FieldInfo} of current field being written. */
  protected FieldInfo fieldInfo;

  /** {@link IndexOptions} of current field being written */
  protected IndexOptions indexOptions;

  /** True if the current field writes freqs. */
  protected boolean writeFreqs;

  /** True if the current field writes positions. */
  protected boolean writePositions;

  /** True if the current field writes payloads. */
  protected boolean writePayloads;

  /** True if the current field writes offsets. */
  protected boolean writeOffsets;

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected PushPostingsWriterBase() {}

  /** Return a newly created empty TermState */
  public abstract BlockTermState newTermState() throws IOException;

  /**
   * Start a new term. Note that a matching call to {@link #finishTerm(BlockTermState)} is done,
   * only if the term has at least one document.
   */
  public abstract void startTerm(NumericDocValues norms) throws IOException;

  /**
   * Finishes the current term. The provided {@link BlockTermState} contains the term's summary
   * statistics, and will holds metadata from PBF when returned
   */
  public abstract void finishTerm(BlockTermState state) throws IOException;

  /**
   * Sets the current field for writing, and returns the fixed length of long[] metadata (which is
   * fixed per field), called when the writing switches to another field.
   */
  @Override
  public void setField(FieldInfo fieldInfo) {
    this.fieldInfo = fieldInfo;
    indexOptions = fieldInfo.getIndexOptions();

    writeFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    writePositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    writeOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    writePayloads = fieldInfo.hasPayloads();

    if (writeFreqs == false) {
      enumFlags = 0;
    } else if (writePositions == false) {
      enumFlags = PostingsEnum.FREQS;
    } else if (writeOffsets == false) {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS;
      } else {
        enumFlags = PostingsEnum.POSITIONS;
      }
    } else {
      if (writePayloads) {
        enumFlags = PostingsEnum.PAYLOADS | PostingsEnum.OFFSETS;
      } else {
        enumFlags = PostingsEnum.OFFSETS;
      }
    }
  }

  // 该方法用来写将term的信息写入.doc,.pos,.pay文件
  @Override
  public final BlockTermState writeTerm(
      BytesRef term, TermsEnum termsEnum, FixedBitSet docsSeen, NormsProducer norms)
      throws IOException {
    NumericDocValues normValues;
    if (fieldInfo.hasNorms() == false) {
      normValues = null;
    } else {
      normValues = norms.getNorms(fieldInfo);
    }

    // 记录pos，offset，payload文件的offset
    startTerm(normValues);

    // 初始化读取bytePool的各种下标。termsEnum类型FreqProxTermsEnum
    postingsEnum = termsEnum.postings(postingsEnum, enumFlags);
    assert postingsEnum != null;

    int docFreq = 0;
    long totalTermFreq = 0;
    while (true) {
      // PostingEnum类型FreqProxPostingsEnum
      // 读取下一个包含该term的docId，freq
      int docID = postingsEnum.nextDoc();
      if (docID == PostingsEnum.NO_MORE_DOCS) {
        break;
      }
      docFreq++;
      docsSeen.set(docID);
      int freq;
      if (writeFreqs) {
        // 读取freq
        freq = postingsEnum.freq();
        totalTermFreq += freq;
      } else {
        freq = -1;
      }

      // 开始写docId文档的数据
      startDoc(docID, freq);

      // 需要写pos数据
      if (writePositions) {
        // 该doc中term每次出现，都要记录pos数据
        for (int i = 0; i < freq; i++) {
          // 读取pos，以及payload，offset
          int pos = postingsEnum.nextPosition();
          BytesRef payload = writePayloads ? postingsEnum.getPayload() : null;
          int startOffset;
          int endOffset;
          if (writeOffsets) {
            startOffset = postingsEnum.startOffset();
            endOffset = postingsEnum.endOffset();
          } else {
            startOffset = -1;
            endOffset = -1;
          }

          // 将pos，payload，offset先写到内存数组
          addPosition(pos, payload, startOffset, endOffset);
        }
      }

      // 结束写doc信息，主要是记录跳表需要的信息
      finishDoc();
    }

    /*
    [
        {
            "field": "content",
            "terms":
            [
                {
                    "term": "book",
                    "postings":
                    [
                        {
                            "docId": 1,
                            "freq": 2,
                            "prox":
                            [
                                {
                                    "position": 0,
                                    "payload": "hi",
                                    "offsetStart": 0,
                                    "offsetLen": 3
                                },
                                {
                                    "position": 5,
                                    "payload": "ni",
                                    "offsetStart": 17,
                                    "offsetLen": 3
                                }
                            ]
                        },
                        {
                            "docId": 2,
                            "freq": 3,
                            "prox":
                            [
                                {
                                    "position": 0,
                                    "payload": "hi",
                                    "offsetStart": 0,
                                    "offsetLen": 3
                                },
                                {
                                    "position": 5,
                                    "payload": "ni",
                                    "offsetStart": 17,
                                    "offsetLen": 3
                                },
                                {
                                    "position": 7,
                                    "payload": "ni",
                                    "offsetStart": 29,
                                    "offsetLen": 3
                                }
                            ]
                        }
                    ]
                }
            ]
        }
    ]
    */

    // 包含该term的所有文档都处理完了，执行收尾工作
    if (docFreq == 0) {
      return null;
    } else {
      // 如果文档数或pos数不是128的倍数，会有一些数据无法生成Block
      // 对于docDeltaBuffer、freqBuffer数组中的信息，将会被存储到索引文件.doc的VIntBlocks中
      // 对于posDeltaBuffer、payloadLengthBuffer、payloadBytes、offsetStartDeltaBuffer、offsetLengthBuffer数组中的信息，将会被存储到索引文件.pos的VIntBlocks中
      BlockTermState state = newTermState();
      state.docFreq = docFreq;
      state.totalTermFreq = writeFreqs ? totalTermFreq : -1;
      finishTerm(state);  // 结束写term信息。将之前保存在内存数组中还未写入文件的数据写入到文件中，并将三个文件的FP等信息填充到state中
      return state;
    }
  }

  /**
   * Adds a new doc in this term. <code>freq</code> will be -1 when term frequencies are omitted for
   * the field.
   */
  public abstract void startDoc(int docID, int freq) throws IOException;

  /**
   * Add a new position and payload, and start/end offset. A null payload means no payload; a
   * non-null payload with zero length also means no payload. Caller may reuse the {@link BytesRef}
   * for the payload between calls (method must fully consume the payload). <code>startOffset</code>
   * and <code>endOffset</code> will be -1 when offsets are not indexed.
   */
  public abstract void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException;

  /** Called when we are done adding positions and payloads for each doc. */
  public abstract void finishDoc() throws IOException;
}
