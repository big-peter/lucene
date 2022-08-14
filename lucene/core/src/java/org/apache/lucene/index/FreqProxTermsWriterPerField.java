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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.util.BytesRef;

// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField {

  private FreqProxPostingsArray freqProxPostingsArray;
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(
      FieldInvertState invertState,
      TermsHash termsHash,
      FieldInfo fieldInfo,
      TermsHashPerField nextPerField) {
    super(
        // streamCount，如果只要保存docId和frequency，为1；否则，即还要保存position，offset，为2
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0
            ? 2
            : 1,
        termsHash.intPool,
        termsHash.bytePool,
        termsHash.termBytePool,
        termsHash.bytesUsed,
        nextPerField,
        fieldInfo.name,
        fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) {
    super.start(f, first);
    termFreqAtt = fieldState.termFreqAttribute;
    payloadAttribute = fieldState.payloadAttribute;
    offsetAttribute = fieldState.offsetAttribute;
    return true;
  }

  void writeProx(int termID, int proxCode) {
    if (payloadAttribute == null) {
      // 如果不保存payload数据，直接保存position。注意是组合值
      writeVInt(1, proxCode << 1);
    } else {
      // 要保存payload数据。先获取payload
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        // 保存position信息，注意是组合值，最低位是1
        writeVInt(1, (proxCode << 1) | 1);
        // 保存payload的长度
        writeVInt(1, payload.length);
        // 保存payload的内容，offset，len
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode << 1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }

  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    // 写入开始offset
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]);  // 数字差值保存
    // 写入结束offset
    writeVInt(1, endOffset - startOffset);  // 数字差值保存
    freqProxPostingsArray.lastOffsets[termID] = startOffset;
  }

  // 将未见过的term的统计信息写入内存中
  @Override
  void newTerm(final int termID, final int docID) {
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;

    postings.lastDocIDs[termID] = docID;
    // 不保存频次信息
    if (!hasFreq) {
      assert postings.termFreqs == null;
      postings.lastDocCodes[termID] = docID;
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);
    }
    // 要保存频次信息
    else {
      postings.lastDocCodes[termID] = docID << 1;
      postings.termFreqs[termID] = getTermFreq();

      // 要保存position信息
      if (hasProx) {
        writeProx(termID, fieldState.position);

        // 要保存offset信息
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency =
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  // 将已经见过的term的统计信息写入内存中
  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;

    // 如果该Field不做频率统计
    if (!hasFreq) {
      assert postings.termFreqs == null;
      if (termFreqAtt.getTermFrequency() != 1) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": must index term freq while using custom TermFrequencyAttribute");
      }
      if (docID != postings.lastDocIDs[termID]) {
        // New document; now encode docCode for previous doc:
        assert docID > postings.lastDocIDs[termID];
        writeVInt(0, postings.lastDocCodes[termID]);
        postings.lastDocCodes[termID] = docID - postings.lastDocIDs[termID];
        postings.lastDocIDs[termID] = docID;
        fieldState.uniqueTermCount++;
      }
    }
    // 如果该Field要做频次统计，且遇到了新doc
    else if (docID != postings.lastDocIDs[termID]) {
      assert docID > postings.lastDocIDs[termID]
          : "id: " + docID + " postings ID: " + postings.lastDocIDs[termID] + " termID: " + termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc,
      // write it & lastDocCode
      // 写入上一个docId，frequency信息。由于遇到了新文档，之前文档的频次已经知道了，可以写入bytePool中
      if (1 == postings.termFreqs[termID]) {
        // 如果频次为1，将docId和frequency组合保存
        writeVInt(0, postings.lastDocCodes[termID] | 1);
      } else {
        // 否则，将docId和frequency分开保存
        writeVInt(0, postings.lastDocCodes[termID]);
        writeVInt(0, postings.termFreqs[termID]);
      }

      // Init freq for the current document
      postings.termFreqs[termID] = getTermFreq();
      fieldState.maxTermFrequency =
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1; // 数字增量保存
      postings.lastDocIDs[termID] = docID;

      // 如果要保存位置信息(+payload)
      if (hasProx) {
        writeProx(termID, fieldState.position); // 写入位置信息

        // 如果要保存偏移量
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);  // 写入偏移量信息
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    }
    // 还在处理之前的文档
    else {
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
      fieldState.maxTermFrequency =
          Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) {
        writeProx(termID, fieldState.position - postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (hasProx) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(
        int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      // System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" +
      // writeOffsets);
    }

    int[] termFreqs; // # times this term occurs in the current doc。 termFreqs[i]表示词项i的频次是termFreqs[i]
    int[] lastDocIDs; // Last docID where this term occurred。当遇到当前docId和lastDocId不一致时，说明遇到了新的doc
    int[] lastDocCodes; // Code for prior doc。docId和频次的组合值
    int[] lastPositions; // Last position where this term occurred
    int[] lastOffsets; // Last endOffset where this term occurred

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(
          size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
