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
package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.codecs.lucene90.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * <p>Postings list for each term will be stored separately.
 *
 * @see Lucene90SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */
public final class Lucene90PostingsWriter extends PushPostingsWriterBase {

  IndexOutput docOut;
  IndexOutput posOut;
  IndexOutput payOut;

  static final IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP;
  private long posStartFP;
  private long payStartFP;

  final long[] docDeltaBuffer;
  final long[] freqBuffer;
  private int docBufferUpto;

  final long[] posDeltaBuffer;
  final long[] payloadLengthBuffer;
  final long[] offsetStartDeltaBuffer;
  final long[] offsetLengthBuffer;
  private int posBufferUpto;

  private byte[] payloadBytes;
  private int payloadByteUpto;

  private int lastBlockDocID;
  private long lastBlockPosFP;
  private long lastBlockPayFP;
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int lastDocID;
  private int lastPosition;
  private int lastStartOffset;
  private int docCount;

  private final PForUtil pforUtil;
  private final Lucene90SkipWriter skipWriter;

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  /** Creates a postings writer */
  public Lucene90PostingsWriter(SegmentWriteState state) throws IOException {

    String docFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      pforUtil = new PForUtil(new ForUtil());
      if (state.fieldInfos.hasProx()) {
        posDeltaBuffer = new long[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName =
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene90PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];

    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter =
        new Lucene90SkipWriter(
            MAX_SKIP_LEVELS, BLOCK_SIZE, state.segmentInfo.maxDoc(), docOut, posOut, payOut);
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    lastState = emptyState;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  @Override
  public void startTerm(NumericDocValues norms) {
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
      }
    }
    lastDocID = 0;
    lastBlockDocID = -1;
    skipWriter.resetSkip();
    this.norms = norms;
    competitiveFreqNormAccumulator.clear();
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // Have collected a block of docs, and get a new doc.
    // Should write skip data as well as postings list for
    // current block.
    if (lastBlockDocID != -1 && docBufferUpto == 0) {
      // 刚刚.doc写入了一个Block，需要写入skipList数据
      // 将SkipData保存到内存数组中。注意参数是在finishDoc中记录的上一个block中的信息
      skipWriter.bufferSkip(
          lastBlockDocID,
          competitiveFreqNormAccumulator,
          docCount,
          lastBlockPosFP,
          lastBlockPayFP,
          lastBlockPosBufferUpto,
          lastBlockPayloadByteUpto);
      competitiveFreqNormAccumulator.clear();
    }

    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException(
          "docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;  // 将docDelta，freqDelta保存到内存数组中
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    docBufferUpto++;
    docCount++;

    if (docBufferUpto == BLOCK_SIZE) {
      // buffer中的元素个数达到128个，生成PackedBlock写入.doc文件。packedBlock === [PackedDocDeltaBlock | PackedFreqBlock]
      // PackedBlock及将buffer中的数据经过PackedInts压缩处理。
      pforUtil.encode(docDeltaBuffer, docOut);
      if (writeFreqs) {
        pforUtil.encode(freqBuffer, docOut);
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that
      // the block was filled so it can save skip data)
    }

    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    long norm;
    if (fieldHasNorms) {
      boolean found = norms.advanceExact(docID);
      if (found == false) {
        // This can happen if indexing hits a problem after adding a doc to the
        // postings but before buffering the norm. Such documents are written
        // deleted and will go away on the first merge.
        norm = 1L;
      } else {
        norm = norms.longValue();
        assert norm != 0 : docID;
      }
    } else {
      norm = 1L;
    }

    competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException(
          "position="
              + position
              + " is too large (> IndexWriter.MAX_POSITION="
              + IndexWriter.MAX_POSITION
              + ")",
          docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }

    // 将pos，payload,offset先保存到内存数组中
    // pos，offset有序，差值存储
    posDeltaBuffer[posBufferUpto] = position - lastPosition;
    if (writePayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0;
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) {
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        }
        System.arraycopy(
            payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length;
      }
    }

    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {
      // 处理了128个pos信息，压缩成Block存储

      // 生成PackedPosBlock写入到.pos文件
      pforUtil.encode(posDeltaBuffer, posOut);

      if (writePayloads) {
        // 生成PackedPayBlock写入到.pay文件
        // 先生成PackedPayLengthBlock,写入.pay文件
        pforUtil.encode(payloadLengthBuffer, payOut);
        // 将payLoadByteUpto写入.pay文件，即当前block中paydata的大小
        payOut.writeVInt(payloadByteUpto);
        // 将payloadBytes写入.pay文件
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }


      if (writeOffsets) {
        // 生成PackedOffsetStartDeltaBlock写入到.pay文件
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        // 生成PackedOffsetLengthBlock写入到.pay文件
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0;
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes,
    // write them to skip file.
    if (docBufferUpto == BLOCK_SIZE) {
      // 如果包含当前term的文档数达到128， 记录下列信息。这些信息用来生成跳表SkipList的

      // 刚刚处理完的文档号
      lastBlockDocID = lastDocID;

      if (posOut != null) {
        if (payOut != null) {
          // .pay文件的FilePointer
          lastBlockPayFP = payOut.getFilePointer();
        }
        // .pos文件的FilePointer
        lastBlockPosFP = posOut.getFilePointer();

        // 在posDeltaBuffer、payloadLengthBuffer、offsetStartDeltaBuffer、offsetLengthBuffer数组中的数组下标值
        lastBlockPosBufferUpto = posBufferUpto;

        // 在payloadBytes数组中的数组下标值
        lastBlockPayloadByteUpto = payloadByteUpto;
      }
      docBufferUpto = 0;
    }
  }

  /** Called when we are done adding docs to this term */
  @Override
  public void finishTerm(BlockTermState _state) throws IOException {
    // 剩余的数不足以生成packedBlock，使用VIntBlock保存
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it.
    final int singletonDocID;
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = (int) docDeltaBuffer[0];
    } else {
      // 写入doc信息，每个doc，存储下列信息：docDelta，freq
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs:
      for (int i = 0; i < docBufferUpto; i++) {
        final int docDelta = (int) docDeltaBuffer[i];
        final int freq = (int) freqBuffer[i];
        if (!writeFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freq == 1) {
          docOut.writeVInt((docDelta << 1) | 1);
        } else {
          docOut.writeVInt(docDelta << 1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;

    // 写入pos数据。每个pos，存储下列信息：posDelta, payloadLength, payloadBytes, offsetDelta, offsetLength
    if (writePositions) {
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) {
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1; // force first payload length to be written
        int lastOffsetLength = -1; // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for (int i = 0; i < posBufferUpto; i++) {
          final int posDelta = (int) posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) {
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta << 1) | 1);
              posOut.writeVInt(payloadLength);
            } else {
              posOut.writeVInt(posDelta << 1);
            }

            if (payloadLength != 0) {
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (writeOffsets) {
            int delta = (int) offsetStartDeltaBuffer[i];
            int length = (int) offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1);
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }

    long skipOffset;
    if (docCount > BLOCK_SIZE) {
      // 将内存中保存的skipList数据写入到.doc文件
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP;
    } else {
      skipOffset = -1;
    }

    state.docStartFP = docStartFP;
    state.posStartFP = posStartFP;
    state.payStartFP = payStartFP;
    state.singletonDocID = singletonDocID;
    state.skipOffset = skipOffset;
    state.lastPosBlockOffset = lastPosBlockOffset;
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    // 保存docStartFp
    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    // 保存posStartFP，payStartFP
    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }

    // pos个数不是128的倍数，最后一部分使用Vint保存，保存lastPosBlockOffset
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }

    // 保存skipOffset
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}
