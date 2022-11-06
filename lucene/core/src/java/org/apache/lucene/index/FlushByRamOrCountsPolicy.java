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

/**
 * Default {@link FlushPolicy} implementation that flushes new segments based on RAM used and
 * document count depending on the IndexWriter's {@link IndexWriterConfig}. It also applies pending
 * deletes based on the number of buffered delete terms.
 *
 * <ul>
 *   <li>{@link #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThread)} - applies pending
 *       delete operations based on the global number of buffered delete terms if the consumed
 *       memory is greater than {@link IndexWriterConfig#getRAMBufferSizeMB()}.
 *   <li>{@link #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThread)} - flushes either
 *       on the number of documents per {@link DocumentsWriterPerThread} ( {@link
 *       DocumentsWriterPerThread#getNumDocsInRAM()}) or on the global active memory consumption in
 *       the current indexing session iff {@link IndexWriterConfig#getMaxBufferedDocs()} or {@link
 *       IndexWriterConfig#getRAMBufferSizeMB()} is enabled respectively
 *   <li>{@link #onUpdate(DocumentsWriterFlushControl, DocumentsWriterPerThread)} - calls {@link
 *       #onInsert(DocumentsWriterFlushControl, DocumentsWriterPerThread)} and {@link
 *       #onDelete(DocumentsWriterFlushControl, DocumentsWriterPerThread)} in order
 * </ul>
 *
 * All {@link IndexWriterConfig} settings are used to mark {@link DocumentsWriterPerThread} as flush
 * pending during indexing with respect to their live updates.
 *
 * <p>If {@link IndexWriterConfig#setRAMBufferSizeMB(double)} is enabled, the largest ram consuming
 * {@link DocumentsWriterPerThread} will be marked as pending iff the global active RAM consumption
 * is {@code >=} the configured max RAM buffer.
 */
class FlushByRamOrCountsPolicy extends FlushPolicy {

  // 如果删除信息占用内存超过配置值，设置flush delete
  @Override
  public void onDelete(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
    if ((flushOnRAM()
        && control.getDeleteBytesUsed() > 1024 * 1024 * indexWriterConfig.getRAMBufferSizeMB())) {
      control.setApplyAllDeletes();
      if (infoStream.isEnabled("FP")) {
        infoStream.message(
            "FP",
            "force apply deletes bytesUsed="
                + control.getDeleteBytesUsed()
                + " vs ramBufferMB="
                + indexWriterConfig.getRAMBufferSizeMB());
      }
    }
  }

  // 根据docCount和内存使用判断是否需要flush
  @Override
  public void onInsert(DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
    // 根据dwpt保存的文档数判断是否需要flush。注意，此处是判断该dwpt
    if (flushOnDocCount()
        && perThread.getNumDocsInRAM() >= indexWriterConfig.getMaxBufferedDocs()) {
      // Flush this state by num docs
      control.setFlushPending(perThread);
    }
    // 根据所有dwpt占用的内存判断是否需要flush。注意，如果需要flush，会选择占用内存最大的dwpt
    else if (flushOnRAM()) { // flush by RAM
      final long limit = (long) (indexWriterConfig.getRAMBufferSizeMB() * 1024.d * 1024.d);
      final long totalRam = control.activeBytes() + control.getDeleteBytesUsed();
      if (totalRam >= limit) {
        if (infoStream.isEnabled("FP")) {
          infoStream.message(
              "FP",
              "trigger flush: activeBytes="
                  + control.activeBytes()
                  + " deleteBytes="
                  + control.getDeleteBytesUsed()
                  + " vs limit="
                  + limit);
        }
        markLargestWriterPending(control, perThread);
      }
    }
  }

  /** Marks the most ram consuming active {@link DocumentsWriterPerThread} flush pending */
  protected void markLargestWriterPending(
      DocumentsWriterFlushControl control, DocumentsWriterPerThread perThread) {
    DocumentsWriterPerThread largestNonPendingWriter =
        findLargestNonPendingWriter(control, perThread);
    if (largestNonPendingWriter != null) {
      control.setFlushPending(largestNonPendingWriter);
    }
  }

  /**
   * Returns <code>true</code> if this {@link FlushPolicy} flushes on {@link
   * IndexWriterConfig#getMaxBufferedDocs()}, otherwise <code>false</code>.
   */
  protected boolean flushOnDocCount() {
    return indexWriterConfig.getMaxBufferedDocs() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
  }

  /**
   * Returns <code>true</code> if this {@link FlushPolicy} flushes on {@link
   * IndexWriterConfig#getRAMBufferSizeMB()}, otherwise <code>false</code>.
   */
  protected boolean flushOnRAM() {
    return indexWriterConfig.getRAMBufferSizeMB() != IndexWriterConfig.DISABLE_AUTO_FLUSH;
  }
}
