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

import org.apache.lucene.util.ArrayUtil;

/**
 * 保存词项到bytePool，intPool的下标的映射。注意arr[i]表示term i的映射数据。
 * 一个Field对应一个该对象
 */
class ParallelPostingsArray {
  static final int BYTES_PER_POSTING = 3 * Integer.BYTES;

  final int size;
  /**
   * 映射到bytePool的term信息开始下标。
   */
  final int[] textStarts; // maps term ID to the terms's text start in the bytesHash
  /**
   * 映射到IntPool的开始下标，buffer[i]表示bytePool中docId，frequency开始写入的下标，
   * buffer[i+1]表示bytePool中position，payload，offset，len开始写入的下标
   */
  final int[] addressOffset; // maps term ID to current stream address
  /**
   * 映射到bytePool的docId，Frequency信息开始下标，
   */
  final int[] byteStarts; // maps term ID to stream start offset in the byte pool

  ParallelPostingsArray(final int size) {
    this.size = size;
    textStarts = new int[size];
    addressOffset = new int[size];
    byteStarts = new int[size];
  }

  int bytesPerPosting() {
    return BYTES_PER_POSTING;
  }

  ParallelPostingsArray newInstance(int size) {
    return new ParallelPostingsArray(size);
  }

  final ParallelPostingsArray grow() {
    int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
    ParallelPostingsArray newArray = newInstance(newSize);
    copyTo(newArray, size);
    return newArray;
  }

  void copyTo(ParallelPostingsArray toArray, int numToCopy) {
    System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
    System.arraycopy(addressOffset, 0, toArray.addressOffset, 0, numToCopy);
    System.arraycopy(byteStarts, 0, toArray.byteStarts, 0, numToCopy);
  }
}
