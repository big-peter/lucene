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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;

/**
 * 存储DocID到文档的正向关系，在排序或者统计计算时，通过DocID可以迅速取字段的值进行二次计算
 * @param <T>
 */
abstract class DocValuesWriter<T extends DocIdSetIterator> {
  abstract void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer consumer)
      throws IOException;

  abstract T getDocValues();
}
