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
package org.apache.lucene.util.fst;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/**
 * Builds a minimal FST (maps an IntsRef term to an arbitrary output) from pre-sorted terms with
 * outputs. The FST becomes an FSA if you use NoOutputs. The FST is written on-the-fly into a
 * compact serialized format byte array, which can be saved to / loaded from a Directory or used
 * directly for traversal. The FST is always finite (no cycles).
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
 *
 * <p>The parameterized type T is the output type. See the subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene 4.2). FSTs containing more than 2.1B
 * nodes are also now possible, however they cannot be packed.
 *
 * @lucene.experimental
 */
public class FSTCompiler<T> {

  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

  private final NodeHash<T> dedupHash;  // 哈希表，用来去重
  final FST<T> fst;
  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  // simplistic pruning: we prune node (and all following
  // nodes) if less than this number of terms go through it:
  private final int minSuffixCount1;

  // better pruning: we prune node (and all following
  // nodes) if the prior node has less than this number of
  // terms go through it:
  private final int minSuffixCount2;

  private final boolean doShareNonSingletonNodes;
  private final int shareMaxTailLength;

  private final IntsRefBuilder lastInput = new IntsRefBuilder();

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  private UnCompiledNode<T>[] frontier;

  // Used for the BIT_TARGET_NEXT optimization (whereby
  // instead of storing the address of the target node for
  // a given arc, we mark a single bit noting that the next
  // node in the byte[] is the target node):
  long lastFrozenNode;

  // 用来定长存储时保存每个arc的byte数
  // Reused temporarily while building the FST:
  int[] numBytesPerArc = new int[4];
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  long arcCount;
  long nodeCount;
  long binarySearchNodeCount;
  long directAddressingNodeCount;

  final boolean allowFixedLengthArcs;
  final float directAddressingMaxOversizingFactor;
  long directAddressingExpansionCredit;

  final BytesStore bytes;

  /**
   * Instantiates an FST/FSA builder with default settings and pruning options turned off. For more
   * tuning and tweaking, see {@link Builder}.
   */
  public FSTCompiler(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
    this(inputType, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15, 1f);
  }

  private FSTCompiler(
      FST.INPUT_TYPE inputType,
      int minSuffixCount1,
      int minSuffixCount2,
      boolean doShareSuffix,
      boolean doShareNonSingletonNodes,
      int shareMaxTailLength,
      Outputs<T> outputs,
      boolean allowFixedLengthArcs,
      int bytesPageBits,
      float directAddressingMaxOversizingFactor) {
    this.minSuffixCount1 = minSuffixCount1;
    this.minSuffixCount2 = minSuffixCount2;
    this.doShareNonSingletonNodes = doShareNonSingletonNodes;
    this.shareMaxTailLength = shareMaxTailLength;
    this.allowFixedLengthArcs = allowFixedLengthArcs;
    this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
    fst = new FST<>(inputType, outputs, bytesPageBits);
    bytes = fst.bytes;
    assert bytes != null;
    if (doShareSuffix) {
      dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings({"rawtypes", "unchecked"})
    final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f;
    for (int idx = 0; idx < frontier.length; idx++) {
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * Fluent-style constructor for FST {@link FSTCompiler}.
   *
   * <p>Creates an FST/FSA builder with all the possible tuning and construction tweaks. Read
   * parameter documentation carefully.
   */
  public static class Builder<T> {

    private final INPUT_TYPE inputType;
    private final Outputs<T> outputs;
    private int minSuffixCount1;
    private int minSuffixCount2;
    private boolean shouldShareSuffix = true;
    private boolean shouldShareNonSingletonNodes = true;
    private int shareMaxTailLength = Integer.MAX_VALUE;
    private boolean allowFixedLengthArcs = true;
    private int bytesPageBits = 15;
    private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

    /**
     * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
     *     enumeration. Shorter types will consume less memory. Strings (character sequences) are
     *     represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
     * @param outputs The output type for each input sequence. Applies only if building an FST. For
     *     FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
     *     singleton output object.
     */
    public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
      this.inputType = inputType;
      this.outputs = outputs;
    }

    /**
     * If pruning the input graph during construction, this threshold is used for telling if a node
     * is kept or pruned. If transition_count(node) &gt;= minSuffixCount1, the node is kept.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount1(int minSuffixCount1) {
      this.minSuffixCount1 = minSuffixCount1;
      return this;
    }

    /**
     * Better pruning: we prune node (and all following nodes) if the prior node has less than this
     * number of terms go through it.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount2(int minSuffixCount2) {
      this.minSuffixCount2 = minSuffixCount2;
      return this;
    }

    /**
     * If {@code true}, the shared suffixes will be compacted into unique paths. This requires an
     * additional RAM-intensive hash map for lookups in memory. Setting this parameter to {@code
     * false} creates a single suffix path for all input sequences. This will result in a larger
     * FST, but requires substantially less memory and CPU during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareSuffix(boolean shouldShareSuffix) {
      this.shouldShareSuffix = shouldShareSuffix;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to true to ensure FST is fully
     * minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareNonSingletonNodes(boolean shouldShareNonSingletonNodes) {
      this.shouldShareNonSingletonNodes = shouldShareNonSingletonNodes;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to Integer.MAX_VALUE to ensure FST
     * is fully minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@link Integer#MAX_VALUE}.
     */
    public Builder<T> shareMaxTailLength(int shareMaxTailLength) {
      this.shareMaxTailLength = shareMaxTailLength;
      return this;
    }

    /**
     * Pass {@code false} to disable the fixed length arc optimization (binary search or direct
     * addressing) while building the FST; this will make the resulting FST smaller but slower to
     * traverse.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
      this.allowFixedLengthArcs = allowFixedLengthArcs;
      return this;
    }

    /**
     * How many bits wide to make each byte[] block in the BytesStore; if you know the FST will be
     * large then make this larger. For example 15 bits = 32768 byte pages.
     *
     * <p>Default = 15.
     */
    public Builder<T> bytesPageBits(int bytesPageBits) {
      this.bytesPageBits = bytesPageBits;
      return this;
    }

    /**
     * Overrides the default the maximum oversizing of fixed array allowed to enable direct
     * addressing of arcs instead of binary search.
     *
     * <p>Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
     * only binary search nodes will be created.
     *
     * <p>This factor does not determine whether to encode a node with a list of variable length
     * arcs or with fixed length arcs. It only determines the effective encoding of a node that is
     * already known to be encoded with fixed length arcs.
     *
     * <p>Default = 1.
     */
    public Builder<T> directAddressingMaxOversizingFactor(float factor) {
      this.directAddressingMaxOversizingFactor = factor;
      return this;
    }

    /** Creates a new {@link FSTCompiler}. */
    public FSTCompiler<T> build() {
      FSTCompiler<T> fstCompiler =
          new FSTCompiler<>(
              inputType,
              minSuffixCount1,
              minSuffixCount2,
              shouldShareSuffix,
              shouldShareNonSingletonNodes,
              shareMaxTailLength,
              outputs,
              allowFixedLengthArcs,
              bytesPageBits,
              directAddressingMaxOversizingFactor);
      return fstCompiler;
    }
  }

  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getTermCount() {
    return frontier[0].inputCount;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1 + nodeCount;
  }

  public long getArcCount() {
    return arcCount;
  }

  public long getMappedStateCount() {
    return dedupHash == null ? 0 : nodeCount;
  }

  private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
    final long node;
    long bytesPosStart = bytes.getPosition();  // bytes开始写入位置
    if (dedupHash != null
        && (doShareNonSingletonNodes || nodeIn.numArcs <= 1)
        && tailLength <= shareMaxTailLength) {
      if (nodeIn.numArcs == 0) {
        node = fst.addNode(this, nodeIn);  // encode
        lastFrozenNode = node;
      } else {
        node = dedupHash.add(this, nodeIn);  // 处理相同后缀，保证相同后缀只存储一份
      }
    } else {
      node = fst.addNode(this, nodeIn);
    }
    assert node != -2;

    long bytesPosEnd = bytes.getPosition();
    if (bytesPosEnd != bytesPosStart) {
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }

    nodeIn.clear();

    final CompiledNode fn = new CompiledNode();
    fn.node = node;
    return fn;
  }

  // 将[prefixLenPlus1, lastInput]的arc冻结，即编码写入bytes
  private void freezeTail(int prefixLenPlus1) throws IOException {
    // System.out.println("  compileTail " + prefixLenPlus1);
    final int downTo = Math.max(1, prefixLenPlus1);
    // 从后向前freeze node，直到公共前缀处
    for (int idx = lastInput.length(); idx >= downTo; idx--) {

      boolean doPrune = false;
      boolean doCompile = false;

      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parent = frontier[idx - 1];

      // 确定是否prune，compile。默认compile，不prune
      // pron，修剪，如果该前缀的次数小于阈值，则删除该节点
      // minSuffixCount1，minSuffixCount2 默认0，即不对input prune
      if (node.inputCount < minSuffixCount1) {
        doPrune = true;
        doCompile = true;
      } else if (idx > prefixLenPlus1) {
        // prune if parent's inputCount is less than suffixMinCount2
        if (parent.inputCount < minSuffixCount2
            || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) {
          // my parent, about to be compiled, doesn't make the cut, so
          // I'm definitely pruned

          // if minSuffixCount2 is 1, we keep only up
          // until the 'distinguished edge', ie we keep only the
          // 'divergent' part of the FST. if my parent, about to be
          // compiled, has inputCount 1 then we are already past the
          // distinguished edge.  NOTE: this only works if
          // the FST outputs are not "compressible" (simple
          // ords ARE compressible).
          doPrune = true;
        } else {
          // my parent, about to be compiled, does make the cut, so
          // I'm definitely not pruned
          doPrune = false;
        }
        doCompile = true;
      } else {
        // if pruning is disabled (count is 0) we can always
        // compile current node
        doCompile = minSuffixCount2 == 0;
      }

      // System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx="
      // + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune="
      // + doPrune);

      // TODO wj
      if (node.inputCount < minSuffixCount2
          || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
        // drop all arcs
        for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
          @SuppressWarnings({"rawtypes", "unchecked"})
          final UnCompiledNode<T> target = (UnCompiledNode<T>) node.arcs[arcIdx].target;
          target.clear();
        }
        node.numArcs = 0;
      }

      if (doPrune) {
        // this node doesn't make it -- deref it
        node.clear();
        parent.deleteLast(lastInput.intAt(idx - 1), node);
      } else {

        if (minSuffixCount2 != 0) {
          compileAllTargets(node, lastInput.length() - idx);
        }
        final T nextFinalOutput = node.output;  // 如果该node是某个term的结束，则output保存相关分量

        // We "fake" the node as being final if it has no
        // outgoing arcs; in theory we could leave it
        // as non-final (the FST can represent this), but
        // FSTEnum, Util, etc., have trouble w/ non-final
        // dead-end states:
        final boolean isFinal = node.isFinal || node.numArcs == 0;

        if (doCompile) {
          // this node makes it and we now compile it.  first,
          // compile any targets that were previously
          // undecided:
          parent.replaceLast(
              lastInput.intAt(idx - 1),  // parent --arc--> node, 获取arc label，用来验证
              compileNode(node, 1 + lastInput.length() - idx),  // 生成CompiledNode，主要是将arc的四元素写到bytes中
              nextFinalOutput,  // 将node的output赋值给arc
              isFinal);
        } else {
          // replaceLast just to install
          // nextFinalOutput/isFinal onto the arc
          parent.replaceLast(lastInput.intAt(idx - 1), node, nextFinalOutput, isFinal);
          // this node will stay in play for now, since we are
          // undecided on whether to prune it.  later, it
          // will be either compiled or pruned, so we must
          // allocate a new node:
          frontier[idx] = new UnCompiledNode<>(this, idx);
        }
      }
    }
  }

  // for debugging
  /*
  private String toString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      return b.toString();
    }
  }
  */

  /**
   * Add the next input/output pair. The provided input must be sorted after the previous one
   * according to {@link IntsRef#compareTo}. It's also OK to add the same input twice in a row with
   * different outputs, as long as {@link Outputs} implements the {@link Outputs#merge} method. Note
   * that input is fully consumed after this method is returned (so caller is free to reuse), but
   * output is not. So if your outputs are changeable (eg {@link ByteSequenceOutputs} or {@link
   * IntSequenceOutputs}) then you cannot reuse across calls.
   */
  public void add(IntsRef input, T value) throws IOException {
    /*
    if (DEBUG) {
      BytesRef b = new BytesRef(input.length);
      for(int x=0;x<input.length;x++) {
        b.bytes[x] = (byte) input.ints[x];
      }
      b.length = input.length;
      if (output == NO_OUTPUT) {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b);
      } else {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b + " output=" + fst.outputs.outputToString(output));
      }
    }
    */

    // De-dup NO_OUTPUT since it must be a singleton:
    if (value.equals(NO_OUTPUT)) {
      value = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0
        : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(value);

    // System.out.println("\nadd: " + input);
    if (input.length == 0) {
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].inputCount++;
      frontier[0].isFinal = true;
      fst.setEmptyOutput(value);
      return;
    }

    // 1. 找到上一个输入与当前输入不相同的Node节点。保证相同前缀只保存一份
    // compare shared prefix length
    int pos1 = 0;
    int pos2 = input.offset;
    final int pos1Stop = Math.min(lastInput.length(), input.length);
    while (true) {
      frontier[pos1].inputCount++;
      // System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" +
      // frontier[pos1]);
      if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
        break;
      }
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1 + 1;

    // frontier扩容
    if (frontier.length < input.length + 1) {
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
      for (int idx = frontier.length; idx < next.length; idx++) {
        next[idx] = new UnCompiledNode<>(this, idx);
      }
      frontier = next;
    }

    // 2. 将不相同Node节点及后面节点中的所有arc的四元组信息写入到current[]数组中，即冻结。后面会重用frontier[prefixLenPlus:lastLength]的节点
    // minimize/compile states from previous input's
    // orphan'd suffix
    freezeTail(prefixLenPlus1);

    // 3. 从和上一个输入的第一个不相同的位置处将当前输入写入到frontier[]数组中
    // init tail states for current input
    for (int idx = prefixLenPlus1; idx <= input.length; idx++) {
      // 从公共前缀末尾节点开始创建arc
      frontier[idx - 1].addArc(input.ints[input.offset + idx - 1], frontier[idx]);
      frontier[idx].inputCount++;
    }

    // 设置node的isFinal属性
    final UnCompiledNode<T> lastNode = frontier[input.length];
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
      lastNode.isFinal = true;
      lastNode.output = NO_OUTPUT;
    }

    // 4. 处理上一个输入与当前输入相同的前缀值，调整output值。保证之前path的output和不变，该次path的output和等于value
    // 一直循环到前缀结束，在新的value大于前缀边上的output时，以便将余量分配到之前的路径的其他边
    // push conflicting outputs forward, only as far as needed
//    value = updateOutputRecursively(input, value, 0, prefixLenPlus1 - 1);
    for (int idx = 1; idx < prefixLenPlus1; idx++) {  // 将common值放到公共arc上，将余量值放到原来的或新的arc上
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx - 1];

      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;
      final T wordSuffix;

      if (lastOutput != NO_OUTPUT) {
        // common/min操作。计算最小值
        commonOutputPrefix = fst.outputs.common(value, lastOutput);
        assert validOutput(commonOutputPrefix);

        // subtract操作。计算余量
        // 如果当前输入的Value大于等于edge的output，最小值为output，output余量为0，后面的操作不会对之前的输入有影响
        // 如果当前输入的Value小于edge的output，最小值为Value，output余量为差值。后面的操作会将output余量加入到后续的边
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);  // output - common
        assert validOutput(wordSuffix);

        // 设置公共的arc的output。
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix);
        // 将余量加到子节点的每个出箭头上
        node.prependOutput(wordSuffix);
      } else {
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      // 计算还有多少Value要加到后面的边上。当输入的Value大于edge的output时，更新后的value才不为0
      // 如果更新后value为0且后面还有公共前缀，则后续循环会更新后面的公共前缀为0，最后会将余量加入到分叉口
      // 树的递归算法思路，每次只考虑一个节点。递归算法参考方法addForPathOutput
      value = fst.outputs.subtract(value, commonOutputPrefix);  // value - common
      assert validOutput(value);
    }

    // 将value余量加到新路径分叉处
    // lastInput和input相同
    if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
      // same input more than 1 time in a row, mapping to multiple outputs
      lastNode.output = fst.outputs.merge(lastNode.output, value);
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1 - 1].setLastOutput(
          input.ints[input.offset + prefixLenPlus1 - 1], value);
    }

    // save last input
    lastInput.copyInts(input);

    // System.out.println("  count[0]=" + frontier[0].inputCount);
  }

  /**
   * 新增input时递归更新output的实现
   *
   * 将拓扑看做一个树，value需要增加到最右面的一条路径
   *
   * @param input
   * @param value
   * @throws IOException
   */
  public T updateOutputRecursively(IntsRef input, T value, int idx, int prefixLen) throws IOException {
    if (idx >= prefixLen) {
      return value;
    }

    // 获取最右边路径的output
    T output = frontier[idx].getLastOutput(input.ints[input.offset + idx]);

    // fast path
    if (output == NO_OUTPUT) {
      T remainingValue = fst.outputs.subtract(value, NO_OUTPUT);
      // 处理最右子节点
      return updateOutputRecursively(input, remainingValue, idx + 1, prefixLen);
    }

    // 获取common值
    T common = fst.outputs.common(value, output);
    // 获取更新output的余量
    T remainingOutput = fst.outputs.subtract(output, common);
    // 获取更新value的余量
    T remainingValue = fst.outputs.subtract(value, common);

    // 获取最右子节点
    int arcNum = frontier[idx].numArcs;
    UnCompiledNode<T> mostRightChildNode = (UnCompiledNode<T>) frontier[idx].arcs[arcNum - 1].target;

    // 更新最右路径为common
    frontier[idx].setLastOutput(input.ints[input.offset + idx], common);
    // 最右子节点的输出全部加output余量
    mostRightChildNode.prependOutput(remainingOutput);

    // 处理最右子节点
    return updateOutputRecursively(input, remainingValue, idx + 1, prefixLen);
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST. NOTE: this will return null if nothing is accepted by the FST. */
  public FST<T> compile() throws IOException {

    final UnCompiledNode<T> root = frontier[0];

    // minimize nodes in the last word's suffix
    freezeTail(0);
    if (root.inputCount < minSuffixCount1
        || root.inputCount < minSuffixCount2
        || root.numArcs == 0) {
      if (fst.emptyOutput == null) {
        return null;
      } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
        // empty string got pruned
        return null;
      }
    } else {
      if (minSuffixCount2 != 0) {
        compileAllTargets(root, lastInput.length());
      }
    }
    // if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + "
    // root.output=" + root.output);
    fst.finish(compileNode(root, lastInput.length()).node);

    return fst;
  }

  private void compileAllTargets(UnCompiledNode<T> node, int tailLength) throws IOException {
    for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
      final Arc<T> arc = node.arcs[arcIdx];
      if (!arc.target.isCompiled()) {
        // not yet compiled
        @SuppressWarnings({"rawtypes", "unchecked"})
        final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
        if (n.numArcs == 0) {
          // System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
          arc.isFinal = n.isFinal = true;
        }
        arc.target = compileNode(n, tailLength - 1);
      }
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) arc. */
  static class Arc<T> {
    int label; // really an "unsigned" byte
    Node target;
    boolean isFinal;
    // 该arc的output/weight
    T output;
    T nextFinalOutput;  // 如果某个term是另一个term的前缀，且output不一样，则该term最后的arc的该field保存output分量
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  interface Node {
    boolean isCompiled();
  }

  public long fstRamBytesUsed() {
    return fst.ramBytesUsed();
  }

  static final class CompiledNode implements Node {
    long node;

    @Override
    public boolean isCompiled() {
      return true;
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) Node. */
  static final class UnCompiledNode<T> implements Node {
    final FSTCompiler<T> owner;
    int numArcs;
    Arc<T>[] arcs;
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...
    T output;  // end node的输出
    boolean isFinal;
    // 输入arc的个数
    long inputCount;

    /** This node's depth, starting from the automaton root. */
    final int depth;

    /**
     * @param depth The node's depth starting from the automaton root. Needed for LUCENE-2934 (node
     *     expansion based on conditions other than the fanout size).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    UnCompiledNode(FSTCompiler<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    @Override
    public boolean isCompiled() {
      return false;
    }

    void clear() {
      numArcs = 0;
      isFinal = false;
      output = owner.NO_OUTPUT;
      inputCount = 0;

      // We don't clear the depth here because it never changes
      // for nodes on the frontier (even when reused).
    }

    T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs - 1].label == labelToMatch;
      return arcs[numArcs - 1].output;
    }

    void addArc(int label, Node target) {
      assert label >= 0;
      assert numArcs == 0 || label > arcs[numArcs - 1].label
          : "arc[numArcs-1].label="
              + arcs[numArcs - 1].label
              + " new label="
              + label
              + " numArcs="
              + numArcs;
      if (numArcs == arcs.length) {
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs);
        for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
          newArcs[arcIdx] = new Arc<>();
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }

    void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target;
      // assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }

    void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs - 1].label;
      assert target == arcs[numArcs - 1].target;
      numArcs--;
    }

    void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }

    // pushes an output prefix forward onto all arcs
    void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);

      // 将余量加入到所有的出边
      for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
        assert owner.validOutput(arcs[arcIdx].output);
      }

      // 如果当前节点是某个输入的结束节点，node.output也累加余量
      if (isFinal) {
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }

  /**
   * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
   */
  static class FixedLengthArcsBuffer {

    // Initial capacity is the max length required for the header of a node with fixed length arcs:
    // header(byte) + numArcs(vint) + numBytes(vint)
    private byte[] bytes = new byte[11];
    private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

    /** Ensures the capacity of the internal byte array. Enlarges it if needed. */
    FixedLengthArcsBuffer ensureCapacity(int capacity) {
      if (bytes.length < capacity) {
        bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
        bado.reset(bytes);
      }
      return this;
    }

    FixedLengthArcsBuffer resetPosition() {
      bado.reset(bytes);
      return this;
    }

    FixedLengthArcsBuffer writeByte(byte b) {
      bado.writeByte(b);
      return this;
    }

    FixedLengthArcsBuffer writeVInt(int i) {
      try {
        bado.writeVInt(i);
      } catch (IOException e) { // Never thrown.
        throw new RuntimeException(e);
      }
      return this;
    }

    int getPosition() {
      return bado.getPosition();
    }

    /** Gets the internal byte array. */
    byte[] getBytes() {
      return bytes;
    }
  }
}
