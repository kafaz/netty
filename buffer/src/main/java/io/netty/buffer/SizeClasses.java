/*
 * Copyright 2020 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.buffer;

import static io.netty.buffer.PoolThreadCache.*;

/**
 * Netty内存池系统的内存大小规格化核心组件。
 * <p>
 * SizeClasses负责将任意内存请求大小映射到预定义的标准内存规格，是Netty内存池高效管理的基础。
 * 该类实现了基于jemalloc内存分配器设计理念的内存规格化算法，通过精心设计的映射表和计算方法，
 * 在内存利用效率和分配速度之间取得平衡。
 * </p>
 * 
 * <h2>核心概念</h2>
 * <p>
 * SizeClasses定义了以下关键概念：
 * <ul>
 * <li><b>大小类别</b>：将所有可能的内存请求大小划分为有限的标准大小</li>
 * <li><b>sizeIdx</b>：每个标准大小对应的索引值，用于快速查找和分类</li>
 * <li><b>子页</b>：小于一个页大小的内存块，通过特殊的子页机制管理</li>
 * <li><b>规格表</b>：记录所有标准大小的完整映射表，包含各种元数据</li>
 * </ul>
 * </p>
 * 
 * <h2>内存规格计算原理</h2>
 * <p>
 * SizeClasses使用以下公式计算标准内存大小：
 * 
 * <pre>
 * size = 1 << log2Group + nDelta * (1 << log2Delta)
 * </pre>
 * 
 * 其中：
 * <ul>
 * <li><b>log2Group</b>：基础大小组的对数值，决定了该组的基础大小</li>
 * <li><b>log2Delta</b>：增量步长的对数值，决定了同组内不同大小的步进</li>
 * <li><b>nDelta</b>：增量倍数，决定了在基础大小上增加多少个增量</li>
 * </ul>
 * 
 * 这种设计具有以下特点：
 * <ul>
 * <li>小规格内存使用较小的增量步长，减少内存浪费</li>
 * <li>大规格内存使用较大的增量步长，简化管理复杂度</li>
 * <li>整体呈现近似指数增长，适合各种大小的内存请求</li>
 * </ul>
 * </p>
 * 
 * <h2>主要属性说明</h2>
 * <p>
 * <ul>
 * <li><b>LOG2_SIZE_CLASS_GROUP</b>：每个大小翻倍的大小类别数量的对数值</li>
 * <li><b>LOG2_MAX_LOOKUP_SIZE</b>：查找表中最大大小类别的对数值</li>
 * <li><b>nSubpages</b>：子页大小类别的数量，用于管理小内存</li>
 * <li><b>nSizes</b>：总的大小类别数量</li>
 * <li><b>nPSizes</b>：页大小整数倍的大小类别数量</li>
 * <li><b>smallMaxSizeIdx</b>：最大的小内存大小类别索引</li>
 * <li><b>lookupMaxClass</b>：查找表包含的最大大小类别</li>
 * <li><b>log2NormalMinClass</b>：最小普通大小类别的对数值</li>
 * </ul>
 * </p>
 * 
 * <h2>规格表结构</h2>
 * <p>
 * 规格表是一个多维数组，每行包含以下元素：
 * <ol>
 * <li><b>index</b>：大小类别索引，即sizeIdx</li>
 * <li><b>log2Group</b>：组基础大小的对数值</li>
 * <li><b>log2Delta</b>：增量步长的对数值</li>
 * <li><b>nDelta</b>：增量倍数</li>
 * <li><b>isMultiPageSize</b>：是否为页大小的整数倍</li>
 * <li><b>isSubPage</b>：是否为子页大小</li>
 * <li><b>log2DeltaLookup</b>：查找表大小类别的log2Delta值</li>
 * </ol>
 * </p>
 * 
 * <h2>内存分类</h2>
 * <p>
 * SizeClasses将内存分为三类：
 * <ul>
 * <li><b>小内存</b>：小于等于smallMaxSizeIdx对应的大小，通常在8B-4KB之间</li>
 * <li><b>普通内存</b>：大于小内存且小于一个chunk大小(通常是16MB)</li>
 * <li><b>大内存</b>：大于chunk大小的内存请求</li>
 * </ul>
 * </p>
 * 
 * <h2>主要计算流程</h2>
 * <p>
 * 1. <b>size2SizeIdx</b>：将请求大小转换为大小类别索引
 * <ul>
 * <li>对于小内存：查表快速定位</li>
 * <li>对于普通内存：通过计算定位</li>
 * <li>对于大内存：返回特殊标记</li>
 * </ul>
 * </p>
 * <p>
 * 2. <b>sizeIdx2size</b>：将大小类别索引转换为实际内存大小
 * <ul>
 * <li>通过规格表直接查询各项参数</li>
 * <li>使用公式：size = 1 << log2Group + nDelta * (1 << log2Delta)计算实际大小</li>
 * </ul>
 * </p>
 * <p>
 * 3. <b>normalizeSize</b>：将任意大小规范化为标准大小
 * <ul>
 * <li>首先调用size2SizeIdx获取大小类别索引</li>
 * <li>然后调用sizeIdx2size获取标准化大小</li>
 * </ul>
 * </p>
 * 
 * <p>
 * 示例规格表(pageShift = 13时的部分规格):
 * 
 * <pre>
 * (index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage, log2DeltaLookup)
 * 
 * ( 0, 4, 4, 0, no, yes, 4)  // 16B
 * ( 1, 4, 4, 1, no, yes, 4)  // 32B
 * ( 2, 4, 4, 2, no, yes, 4)  // 48B
 * ( 3, 4, 4, 3, no, yes, 4)  // 64B
 * 
 * ( 4, 6, 4, 1, no, yes, 4)  // 80B
 * ...
 * 
 * ( 76, 24, 22, 1, yes, no, no)  // 大内存
 * </pre>
 * </p>
 * 
 * <p>
 * 通过这种精心设计的规格系统，Netty能够高效地管理各种大小的内存请求，
 * 在内存利用率和分配速度之间取得良好的平衡。
 * </p>
 * 
 * @see PoolArena
 * @see PoolChunk
 * @see PoolSubpage
 */
final class SizeClasses implements SizeClassesMetric {

    static final int LOG2_QUANTUM = 4;

    private static final int LOG2_SIZE_CLASS_GROUP = 2;
    private static final int LOG2_MAX_LOOKUP_SIZE = 12;

    private static final int LOG2GROUP_IDX = 1;
    private static final int LOG2DELTA_IDX = 2;
    private static final int NDELTA_IDX = 3;
    private static final int PAGESIZE_IDX = 4;
    private static final int SUBPAGE_IDX = 5;
    private static final int LOG2_DELTA_LOOKUP_IDX = 6;

    private static final byte no = 0, yes = 1;

    final int pageSize;
    final int pageShifts;
    final int chunkSize;
    final int directMemoryCacheAlignment;

    final int nSizes;
    final int nSubpages;
    final int nPSizes;
    final int lookupMaxSize;
    final int smallMaxSizeIdx;

    private final int[] pageIdx2sizeTab;

    // lookup table for sizeIdx <= smallMaxSizeIdx
    private final int[] sizeIdx2sizeTab;

    // lookup table used for size <= lookupMaxClass
    // spacing is 1 << LOG2_QUANTUM, so the size of array is lookupMaxClass >>
    // LOG2_QUANTUM
    private final int[] size2idxTab;

    SizeClasses(int pageSize, int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
        int group = log2(chunkSize) - LOG2_QUANTUM - LOG2_SIZE_CLASS_GROUP + 1;

        // generate size classes
        // [index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage,
        // log2DeltaLookup]
        short[][] sizeClasses = new short[group << LOG2_SIZE_CLASS_GROUP][7];

        int normalMaxSize = -1;
        int nSizes = 0;
        int size = 0;

        int log2Group = LOG2_QUANTUM;
        int log2Delta = LOG2_QUANTUM;
        int ndeltaLimit = 1 << LOG2_SIZE_CLASS_GROUP;

        // First small group, nDelta start at 0.
        // first size class is 1 << LOG2_QUANTUM
        for (int nDelta = 0; nDelta < ndeltaLimit; nDelta++, nSizes++) {
            short[] sizeClass = newSizeClass(nSizes, log2Group, log2Delta, nDelta, pageShifts);
            sizeClasses[nSizes] = sizeClass;
            size = sizeOf(sizeClass, directMemoryCacheAlignment);
        }

        log2Group += LOG2_SIZE_CLASS_GROUP;

        // All remaining groups, nDelta start at 1.
        for (; size < chunkSize; log2Group++, log2Delta++) {
            for (int nDelta = 1; nDelta <= ndeltaLimit && size < chunkSize; nDelta++, nSizes++) {
                short[] sizeClass = newSizeClass(nSizes, log2Group, log2Delta, nDelta, pageShifts);
                sizeClasses[nSizes] = sizeClass;
                size = normalMaxSize = sizeOf(sizeClass, directMemoryCacheAlignment);
            }
        }

        // chunkSize must be normalMaxSize
        assert chunkSize == normalMaxSize;

        int smallMaxSizeIdx = 0;
        int lookupMaxSize = 0;
        int nPSizes = 0;
        int nSubpages = 0;
        for (int idx = 0; idx < nSizes; idx++) {
            short[] sz = sizeClasses[idx];
            if (sz[PAGESIZE_IDX] == yes) {
                nPSizes++;
            }
            if (sz[SUBPAGE_IDX] == yes) {
                nSubpages++;
                smallMaxSizeIdx = idx;
            }
            if (sz[LOG2_DELTA_LOOKUP_IDX] != no) {
                lookupMaxSize = sizeOf(sz, directMemoryCacheAlignment);
            }
        }
        this.smallMaxSizeIdx = smallMaxSizeIdx;
        this.lookupMaxSize = lookupMaxSize;
        this.nPSizes = nPSizes;
        this.nSubpages = nSubpages;
        this.nSizes = nSizes;

        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.directMemoryCacheAlignment = directMemoryCacheAlignment;

        // generate lookup tables
        this.sizeIdx2sizeTab = newIdx2SizeTab(sizeClasses, nSizes, directMemoryCacheAlignment);
        this.pageIdx2sizeTab = newPageIdx2sizeTab(sizeClasses, nSizes, nPSizes, directMemoryCacheAlignment);
        this.size2idxTab = newSize2idxTab(lookupMaxSize, sizeClasses);
    }

    // calculate size class
    private static short[] newSizeClass(int index, int log2Group, int log2Delta, int nDelta, int pageShifts) {
        short isMultiPageSize;
        if (log2Delta >= pageShifts) {
            isMultiPageSize = yes;
        } else {
            int pageSize = 1 << pageShifts;
            int size = calculateSize(log2Group, nDelta, log2Delta);

            isMultiPageSize = size == size / pageSize * pageSize ? yes : no;
        }

        int log2Ndelta = nDelta == 0 ? 0 : log2(nDelta);

        byte remove = 1 << log2Ndelta < nDelta ? yes : no;

        int log2Size = log2Delta + log2Ndelta == log2Group ? log2Group + 1 : log2Group;
        if (log2Size == log2Group) {
            remove = yes;
        }

        short isSubpage = log2Size < pageShifts + LOG2_SIZE_CLASS_GROUP ? yes : no;

        int log2DeltaLookup = log2Size < LOG2_MAX_LOOKUP_SIZE ||
                log2Size == LOG2_MAX_LOOKUP_SIZE && remove == no
                        ? log2Delta
                        : no;

        return new short[] {
                (short) index, (short) log2Group, (short) log2Delta,
                (short) nDelta, isMultiPageSize, isSubpage, (short) log2DeltaLookup
        };
    }

    private static int[] newIdx2SizeTab(short[][] sizeClasses, int nSizes, int directMemoryCacheAlignment) {
        int[] sizeIdx2sizeTab = new int[nSizes];

        for (int i = 0; i < nSizes; i++) {
            short[] sizeClass = sizeClasses[i];
            sizeIdx2sizeTab[i] = sizeOf(sizeClass, directMemoryCacheAlignment);
        }
        return sizeIdx2sizeTab;
    }

    private static int calculateSize(int log2Group, int nDelta, int log2Delta) {
        return (1 << log2Group) + (nDelta << log2Delta);
    }

    private static int sizeOf(short[] sizeClass, int directMemoryCacheAlignment) {
        int log2Group = sizeClass[LOG2GROUP_IDX];
        int log2Delta = sizeClass[LOG2DELTA_IDX];
        int nDelta = sizeClass[NDELTA_IDX];

        int size = calculateSize(log2Group, nDelta, log2Delta);

        return alignSizeIfNeeded(size, directMemoryCacheAlignment);
    }

    private static int[] newPageIdx2sizeTab(short[][] sizeClasses, int nSizes, int nPSizes,
            int directMemoryCacheAlignment) {
        int[] pageIdx2sizeTab = new int[nPSizes];
        int pageIdx = 0;
        for (int i = 0; i < nSizes; i++) {
            short[] sizeClass = sizeClasses[i];
            if (sizeClass[PAGESIZE_IDX] == yes) {
                pageIdx2sizeTab[pageIdx++] = sizeOf(sizeClass, directMemoryCacheAlignment);
            }
        }
        return pageIdx2sizeTab;
    }

    private static int[] newSize2idxTab(int lookupMaxSize, short[][] sizeClasses) {
        int[] size2idxTab = new int[lookupMaxSize >> LOG2_QUANTUM];
        int idx = 0;
        int size = 0;

        for (int i = 0; size <= lookupMaxSize; i++) {
            int log2Delta = sizeClasses[i][LOG2DELTA_IDX];
            int times = 1 << log2Delta - LOG2_QUANTUM;

            while (size <= lookupMaxSize && times-- > 0) {
                size2idxTab[idx++] = i;
                size = idx + 1 << LOG2_QUANTUM;
            }
        }
        return size2idxTab;
    }

    @Override
    public int sizeIdx2size(int sizeIdx) {
        return sizeIdx2sizeTab[sizeIdx];
    }

    @Override
    public int sizeIdx2sizeCompute(int sizeIdx) {
        int group = sizeIdx >> LOG2_SIZE_CLASS_GROUP;
        int mod = sizeIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        int groupSize = group == 0 ? 0 : 1 << LOG2_QUANTUM + LOG2_SIZE_CLASS_GROUP - 1 << group;

        int shift = group == 0 ? 1 : group;
        int lgDelta = shift + LOG2_QUANTUM - 1;
        int modSize = mod + 1 << lgDelta;

        return groupSize + modSize;
    }

    @Override
    public long pageIdx2size(int pageIdx) {
        return pageIdx2sizeTab[pageIdx];
    }

    @Override
    public long pageIdx2sizeCompute(int pageIdx) {
        int group = pageIdx >> LOG2_SIZE_CLASS_GROUP;
        int mod = pageIdx & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        long groupSize = group == 0 ? 0 : 1L << pageShifts + LOG2_SIZE_CLASS_GROUP - 1 << group;

        int shift = group == 0 ? 1 : group;
        int log2Delta = shift + pageShifts - 1;
        int modSize = mod + 1 << log2Delta;

        return groupSize + modSize;
    }

    /**
     * 将请求的内存大小转换为对应的sizeIdx
     * 这是Netty内存分配中的关键算法，用于确定内存分配的大小类别
     * 
     * @param size 请求的内存大小
     * @return 对应的sizeIdx
     */
    @Override
    public int size2SizeIdx(int size) {
        // 1. 处理特殊情况
        if (size == 0) {
            return 0;
        }
        if (size > chunkSize) {
            return nSizes; // 超过chunkSize的请求返回最大sizeIdx
        }

        // 2. 内存对齐处理
        size = alignSizeIfNeeded(size, directMemoryCacheAlignment);

        // 3. 小对象快速查找
        if (size <= lookupMaxSize) {
            // 使用查找表快速定位sizeIdx
            return size2idxTab[size - 1 >> LOG2_QUANTUM];
        }

        // 4. 大对象计算sizeIdx
        // 计算log2(size*2-1)，用于确定sizeIdx的组
        int x = log2((size << 1) - 1);

        // 计算位移量
        int shift = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? 0
                : x - (LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM);

        // 计算组号
        int group = shift << LOG2_SIZE_CLASS_GROUP;

        // 计算组内偏移
        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM
                : x - LOG2_SIZE_CLASS_GROUP - 1;

        // 计算组内索引
        int mod = size - 1 >> log2Delta & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        // 返回最终的sizeIdx
        return group + mod;
    }

    @Override
    public int pages2pageIdx(int pages) {
        return pages2pageIdxCompute(pages, false);
    }

    @Override
    public int pages2pageIdxFloor(int pages) {
        return pages2pageIdxCompute(pages, true);
    }

    private int pages2pageIdxCompute(int pages, boolean floor) {
        int pageSize = pages << pageShifts;
        if (pageSize > chunkSize) {
            return nPSizes;
        }

        int x = log2((pageSize << 1) - 1);

        int shift = x < LOG2_SIZE_CLASS_GROUP + pageShifts
                ? 0
                : x - (LOG2_SIZE_CLASS_GROUP + pageShifts);

        int group = shift << LOG2_SIZE_CLASS_GROUP;

        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + pageShifts + 1 ? pageShifts : x - LOG2_SIZE_CLASS_GROUP - 1;

        int mod = pageSize - 1 >> log2Delta & (1 << LOG2_SIZE_CLASS_GROUP) - 1;

        int pageIdx = group + mod;

        if (floor && pageIdx2sizeTab[pageIdx] > pages << pageShifts) {
            pageIdx--;
        }

        return pageIdx;
    }

    // Round size up to the nearest multiple of alignment.
    private static int alignSizeIfNeeded(int size, int directMemoryCacheAlignment) {
        if (directMemoryCacheAlignment <= 0) {
            return size;
        }
        int delta = size & directMemoryCacheAlignment - 1;
        return delta == 0 ? size : size + directMemoryCacheAlignment - delta;
    }

    @Override
    public int normalizeSize(int size) {
        if (size == 0) {
            return sizeIdx2sizeTab[0];
        }
        size = alignSizeIfNeeded(size, directMemoryCacheAlignment);
        if (size <= lookupMaxSize) {
            int ret = sizeIdx2sizeTab[size2idxTab[size - 1 >> LOG2_QUANTUM]];
            assert ret == normalizeSizeCompute(size);
            return ret;
        }
        return normalizeSizeCompute(size);
    }

    private static int normalizeSizeCompute(int size) {
        int x = log2((size << 1) - 1);
        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM
                : x - LOG2_SIZE_CLASS_GROUP - 1;
        int delta = 1 << log2Delta;
        int delta_mask = delta - 1;
        return size + delta_mask & ~delta_mask;
    }
}
