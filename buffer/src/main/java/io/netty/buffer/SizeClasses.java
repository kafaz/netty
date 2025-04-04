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

    /**
     * 量子大小的对数值，决定了最小内存分配单位。
     * <p>
     * LOG2_QUANTUM = 4表示最小分配单位为2^4 = 16字节。这是内存分配的基本粒度，
     * 所有的内存请求都会被上调至该值的整数倍。该常量影响内存对齐、查找表索引计算
     * 以及运行块(run)的最小大小。
     * </p>
     */
    static final int LOG2_QUANTUM = 4;

    /**
     * 每组大小类别的数量的对数值。
     * <p>
     * LOG2_SIZE_CLASS_GROUP = 2表示每组包含2^2 = 4个大小类别。这是jemalloc风格内存分配器的
     * 核心参数之一，用于组织不同规格的内存大小。较小的值提供更精细的大小粒度，而较大的值
     * 简化了查找过程但会增加内部碎片。
     * </p>
     */
    private static final int LOG2_SIZE_CLASS_GROUP = 2;

    /**
     * 查找表中最大大小类别的对数值。
     * <p>
     * LOG2_MAX_LOOKUP_SIZE = 12表示查找表覆盖的最大内存大小为2^12 = 4096字节。
     * 超过此大小的内存请求将使用计算方式而非查表方式来确定大小类别，这是空间与时间的权衡。
     * </p>
     */
    private static final int LOG2_MAX_LOOKUP_SIZE = 12;

    /**
     * 大小类别规格表中基础大小组对数值的索引位置。
     * <p>
     * 在规格表的每一行中，位置1存储log2Group值，表示该大小类别的基础大小的对数值。
     * 它是计算实际内存大小的核心参数之一。
     * </p>
     */
    private static final int LOG2GROUP_IDX = 1;

    /**
     * 大小类别规格表中增量步长对数值的索引位置。
     * <p>
     * 在规格表的每一行中，位置2存储log2Delta值，表示同组内不同大小间的增量步长的对数值。
     * 例如，log2Delta = 4表示增量步长为16字节。
     * </p>
     */
    private static final int LOG2DELTA_IDX = 2;

    /**
     * 大小类别规格表中增量倍数的索引位置。
     * <p>
     * 在规格表的每一行中，位置3存储nDelta值，表示在基础大小上增加的增量数量。
     * 实际大小计算公式为: size = (1 << log2Group) + (nDelta << log2Delta)
     * </p>
     */
    private static final int NDELTA_IDX = 3;

    /**
     * 大小类别规格表中页大小标志的索引位置。
     * <p>
     * 在规格表的每一行中，位置4存储isMultiPageSize标志，表示该大小是否为页大小的整数倍。
     * 值为1(yes)表示是页大小的整数倍，0(no)表示不是。这影响内存分配的路径选择。
     * </p>
     */
    private static final int PAGESIZE_IDX = 4;

    /**
     * 大小类别规格表中子页标志的索引位置。
     * <p>
     * 在规格表的每一行中，位置5存储isSubpage标志，表示该大小是否为子页大小(小于pageSize)。
     * 值为1(yes)表示是子页大小，需要使用子页分配机制；0(no)表示是普通或大内存，使用运行块分配。
     * </p>
     */
    private static final int SUBPAGE_IDX = 5;

    /**
     * 大小类别规格表中查找表增量值的索引位置。
     * <p>
     * 在规格表的每一行中，位置6存储log2DeltaLookup值，用于确定该大小类别是否包含在查找表中。
     * 非no值表示包含在查找表中，no值表示不包含（需要计算）。
     * </p>
     */
    private static final int LOG2_DELTA_LOOKUP_IDX = 6;

    /**
     * 用于表示布尔值的字节常量。
     * <p>
     * no(0)表示否定值，yes(1)表示肯定值。这些常量用于规格表中的各种标志位，
     * 使代码更具可读性并节省内存（相比使用boolean类型）。
     * </p>
     */
    private static final byte no = 0, yes = 1;

    /**
     * 页面大小，单位为字节。
     * <p>
     * 这是内存池系统的基本分配单位，通常为8KB (8192字节)。页面是子页划分和运行块分配的基础单位，
     * 影响内存分配的粒度和效率。页面大小越大，适合大内存分配；页面大小越小，浪费更少但管理开销更大。
     * </p>
     */
    final int pageSize;

    /**
     * 页面大小的位移值，即log2(pageSize)。
     * <p>
     * 例如，对于8KB的页面大小，pageShifts = 13 (2^13 = 8192)。使用位移值可以通过位运算快速计算
     * 页面数量和内存地址，避免较慢的乘除法运算。这是内存管理中常用的优化技术。
     * </p>
     */
    final int pageShifts;

    /**
     * chunk大小，单位为字节。
     * <p>
     * chunk是内存池分配的最大连续内存单元，通常为16MB。一个chunk包含多个页面，用于分配中大型内存。
     * 所有大于chunkSize的内存请求被视为huge内存，会通过特殊路径处理。chunkSize也是内存规格表的上限。
     * </p>
     */
    final int chunkSize;

    /**
     * 直接内存缓存对齐大小。
     * <p>
     * 用于确保内存分配按特定边界对齐，通常是CPU缓存行大小的倍数(如64字节)。
     * 正确的内存对齐可以显著提高内存访问性能，特别是在需要高性能的网络应用中。
     * 值为0或负数表示不需要特殊对齐。
     * </p>
     */
    final int directMemoryCacheAlignment;

    /**
     * 总的大小类别数量。
     * <p>
     * 表示内存规格表中定义的标准内存大小的总数。这决定了内存分配的精度和范围，
     * 从最小的(通常16字节)到最大的(chunkSize)。nSizes直接影响内存管理的精细度和复杂性。
     * </p>
     */
    final int nSizes;

    /**
     * 子页大小类别的数量。
     * <p>
     * 表示小于页面大小的标准内存大小的数量。这些大小类别使用特殊的子页机制管理，
     * 能高效处理小内存分配请求。nSubpages决定了smallSubpagePools数组的大小和子页分配的多样性。
     * </p>
     */
    final int nSubpages;

    /**
     * 页大小整数倍的大小类别数量。
     * <p>
     * 表示那些恰好是页面大小整数倍的标准内存大小的数量。这些大小使用页面级别的分配策略，
     * 无需进一步细分，适合中型内存请求。此值影响pageIdx2sizeTab数组的大小。
     * </p>
     */
    final int nPSizes;

    /**
     * 查找表覆盖的最大内存大小。
     * <p>
     * 小于等于此值的内存请求可以使用快速查找表获得对应的大小类别，无需计算。
     * 这是空间换时间的优化，典型值为4KB。lookupMaxSize越大，查找表越大，但查找越快。
     * </p>
     */
    final int lookupMaxSize;

    /**
     * 最大的小内存大小类别索引。
     * <p>
     * 表示最大的需要使用子页机制分配的内存大小对应的索引值。这是区分小内存和普通内存的边界点，
     * 对应的实际大小通常接近但小于pageSize。此值用于快速判断分配路径。
     * </p>
     */
    final int smallMaxSizeIdx;

    /**
     * 页索引到实际大小的映射表。
     * <p>
     * 存储每个页大小整数倍的标准内存大小，用于快速查找页面索引对应的实际内存大小。
     * 数组长度为nPSizes，按页索引增序排列。用于在页级别分配时确定内存块大小。
     * </p>
     */
    private final int[] pageIdx2sizeTab;

    /**
     * 大小类别索引到实际大小的映射表。
     * <p>
     * 存储每个大小类别索引对应的标准内存大小，用于快速从sizeIdx获取实际分配的字节数。
     * 数组长度为nSizes，包含从最小到最大的所有标准内存大小。这是内存规格化的核心数据结构。
     * </p>
     */
    private final int[] sizeIdx2sizeTab;

    /**
     * 实际大小到大小类别索引的映射表。
     * <p>
     * 用于小内存请求(≤lookupMaxSize)的快速查找，提供O(1)时间复杂度的大小到索引转换。
     * 数组大小为lookupMaxSize >> LOG2_QUANTUM，按照量子大小(16字节)间隔组织。
     * 这是size2SizeIdx方法中小对象快速路径的关键数据结构。
     * </p>
     */
    private final int[] size2idxTab;

    /**
     * SizeClasses类的构造函数，负责初始化Netty内存池系统的大小类别表和查找表。
     * <p>
     * 该构造函数是Netty内存池系统的核心初始化过程，通过精确计算生成一系列标准化的内存大小类别，
     * 建立高效的查找表结构，为后续内存分配提供基础。整个初始化过程遵循jemalloc的设计理念，
     * 通过组号、组内偏移等机制实现内存大小的分层管理。
     * </p>
     * 
     * <h3>初始化流程</h3>
     * <ol>
     * <li>根据chunk大小计算所需的大小类别组数</li>
     * <li>生成所有大小类别规格，从小到大构建完整规格表</li>
     * <li>扫描规格表，统计子页、页面整数倍等特殊规格数量</li>
     * <li>构建三种查找表，用于不同场景的快速大小转换</li>
     * </ol>
     * 
     * <h3>查找表说明</h3>
     * <ul>
     * <li><b>sizeIdx2sizeTab</b>：从大小类别索引映射到实际内存大小</li>
     * <li><b>pageIdx2sizeTab</b>：从页索引映射到页整数倍的内存大小</li>
     * <li><b>size2idxTab</b>：从实际内存大小映射到大小类别索引（仅覆盖小内存范围）</li>
     * </ul>
     *
     * @param pageSize                   页面大小，通常为8KB (8192字节)
     * @param pageShifts                 页面大小的位移值，即log2(pageSize)，通常为13
     * @param chunkSize                  chunk大小，通常为16MB (16777216字节)
     * @param directMemoryCacheAlignment 直接内存缓存对齐大小，通常与CPU缓存行大小相关
     * 
     * @see #newSizeClass(int, int, int, int, int)
     * @see #newIdx2SizeTab(short[][], int, int)
     * @see #newPageIdx2sizeTab(short[][], int, int, int)
     * @see #newSize2idxTab(int, short[][])
     */
    SizeClasses(int pageSize, int pageShifts, int chunkSize, int directMemoryCacheAlignment) {
        // 计算需要多少组大小类别
        // group值决定了总共需要生成多少组大小类别，每组有2^LOG2_SIZE_CLASS_GROUP个类别
        int group = log2(chunkSize) - LOG2_QUANTUM - LOG2_SIZE_CLASS_GROUP + 1;

        // 初始化大小类别规格表
        // 规格表每行存储7个属性：[index, log2Group, log2Delta, nDelta, isMultiPageSize, isSubPage,
        // log2DeltaLookup]
        // 每个大小类别由这7个属性完全定义
        short[][] sizeClasses = new short[group << LOG2_SIZE_CLASS_GROUP][7];

        // 初始化变量
        int normalMaxSize = -1; // 记录普通大小类别的最大值
        int nSizes = 0; // 大小类别总数
        int size = 0; // 当前计算的大小值

        // 设置初始值 - 第一组使用最小量子大小作为基础
        int log2Group = LOG2_QUANTUM; // 第一组的基础大小对数值
        int log2Delta = LOG2_QUANTUM; // 第一组的增量步长对数值
        int ndeltaLimit = 1 << LOG2_SIZE_CLASS_GROUP; // 每组的大小类别数量上限

        // 生成第一组小规格大小类别，nDelta从0开始（特殊情况）
        // 这组生成最小的几个大小类别，如16B,32B,48B,64B
        for (int nDelta = 0; nDelta < ndeltaLimit; nDelta++, nSizes++) {
            // 为每个nDelta值创建一个大小类别规格
            short[] sizeClass = newSizeClass(nSizes, log2Group, log2Delta, nDelta, pageShifts);
            sizeClasses[nSizes] = sizeClass;
            // 计算该规格的实际大小
            size = sizeOf(sizeClass, directMemoryCacheAlignment);
        }

        // 增加基础大小和增量步长，用于后续组
        log2Group += LOG2_SIZE_CLASS_GROUP; // 增加基础大小

        // 生成所有剩余组的大小类别，每组nDelta从1开始
        // 这些组生成逐渐增大的大小类别，直到达到chunkSize
        for (; size < chunkSize; log2Group++, log2Delta++) {
            // 每组有多个大小类别，由nDelta定义
            for (int nDelta = 1; nDelta <= ndeltaLimit && size < chunkSize; nDelta++, nSizes++) {
                // 为每个组和nDelta值创建一个大小类别规格
                short[] sizeClass = newSizeClass(nSizes, log2Group, log2Delta, nDelta, pageShifts);
                sizeClasses[nSizes] = sizeClass;
                // 计算该规格的实际大小，并记录最大值
                size = normalMaxSize = sizeOf(sizeClass, directMemoryCacheAlignment);
            }
        }

        // 验证生成的最大大小等于chunkSize
        // 这确保我们能完全覆盖从最小单位到整个chunk的所有可能大小
        assert chunkSize == normalMaxSize;

        // 扫描所有生成的大小类别，计算各种属性和特殊类别数量
        int smallMaxSizeIdx = 0; // 最大的小内存大小类别索引
        int lookupMaxSize = 0; // 查找表覆盖的最大大小
        int nPSizes = 0; // 页大小整数倍的规格数量
        int nSubpages = 0; // 子页规格数量

        // 遍历所有生成的大小类别
        for (int idx = 0; idx < nSizes; idx++) {
            short[] sz = sizeClasses[idx];
            // 统计页大小整数倍的规格数量
            if (sz[PAGESIZE_IDX] == yes) {
                nPSizes++;
            }
            // 统计子页规格数量和最大小内存索引
            if (sz[SUBPAGE_IDX] == yes) {
                nSubpages++;
                smallMaxSizeIdx = idx; // 更新最大小内存索引
            }
            // 确定查找表覆盖的最大大小
            if (sz[LOG2_DELTA_LOOKUP_IDX] != no) {
                lookupMaxSize = sizeOf(sz, directMemoryCacheAlignment);
            }
        }

        // 保存计算结果到实例字段
        this.smallMaxSizeIdx = smallMaxSizeIdx;
        this.lookupMaxSize = lookupMaxSize;
        this.nPSizes = nPSizes;
        this.nSubpages = nSubpages;
        this.nSizes = nSizes;

        // 保存基础参数到实例字段
        this.pageSize = pageSize;
        this.pageShifts = pageShifts;
        this.chunkSize = chunkSize;
        this.directMemoryCacheAlignment = directMemoryCacheAlignment;

        // 生成各种查找表，用于快速转换
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

    /**
     * 创建内存大小到大小类别索引的快速查找表。
     * <p>
     * 该方法生成用于小内存请求(≤lookupMaxSize)的O(1)时间复杂度查找表，是内存规格化系统的关键性能优化。
     * 查找表按照量子大小(2^LOG2_QUANTUM)间隔组织，将实际内存大小映射到对应的大小类别索引(sizeIdx)。
     * 该表允许系统在处理小内存请求时避免复杂的对数计算，显著提高分配速度。
     * </p>
     * 
     * <h3>查找表原理</h3>
     * <p>
     * 查找表中的每个条目对应一个量子单位(16字节)的大小增量：
     * <ul>
     * <li>索引0对应16字节(1个量子)</li>
     * <li>索引1对应32字节(2个量子)</li>
     * <li>索引2对应48字节(3个量子)</li>
     * <li>以此类推...</li>
     * </ul>
     * 内存请求时，通过简单的位移运算((size-1) >> LOG2_QUANTUM)即可索引到对应的大小类别。
     * </p>
     * 
     * <h3>生成算法</h3>
     * <p>
     * 算法遍历所有大小类别，按照每个类别的log2Delta值确定其在查找表中占据的条目数量，
     * 确保每个内存大小都映射到合适的大小类别索引。
     * </p>
     *
     * @param lookupMaxSize 查找表覆盖的最大内存大小
     * @param sizeClasses   完整的大小类别规格表
     * @return 内存大小到大小类别索引的映射数组
     */
    private static int[] newSize2idxTab(int lookupMaxSize, short[][] sizeClasses) {
        // 计算查找表大小，除以量子大小(16字节)得到需要多少个表项
        int[] size2idxTab = new int[lookupMaxSize >> LOG2_QUANTUM];
        int idx = 0; // 当前查找表的索引位置
        int size = 0; // 当前处理的内存大小(字节)

        // 遍历所有大小类别，填充查找表
        for (int i = 0; size <= lookupMaxSize; i++) {
            // 获取当前大小类别的增量步长对数值
            int log2Delta = sizeClasses[i][LOG2DELTA_IDX];

            // 计算当前大小类别在查找表中占据的条目数
            // 即计算从当前log2Delta到LOG2_QUANTUM需要多少倍的条目
            int times = 1 << (log2Delta - LOG2_QUANTUM);

            // 填充当前大小类别对应的所有条目
            while (size <= lookupMaxSize && times-- > 0) {
                // 将当前查找表位置映射到大小类别i
                size2idxTab[idx++] = i;

                // 计算下一个要处理的内存大小，以量子大小(16字节)为单位递增
                // (idx + 1) << LOG2_QUANTUM 表示(idx + 1) * 16字节
                size = (idx + 1) << LOG2_QUANTUM;
            }
        }

        // 返回填充完成的查找表
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
     * 将请求的内存大小转换为对应的大小类别索引(sizeIdx)。
     * <p>
     * 这是Netty内存池系统中的核心算法，用于将各种大小的内存请求映射到预定义的标准大小类别。
     * 该方法使用高效的混合策略：对小尺寸使用查找表快速定位，对大尺寸使用对数计算。
     * 最终返回的sizeIdx用于在内存池中定位适合的子页池或运行块。
     * </p>
     * 
     * <h3>主要策略说明</h3>
     * <ul>
     * <li><b>量化处理</b>: 所有内存请求都被规范化为一组有限的标准大小</li>
     * <li><b>内存对齐</b>: 根据系统要求对请求大小进行边界对齐</li>
     * <li><b>阶梯式增长</b>: 小尺寸时增长较细，大尺寸时增长较粗</li>
     * <li><b>上取整策略</b>: 总是分配大于或等于请求大小的内存块</li>
     * </ul>
     * 
     * <h3>性能优化</h3>
     * <ul>
     * <li>小尺寸(≤lookupMaxSize)使用预计算的查找表，时间复杂度O(1)</li>
     * <li>大尺寸使用对数计算，避免了过大的查找表</li>
     * <li>使用位运算代替除法和取模操作，提高计算效率</li>
     * </ul>
     *
     * @param size 请求的内存大小（字节数）
     * @return 对应的大小类别索引(sizeIdx)，范围从0到nSizes-1
     * 
     * @see #alignSizeIfNeeded(int, int)
     * @see #lookup 查找表数据
     */
    @Override
    public int size2SizeIdx(int size) {
        // 1. 处理特殊情况：零大小请求
        if (size == 0) {
            return 0; // 零大小请求映射到最小的大小类别
        }

        // 超大请求，超过整个chunk大小的请求
        if (size > chunkSize) {
            return nSizes; // 返回最大的大小类别索引，表示要使用多个chunk
        }

        // 2. 内存对齐处理
        // 某些平台(如x86)要求内存地址按特定边界对齐以优化访问性能
        // directMemoryCacheAlignment通常是CPU缓存行大小(通常为64字节)的倍数
        size = alignSizeIfNeeded(size, directMemoryCacheAlignment);

        // 3. 小对象快速查找路径
        // lookupMaxSize是预计算查找表覆盖的最大大小
        // 对于小于此值的请求，使用查找表可以O(1)时间获得结果
        if (size <= lookupMaxSize) {
            // size2idxTab是预计算的查找表，将大小映射到对应的sizeIdx
            // (size - 1) >> LOG2_QUANTUM 将大小转换为以量子单位(通常为8字节)为单位的索引
            // 减1是为了处理边界情况，例如大小正好是8的倍数
            return size2idxTab[(size - 1) >> LOG2_QUANTUM];
        }

        // 4. 大对象计算sizeIdx - 使用对数公式计算
        // 对于大于lookupMaxSize的请求，使用基于对数的计算方法

        // 计算log2((size*2)-1)，用于确定大小类别的组
        // 乘2减1的目的是为了上取整到下一个2的幂
        int x = log2((size << 1) - 1);

        // 计算位移量，用于后续组号计算
        // 如果x小于特定阈值，shift为0，否则根据x计算
        // 这是为了处理过渡区域的大小类别
        int shift = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? 0 // 较小的大对象
                : x - (LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM); // 较大的大对象

        // 计算组号 - 每组包含多个连续的大小类别
        // 位移操作相当于乘以SIZE_CLASS_GROUP_SIZE(通常为4)
        int group = shift << LOG2_SIZE_CLASS_GROUP;

        // 计算用于确定组内偏移的对数增量
        // 对于较小的大对象使用固定的LOG2_QUANTUM
        // 对于较大的大对象则根据x动态计算
        int log2Delta = x < LOG2_SIZE_CLASS_GROUP + LOG2_QUANTUM + 1
                ? LOG2_QUANTUM // 较小的大对象使用固定量子大小
                : x - LOG2_SIZE_CLASS_GROUP - 1; // 较大的大对象使用可变增量

        // 计算组内索引，确定在当前组中的具体位置
        // size - 1是为了处理边界情况
        // >> log2Delta将大小归一化为组内索引单位
        // & ((1 << LOG2_SIZE_CLASS_GROUP) - 1)操作相当于对SIZE_CLASS_GROUP_SIZE取模
        int mod = (size - 1) >> log2Delta & ((1 << LOG2_SIZE_CLASS_GROUP) - 1);

        // 返回最终的大小类别索引，由组号和组内索引共同决定
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
