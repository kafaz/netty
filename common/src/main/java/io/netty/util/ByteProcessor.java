/*
 * Copyright 2015 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License, version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at:
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.netty.util;

import static io.netty.util.ByteProcessorUtils.CARRIAGE_RETURN;
import static io.netty.util.ByteProcessorUtils.HTAB;
import static io.netty.util.ByteProcessorUtils.LINE_FEED;
import static io.netty.util.ByteProcessorUtils.SPACE;

/**
 * 提供遍历和处理字节集合的函数式接口
 */
@FunctionalInterface
public interface ByteProcessor {
    /**
     * 包含查找特定字节的处理器实现
     * IndexOfProcessor 的常见应用:
     *  查找特定分隔符（如逗号、分号、换行符）
     *  查找消息边界标记
     *  定位特定控制字符（如 NULL 终止符）
     */
    class IndexOfProcessor implements ByteProcessor {
        private final byte byteToFind;

        public IndexOfProcessor(byte byteToFind) {
            this.byteToFind = byteToFind;
        }

        @Override
        public boolean process(byte value) {
            return value != byteToFind;
        }
    }

    /**
     * 包含查找非特定字节的处理器实现
     * IndexNotOfProcessor 的常见应用:
     *  跳过前导空白字符，找到内容的实际开始
     *  跳过一系列相同的填充字节
     *  在解析文本时，找到非空白字符的位置
     */
    class IndexNotOfProcessor implements ByteProcessor {
        private final byte byteToNotFind;

        public IndexNotOfProcessor(byte byteToNotFind) {
            this.byteToNotFind = byteToNotFind;
        }

        @Override
        public boolean process(byte value) {
            return value == byteToNotFind;
        }
    }

    /**
     * 查找NULL字节(0x00)
     */
    ByteProcessor FIND_NUL = value -> value != 0;

    /**
     * 查找非NULL字节
     */
    ByteProcessor FIND_NON_NUL = value -> value == 0;

    /**
     * 查找回车符('\r')
     */
    ByteProcessor FIND_CR = value -> value != CARRIAGE_RETURN;

    /**
     * 查找非回车符
     */
    ByteProcessor FIND_NON_CR = value -> value == CARRIAGE_RETURN;

    /**
     * 查找换行符('\n')
     */
    ByteProcessor FIND_LF = value -> value != LINE_FEED;

    /**
     * 查找非换行符
     */
    ByteProcessor FIND_NON_LF = value -> value == LINE_FEED;

    /**
     * 查找分号(';')
     */
    ByteProcessor FIND_SEMI_COLON = value -> value != ';';

    /**
     * 查找逗号(',')
     */
    ByteProcessor FIND_COMMA = value -> value != ',';

    /**
     * 查找ASCII空格字符(' ')
     */
    ByteProcessor FIND_ASCII_SPACE = value -> value != SPACE;

    /**
     * 查找回车符或换行符
     */
    ByteProcessor FIND_CRLF = value -> value != CARRIAGE_RETURN && value != LINE_FEED;

    /**
     * 查找非回车符和非换行符
     */
    ByteProcessor FIND_NON_CRLF = value -> value == CARRIAGE_RETURN || value == LINE_FEED;

    /**
     * 查找空白字符(空格或制表符)
     */
    ByteProcessor FIND_LINEAR_WHITESPACE = value -> value != SPACE && value != HTAB;

    /**
     * 查找非空白字符
     */
    ByteProcessor FIND_NON_LINEAR_WHITESPACE = value -> value == SPACE || value == HTAB;

    /**
     * 处理单个字节的方法
     * 
     * @param value 要处理的字节值
     * @return true表示继续处理下一个字节，false表示停止处理
     * @throws Exception 处理过程中可能抛出的异常
     */
    boolean process(byte value) throws Exception;
}
