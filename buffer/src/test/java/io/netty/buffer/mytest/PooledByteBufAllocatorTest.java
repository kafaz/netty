package io.netty.buffer.mytest;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.jupiter.api.Test;

public class PooledByteBufAllocatorTest {
    @Test
    public void testAllocateProcess(){
        PooledByteBufAllocator allocator = new PooledByteBufAllocator();
        ByteBuf byteBuf = allocator.directBuffer(1 << 30);

    }
}
