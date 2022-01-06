package com.github.sreznick.jstxxl.queue

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.AllocatingByteBufferProvider
import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class IntQueueBasicTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Pushing a list of values and then popping them all works`(size: Long) {
        val intQueueBasic = IntQueueBasic("int-queue-data.bin", AllocatingByteBufferProvider(false))
        for (i in 0 until size) {
            intQueueBasic.pushInt(TestUtils.randomLongToInt(i))
            assert(intQueueBasic.size == i + 1)
        }
        for (i in 0 until size) {
            Assertions.assertEquals(TestUtils.randomLongToInt(i), intQueueBasic.popInt())
        }
    }

    companion object {
        @JvmStatic
        fun arguments(): Stream<Arguments> {
            val sizes = listOf(0, 1, 2, 10, 1024, 4096, 10_000, 1L shl 15 + 1, 1_000_000, 1L shl 20 - 1)
            val sb = Stream.builder<Arguments>()
            for (size in sizes) {
                sb.add(Arguments.of(size))
            }
            return sb.build()
        }
    }
}
