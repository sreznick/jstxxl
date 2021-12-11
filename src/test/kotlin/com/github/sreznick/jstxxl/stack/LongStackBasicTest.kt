package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.AllocatingByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.RandomAccessFile
import java.util.stream.Stream

class LongStackBasicTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Pushing a list of values and then popping them all works`(size: Long) {
        val storage = RandomAccessFileSyncStorage(RandomAccessFile("long-stack-data.bin", "rw"))
        val longStackBasic = LongStackBasic(storage, AllocatingByteBufferProvider(false))
        for (i in 0 until size) {
            longStackBasic.pushLong(TestUtils.randomLongToLong(i))
            assert(longStackBasic.size == i + 1)
        }
        for (i in (size - 1) downTo 0) {
            Assertions.assertEquals(TestUtils.randomLongToLong(i), longStackBasic.popLong())
        }
    }

    companion object {
        @JvmStatic
        fun arguments(): Stream<Arguments> {
            val sizes = listOf(0, 1, 2, 10, 1024, 1_000_000, 1L shl 20)
            val sb = Stream.builder<Arguments>()
            for (size in sizes) {
                sb.add(Arguments.of(size))
            }
            return sb.build()
        }
    }
}
