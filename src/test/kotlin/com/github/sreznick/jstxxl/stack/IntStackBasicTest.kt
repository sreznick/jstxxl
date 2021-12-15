package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.AllocatingByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import com.github.sreznick.jstxxl.testutils.TestUtils.randomLongToInt
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.RandomAccessFile
import java.util.stream.Stream

internal class IntStackBasicTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Pushing a list of values and then popping them all works`(size: Long) {
        val intStackBasic = IntStackBasic("int-stack-data.bin", AllocatingByteBufferProvider(false))
        for (i in 0 until size) {
            intStackBasic.pushInt(randomLongToInt(i))
            assert(intStackBasic.size == i + 1)
        }
        for (i in (size - 1) downTo 0) {
            assertEquals(randomLongToInt(i), intStackBasic.popInt())
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
            sb.add(Arguments.of( 1_000_000_000))
            return sb.build()
        }
    }
}
