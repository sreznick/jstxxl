package com.github.sreznick.jstxxl.blob

import com.github.sreznick.jstxxl.blob_helper.*
import com.github.sreznick.jstxxl.testutils.TestClassForBlob
import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class BlobBasicTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Making blob from object and making object from blob `(size: Long) {
        for (i in 0 until size) {
            val arraySize =
                (((TestUtils.randomLongToInt(i + size) % 239) * (TestUtils.randomLongToInt(i + size) % 366)) % 533)
            val tmpArray = Array(arraySize) { 0 }
            for (j in 0 until arraySize) {
                tmpArray[j] = TestUtils.randomLongToInt(j * (size % 239))
            }
            val blob = Blob(TestClassForBlob(tmpArray))
            val result = TestClassForBlob()
            blob.toBlobConvertible(result)
            assert(result == TestClassForBlob(tmpArray))
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

