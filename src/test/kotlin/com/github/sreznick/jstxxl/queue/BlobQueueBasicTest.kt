package com.github.sreznick.jstxxl.queue

import com.github.sreznick.jstxxl.blob_helper.Blob
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.AllocatingByteBufferProvider
import com.github.sreznick.jstxxl.testutils.TestClassForBlob
import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

class BlobQueueTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Pushing a list of values and then popping them all works`(size: Long) {
        val blobQueue = BlobQueue("blob_elements-queue-data.bin", AllocatingByteBufferProvider(false))
        var prevSize = 0L
        for (i in 0 until size) {
            val arraySize = (((TestUtils.randomLongToInt(i) % 239) * (TestUtils.randomLongToInt(i) % 366)) % 533)
            val tmpArray = Array(arraySize) { 0 }
            for (j in 0 until arraySize) {
                tmpArray[j] = TestUtils.randomIntToInt(j)
            }
            val blob = Blob(TestClassForBlob(tmpArray))
            blobQueue.pushBlob(blob)
            prevSize += blob.amountOfBlobElements
            assert(blobQueue.size == prevSize)
        }
        for (i in 0 until size) {
            val blob = blobQueue.popBlob()
            val queResult = TestClassForBlob()
            blob.toBlobConvertible(queResult)
            val arraySize = (((TestUtils.randomLongToInt(i) % 239) * (TestUtils.randomLongToInt(i) % 366)) % 533)
            val tmpArray = Array(arraySize) { 0 }
            for (j in 0 until arraySize) {
                tmpArray[j] = TestUtils.randomIntToInt(j)
            }
            Assertions.assertEquals(TestClassForBlob(tmpArray), queResult)
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

