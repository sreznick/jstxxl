package com.github.sreznick.jstxxl.btree

import com.github.sreznick.jstxxl.blob_helper.Blob
import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import com.github.sreznick.jstxxl.testutils.TestUtils
import com.github.sreznick.jstxxl.testutils.TestClassForBlob
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.stream.Stream

class BlobBTreeBasicTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Putting a list of values and then getting them all works`(size: Int) {
        val blobBTree = BlobBTree("IntBTreeTest")

        for (i in 0 until size) {
            val arraySize = (((TestUtils.randomIntToInt(i + size) % 239) * (TestUtils.randomIntToInt(i + size) % 366)) % 533)
            val tmpArray = Array(arraySize) { 0 }
            for (j in 0 until arraySize) {
                tmpArray[j] = TestUtils.randomIntToInt(j)
            }
            val blob = Blob(TestClassForBlob(tmpArray))
            blobBTree.put(TestUtils.randomIntToInt(i), blob)
            assert(blobBTree.size() == i + 1)
        }

        for (i in 0 until size) {
            val blob = blobBTree.get(TestUtils.randomIntToInt(i))
            val bTreeResult = TestClassForBlob()
            blob!!.toBlobConvertible(bTreeResult)
            val arraySize = (((TestUtils.randomIntToInt(i + size) % 239) * (TestUtils.randomIntToInt(i + size) % 366)) % 533)
            val tmpArray = Array(arraySize) { 0 }
            for (j in 0 until arraySize) {
                tmpArray[j] = TestUtils.randomIntToInt(j)
            }
            assertEquals(TestClassForBlob(tmpArray), bTreeResult)
        }

        for (i in 0 until size / 256 + 1) {
            File("IntBTreeTest$i").delete()
        }
    }

    companion object {
        @JvmStatic
        fun arguments(): Stream<Arguments> {
            val sizes = listOf(0, 1, 2, 10, 1024, 4096, 10_000, 1 shl 15 + 1, 100_000)
            val sb = Stream.builder<Arguments>()
            for (size in sizes) {
                sb.add(Arguments.of(size))
            }
            return sb.build()
        }
    }
}
