package com.github.sreznick.jstxxl.btree

import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.stream.Stream

class IntBTreeTest {
    @ParameterizedTest
    @MethodSource("arguments")
    fun `Putting a list of values and then getting them all works`(size: Int) {
        val intBTree = IntBTree("IntBTreeTest")

        for (i in 0 until size) {
            intBTree.put(TestUtils.randomIntToInt(i), i)
            assert(intBTree.size() == i + 1)
        }

        for (i in 0 until size) {
            assertEquals(i, intBTree.get(TestUtils.randomIntToInt(i)))
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
