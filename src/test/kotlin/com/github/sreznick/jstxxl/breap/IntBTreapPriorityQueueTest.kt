package com.github.sreznick.jstxxl.breap

import com.github.sreznick.jstxxl.btreap.IntBTreapPriorityQueue
import com.github.sreznick.jstxxl.testutils.TestUtils
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import java.util.PriorityQueue
import java.util.stream.Stream
import kotlin.random.Random

class IntBTreapPriorityQueueTest {

    private val directory = "TestData"
    private val filePrefix = "node"
    private val nodeCapacity = 20

    private enum class Actions {
        ADD, POLL, PEEK
    }

    @BeforeEach
    fun createDir() {
        if (!File(directory).exists())
            Files.createDirectory(Paths.get(directory))
    }

    @AfterEach
    fun clear() {
        File(directory).deleteRecursively()
    }

    @ParameterizedTest
    @MethodSource("arguments")
    fun `Putting a list of values and then getting them all works`(size: Int) {
        val priorityQueue = IntBTreapPriorityQueue("$directory/$filePrefix", nodeCapacity)

        for (i in 0 until size) {
            priorityQueue.add(TestUtils.randomIntToInt(i))
            assertEquals(i.toLong() + 1, priorityQueue.size)
        }

        for (i in 0 until size) {
            assertTrue(priorityQueue.contains(TestUtils.randomIntToInt(i)))
        }

        var prev = Integer.MIN_VALUE

        for (i in 0 until size) {
            val value = priorityQueue.poll() ?: fail()
            assertTrue(value >= prev)
            prev = value
        }

        assertTrue(priorityQueue.isEmpty())
    }

    @ParameterizedTest
    @MethodSource("arguments")
    fun `Priority queue functionality test`(iterationNumber: Int) {
        val random = Random(iterationNumber)
        val priorityQueue = PriorityQueue<Int>()
        val btreapPriorityQueue = IntBTreapPriorityQueue("$directory/$filePrefix", nodeCapacity)

        repeat(iterationNumber) {
            val actionIndex = random.nextInt(0, 3)
            val action = Actions.values()[actionIndex]

            when (action) {
                Actions.PEEK ->
                    if (priorityQueue.isNotEmpty())
                        assertEquals(priorityQueue.peek(), btreapPriorityQueue.peek())

                Actions.POLL ->
                    if (priorityQueue.isNotEmpty())
                        assertEquals(priorityQueue.poll(), btreapPriorityQueue.poll())

                Actions.ADD ->
                    repeat(10) {
                        val value = random.nextInt()
                        priorityQueue.add(value)
                        btreapPriorityQueue.add(value)
                    }
            }

            assertEquals(priorityQueue.size.toLong(), btreapPriorityQueue.size)
        }
    }

    companion object {
        @JvmStatic
        fun arguments(): Stream<Arguments> {
            val sizes = listOf(0, 1, 2, 10, 1024, 4096, 10_000, 100_000)
            val sb = Stream.builder<Arguments>()
            for (size in sizes) {
                sb.add(Arguments.of(size))
            }
            return sb.build()
        }
    }
}