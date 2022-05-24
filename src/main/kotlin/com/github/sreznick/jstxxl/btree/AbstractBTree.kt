package com.github.sreznick.jstxxl.btree

import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer

abstract class AbstractBTree<Key : Comparable<Key>, Value>(
    protected val fileName: String
) {
    private var height = 0
    private var size = 0
    private var numberOfRoot = 0
    private var counterFiles = 1
    private var root = Node(0)

    protected inner class Node(
        var numberOfEntries: Int
    ) {
        val entries = arrayOfNulls<Entry>(MAX_ENTRIES)
    }

    protected inner class Entry(
        var key: Key,
        var value: Value?,
        var child: Int = -1
    )

    val isEmpty: Boolean
        get() = size() == 0

    fun size(): Int {
        return size
    }

    fun height(): Int {
        return height
    }

    protected abstract fun keySizeBytes(): Int

    protected abstract fun valueSizeBytes(): Int

    abstract fun readKey(byteBuffer: ByteBuffer): Key

    abstract fun readValue(byteBuffer: ByteBuffer): Value?

    abstract fun writeKey(key: Key, byteBuffer: ByteBuffer)

    abstract fun writeValue(value: Value?, byteBuffer: ByteBuffer)

    private fun lowerBoundByEntries(node: Node, key: Key): Int {
        var left = 0
        var right = node.numberOfEntries
        while (right - left > 1) {
            val middle = (left + right) / 2
            val middleKey = node.entries[middle]?.key!!
            if (key < middleKey) {
                right = middle
            } else {
                left = middle
            }
        }
        return left
    }

    fun get(key: Key): Value? {
        return search(readNode(numberOfRoot), key, height)
    }

    private fun search(curNode: Node, key: Key, level: Int): Value? {
        val entries = curNode.entries

        if (level == 0) {
            val i = lowerBoundByEntries(curNode, key)
            if (key == entries[i]!!.key) {
                return entries[i]!!.value
            }
        } else {
            val i = lowerBoundByEntries(curNode, key)
            val node = readNode(entries[i]!!.child)
            return search(
                node,
                key,
                level - 1
            )
        }
        return null
    }

    fun put(key: Key, value: Value) {
        val child = insert(root, numberOfRoot, key, value, height)
        size++
        if (child == null) {
            return
        }

        val newRoot = Node(2)
        val nextNode = readNode(child)

        newRoot.entries[0] = Entry(root.entries[0]!!.key, null, numberOfRoot)
        newRoot.entries[1] = Entry(nextNode.entries[0]!!.key, null, child)
        height++

        root = newRoot
        numberOfRoot = counterFiles
        saveNode(newRoot, counterFiles++)
    }

    private fun insert(curNode: Node, number: Int, key: Key, value: Value, level: Int): Int? {
        var j: Int
        val entry = Entry(key, value)

        if (level == 0) {
            j = lowerBoundByEntries(curNode, key)
            if (j < curNode.numberOfEntries && key >= curNode.entries[j]!!.key) {
                j++
            }
        } else {
            j = lowerBoundByEntries(curNode, key)

            val node = readNode(curNode.entries[j]!!.child)
            val child = insert(node, curNode.entries[j++]!!.child, key, value, level - 1) ?: return null

            val nextNode = readNode(child)

            entry.key = nextNode.entries[0]!!.key
            entry.value = null
            entry.child = child
        }
        for (i in curNode.numberOfEntries downTo j + 1) {
            curNode.entries[i] = curNode.entries[i - 1]
        }
        curNode.entries[j] = entry
        curNode.numberOfEntries++

        saveNode(curNode, number)
        return if (curNode.numberOfEntries < MAX_ENTRIES) null else split(curNode, number)
    }

    private fun split(node: Node, number: Int): Int {
        val newNode = Node(MAX_ENTRIES / 2)
        node.numberOfEntries = MAX_ENTRIES / 2
        for (j in 0 until MAX_ENTRIES / 2) {
            newNode.entries[j] = node.entries[MAX_ENTRIES / 2 + j]
        }
        saveNode(newNode, counterFiles)

        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + number), "rw"))
        storage.writeFrom(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(MAX_ENTRIES / 2), 0, Int.SIZE_BYTES)
        storage.close()

        return counterFiles++
    }

    protected open fun readNode(number: Int): Node {
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + number), "rw"))

        val size = Int.SIZE_BYTES + MAX_ENTRIES * (keySizeBytes() + valueSizeBytes() + Int.SIZE_BYTES)
        val byteBuffer = ByteBuffer.allocate(size)
        storage.readInto(byteBuffer, 0, size)
        storage.close()

        val numberOfKeys = byteBuffer.int
        val node = Node(numberOfKeys)

        for (i in 0 until numberOfKeys) {
            val key = readKey(byteBuffer)
            val value = readValue(byteBuffer)
            val next = byteBuffer.int
            node.entries[i] = Entry(key, value, next)
        }
        return node
    }

    protected open fun saveNode(node: Node, number: Int) {
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + number), "rw"))

        val size = Int.SIZE_BYTES + MAX_ENTRIES * (keySizeBytes() + valueSizeBytes() + Int.SIZE_BYTES)
        val byteBuffer = ByteBuffer.allocate(size).putInt(node.numberOfEntries)

        for (j in 0 until node.numberOfEntries) {
            writeKey(node.entries[j]?.key!!, byteBuffer)
            writeValue(node.entries[j]?.value, byteBuffer)
            byteBuffer.putInt(node.entries[j]?.child!!)
        }
        storage.writeFrom(byteBuffer, 0, byteBuffer.position())
        storage.close()
    }



    companion object {
        @JvmStatic
        protected val MAX_ENTRIES = 512
    }
}
