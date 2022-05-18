package com.github.sreznick.jstxxl.btreap

import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import java.io.File
import java.io.RandomAccessFile
import java.lang.Integer.max
import java.lang.Integer.min
import java.nio.ByteBuffer
import kotlin.collections.HashMap
import kotlin.random.Random.Default.nextLong

abstract class AbstractBTreap<Key : Comparable<Key>>(
    private val fileName: String,
    private val nodeCapacity: Int
) {
    var size: Long = 0

    private var root: Node? = null
    private var nextId = 1L

    private data class NodeSize(val rank: Int, val subTreeSize: Int)

    private enum class Direction {
        LEFT, RIGHT, BOTH
    }

    private inner class Node(
        val id: Long,
        val key: Key,
        var leftId: Long,
        var rightId: Long,
        var leftRank: Int,
        var rightRank: Int,
        var isMultipleLeader: Boolean,
        var size: NodeSize,
        val priority: Long
    ) {
        var parentNode: Node? = null

        var leftNode: Node? = null
            set(value) {
                field = value
                leftId = value?.id ?: -1
                leftRank = value?.size?.rank ?: -1
                value?.parentNode = this
            }

        var rightNode: Node? = null
            set(value) {
                field = value
                rightId = value?.id ?: -1
                rightRank = value?.size?.rank ?: -1
                value?.parentNode = this
            }

        fun getAndReadLeftNode(): Node? {
            if (leftNode != null || leftId == -1L)
                return leftNode

            readBTreapNode(this, Direction.LEFT)
            return leftNode
        }

        fun getAndReadRightNode(): Node? {
            if (rightNode != null || rightId == -1L)
                return rightNode

            readBTreapNode(this, Direction.RIGHT)
            return rightNode
        }

        fun updateSize(): Boolean {
            val lRank = max(leftRank, 0)
            val rRank = max(rightRank, 0)

            val rank = max(lRank, rRank)
            var subTreeSize = 1

            if (rank == lRank)
                subTreeSize += getAndReadLeftNode()?.size?.subTreeSize ?: 0
            if (rank == rRank)
                subTreeSize += getAndReadRightNode()?.size?.subTreeSize ?: 0

            val prevSize = size
            size = if (subTreeSize >= nodeCapacity)
                NodeSize(rank + 1, 1)
            else
                NodeSize(rank, subTreeSize)

            if (parentNode?.leftNode == this)
                parentNode?.leftRank = size.rank

            if (parentNode?.rightNode == this)
                parentNode?.rightRank = size.rank

            return size != prevSize
        }

        fun updateIsMultipleLeader() {
            val newFlag = leftRank != rightRank &&
                    size.rank > max(leftRank, rightRank) && min(leftRank, rightRank) != -1

            // To not lose nodes due to different file names
            if (newFlag != isMultipleLeader) {
                readBTreapNode(this, Direction.LEFT)
                readBTreapNode(this, Direction.RIGHT)
            }

            isMultipleLeader = newFlag
        }
    }

    protected abstract fun keySizeBytes(): Int
    protected abstract fun readKey(byteBuffer: ByteBuffer): Key
    protected abstract fun writeKey(key: Key, byteBuffer: ByteBuffer)

    fun isEmpty() = size == 0L

    open fun add(key: Key) {
        val node = Node(id = nextId++, key = key, leftId = -1, rightId = -1, leftRank = -1, rightRank = -1,
                isMultipleLeader = false, size = NodeSize(0, 1), priority = nextLong())
        root = treapInsert(root, node)

        fixAncestors(node.parentNode)
        saveBTreap(root)
        size++
    }

    fun remove(value: Key) {
        val node = find(root, value) ?: return
        val parent = node.parentNode
        val newSubTreeRoot = treapDelete(node)

        if (parent != null) {
            if (parent.leftNode == node)
                parent.leftNode = newSubTreeRoot
            else
                parent.rightNode = newSubTreeRoot
        } else
            root = newSubTreeRoot

        fixAncestors(parent)
        saveBTreap(root)
        size--
    }

    fun contains(value: Key): Boolean {
        val node = find(root, value)
        saveBTreap(root)
        return node != null
    }

    fun first(): Key? {
        val first = getFirst(root)

        saveBTreap(root)
        return first?.key
    }

    fun last(): Key? {
        val last = getLast(root)

        saveBTreap(root)
        return last?.key
    }

    private fun getFirst(node: Node?): Node? {
        if (node == null)
            return null

        return getFirst(node.getAndReadLeftNode()) ?: node
    }

    private fun getLast(node: Node?): Node? {
        if (node == null)
            return null

        return getFirst(node.getAndReadRightNode()) ?: node
    }

    private fun rightRotation(node: Node): Node {
        val x = requireNotNull(node.getAndReadLeftNode())
        val t1 = x.getAndReadLeftNode()
        val t2 = x.getAndReadRightNode()
        val t3 = node.getAndReadRightNode()

        node.leftNode = t2
        node.rightNode = t3
        node.updateSize()

        x.parentNode = node.parentNode
        x.leftNode = t1
        x.rightNode = node
        x.updateSize()

        return x
    }

    private fun leftRotation(node: Node): Node {
        val y = requireNotNull(node.getAndReadRightNode())
        val t1 = node.getAndReadLeftNode()
        val t2 = y.getAndReadLeftNode()
        val t3 = y.getAndReadRightNode()

        node.leftNode = t1
        node.rightNode = t2
        node.updateSize()

        y.parentNode = node.parentNode
        y.leftNode = node
        y.rightNode = t3
        y.updateSize()

        return y
    }

    private fun treapInsert(node: Node?, newNode: Node): Node {
        if (node == null)
            return newNode

        if (node.key <= newNode.key) {
            node.rightNode = treapInsert(node.getAndReadRightNode(), newNode)

            if (node.rightNode!!.priority > node.priority) {
                val newSubTreeRoot = leftRotation(node)
                newSubTreeRoot.leftNode?.updateIsMultipleLeader()
                newSubTreeRoot.updateIsMultipleLeader()
                return newSubTreeRoot
            }
        } else {
            node.leftNode = treapInsert(node.getAndReadLeftNode(), newNode)

            if (node.leftNode!!.priority > node.priority) {
                val newSubTreeRoot = rightRotation(node)
                newSubTreeRoot.rightNode?.updateIsMultipleLeader()
                newSubTreeRoot.updateIsMultipleLeader()
                return newSubTreeRoot
            }
        }

        node.updateSize()
        node.updateIsMultipleLeader()
        return node
    }

    private fun treapDelete(node: Node): Node? {
        if (node.leftId == -1L && node.rightId == -1L)
            return null

        val newSubTreeRoot: Node

        if (node.getAndReadLeftNode() == null || node.getAndReadRightNode() != null &&
                node.rightNode!!.priority > node.leftNode!!.priority
        ) {
            newSubTreeRoot = leftRotation(node)
            newSubTreeRoot.leftNode = treapDelete(node)
        } else {
            newSubTreeRoot = rightRotation(node)
            newSubTreeRoot.rightNode = treapDelete(node)
        }

        newSubTreeRoot.updateSize()
        newSubTreeRoot.updateIsMultipleLeader()
        return newSubTreeRoot
    }

    private fun fixAncestors(node: Node?) {
        if (node == null)
            return

        val modified = node.updateSize()
        node.updateIsMultipleLeader()
        if (modified)
            fixAncestors(node.parentNode)
    }

    private fun find(node: Node?, value: Key): Node? {
        if (node == null || node.key == value)
            return node

        return if (value > node.key)
            find(node.getAndReadRightNode(), value)
        else
            find(node.getAndReadLeftNode(), value)
    }

    private fun buildSubTree(node: Node?, idToNode: HashMap<Long, Node>): Node? {
        if (node == null)
            return null

        if (idToNode.contains(node.leftId))
            node.leftNode = buildSubTree(idToNode[node.leftId], idToNode)

        if (idToNode.contains(node.rightId))
            node.rightNode = buildSubTree(idToNode[node.rightId], idToNode)

        return node
    }

    private fun getPartitionElements(node: Node?, rank: Int): Sequence<Node> {
        if (node == null || node.size.rank != rank)
            return sequenceOf()

        return getPartitionElements(node.leftNode, rank) + getPartitionElements(node.rightNode, rank) + node
    }

    private fun getBTreapNodeEntries(node: Node, direction: Direction): List<Node> {
        val result =  when (direction) {
            Direction.LEFT -> getPartitionElements(node.leftNode, node.leftRank)
            Direction.RIGHT -> getPartitionElements(node.rightNode, node.rightRank)
            Direction.BOTH -> {
                var entries = sequenceOf<Node>()

                if (node.size.rank > node.leftRank)
                    entries += getPartitionElements(node.leftNode, node.leftRank)
                if (node.size.rank > node.rightRank)
                    entries += getPartitionElements(node.rightNode, node.rightRank)

                entries
            }
        }

        return result.toList()
    }

    private fun readBTreapNode(node: Node, direction: Direction): Node {
        val leader = node.id
        val fileNameSuffix = if (node.isMultipleLeader)
            "$leader $direction"
        else
            leader
        val file = File(fileName + fileNameSuffix)
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(file, "rw"))

        val size = Int.SIZE_BYTES + Int.SIZE_BYTES + max(nodeCapacity, (nodeCapacity - 1) * 2) *
                (4 * Long.SIZE_BYTES + keySizeBytes() + 3 * Int.SIZE_BYTES + Byte.SIZE_BYTES)
        val byteBuffer = ByteBuffer.allocate(size)
        storage.readInto(byteBuffer, 0, size)
        storage.close()
        file.delete()

        val numberOfTreapNodes = byteBuffer.int
        val rank = byteBuffer.int
        val idToNode = hashMapOf<Long, Node>()

        repeat(numberOfTreapNodes) {
            val id = byteBuffer.long
            val key = readKey(byteBuffer)
            val leftId = byteBuffer.long
            val rightId = byteBuffer.long
            val leftRank = byteBuffer.int
            val rightRank = byteBuffer.int
            val isMultipleLeader = byteBuffer.get() == 1.toByte()
            val subTreeSize = byteBuffer.int
            val priority = byteBuffer.long

            idToNode[id] = Node(id = id, key = key, leftId = leftId,
                    rightId = rightId, leftRank = leftRank, rightRank = rightRank, isMultipleLeader = isMultipleLeader,
                    size = NodeSize(rank, subTreeSize), priority = priority)
        }

        return requireNotNull(buildSubTree(node, idToNode))
    }

    private fun saveBTreapNode(node: Node, direction: Direction) {
        val leader = node.id
        val fileNameSuffix = if (direction != Direction.BOTH)
            "$leader $direction"
        else
            leader
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + fileNameSuffix), "rw"))

        val size = Int.SIZE_BYTES + Int.SIZE_BYTES + max(nodeCapacity, (nodeCapacity - 1) * 2) *
                (4 * Long.SIZE_BYTES + keySizeBytes() + 3 * Int.SIZE_BYTES + Byte.SIZE_BYTES)
        val entryList = getBTreapNodeEntries(node, direction)
        val byteBuffer = ByteBuffer.allocate(size)
        byteBuffer.putInt(entryList.size)
        byteBuffer.putInt(entryList.first().size.rank)

        for (entry in entryList) {
            byteBuffer.putLong(entry.id)
            writeKey(entry.key, byteBuffer)
            byteBuffer.putLong(entry.leftId)
            byteBuffer.putLong(entry.rightId)
            byteBuffer.putInt(entry.leftRank)
            byteBuffer.putInt(entry.rightRank)
            byteBuffer.put(if (entry.isMultipleLeader) 1 else 0)
            byteBuffer.putInt(entry.size.subTreeSize)
            byteBuffer.putLong(entry.priority)
        }

        storage.writeFrom(byteBuffer, 0, byteBuffer.position())
        storage.close()
    }

    private fun saveBTreapNode(node: Node) {
        if (node.isMultipleLeader) {
            if (node.leftNode != null)
                saveBTreapNode(node, Direction.LEFT)
            if (node.rightNode != null)
                saveBTreapNode(node, Direction.RIGHT)
        } else {
            if (node.leftNode != null || node.rightNode != null)
                saveBTreapNode(node, Direction.BOTH)
        }

        val leftNode = node.leftNode
        val rightNode = node.rightNode

        if (leftNode != null && node.size.rank > leftNode.size.rank) {
            node.leftNode = null
            node.leftRank = leftNode.size.rank
            node.leftId = leftNode.id
        }

        if (rightNode != null && node.size.rank > rightNode.size.rank) {
            node.rightNode = null
            node.rightRank = rightNode.size.rank
            node.rightId = rightNode.id
        }
    }

    private fun saveBTreap(node: Node?) {
        if (node == null)
            return

        saveBTreap(node.leftNode)
        saveBTreap(node.rightNode)

        if (node.size.rank > node.leftRank && node.leftNode != null ||
                node.size.rank > node.rightRank && node.rightNode != null
        ) {
            saveBTreapNode(node)
        }
    }
}