package com.github.sreznick.jstxxl.btreap

import java.nio.ByteBuffer

const val DEFAULT_NODE_CAPACITY = 100

class IntBTreapPriorityQueue(
    fileName: String,
    nodeCapacity: Int = DEFAULT_NODE_CAPACITY
) : AbstractBTreapPriorityQueue<Int>(fileName, nodeCapacity) {

    override fun keySizeBytes(): Int = Int.SIZE_BYTES

    override fun readKey(byteBuffer: ByteBuffer): Int = byteBuffer.int

    override fun writeKey(key: Int, byteBuffer: ByteBuffer) {
        byteBuffer.putInt(key)
    }
}