package com.github.sreznick.jstxxl.btree

import java.nio.ByteBuffer

class IntBTree(fileName: String) : AbstractBTree<Int, Int>(fileName) {
    override fun keySizeBytes(): Int = Int.SIZE_BYTES

    override fun valueSizeBytes(): Int = Int.SIZE_BYTES

    override fun readKey(byteBuffer: ByteBuffer): Int = byteBuffer.int

    override fun readValue(byteBuffer: ByteBuffer): Int = byteBuffer.int

    override fun writeKey(key: Int, byteBuffer: ByteBuffer) {
        byteBuffer.putInt(key)
    }

    override fun writeValue(value: Int?, byteBuffer: ByteBuffer) {
        if (value == null) {
            byteBuffer.putInt(0)
        } else {
            byteBuffer.putInt(value)
        }
    }
}
