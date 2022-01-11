package com.github.sreznick.jstxxl.platformdeps.storage

import java.io.RandomAccessFile
import java.nio.ByteBuffer

class RandomAccessFileSyncStorage(private val file: RandomAccessFile) : SyncStorage {
    override fun writeFrom(byteBuffer: ByteBuffer, offset: Int, length: Int) {
        file.write(byteBuffer.array(), offset, length)
    }

    override fun readInto(byteBuffer: ByteBuffer, offset: Int, length: Int) {
        file.read(byteBuffer.array(), offset, length)
    }

    override fun seek(position: Long) {
        file.seek(position)
    }

    override fun close() {
        file.close()
    }
}
