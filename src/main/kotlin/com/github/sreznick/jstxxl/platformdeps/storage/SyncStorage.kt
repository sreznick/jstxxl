package com.github.sreznick.jstxxl.platformdeps.storage

import java.nio.ByteBuffer

interface SyncStorage {
    fun writeFrom(byteBuffer: ByteBuffer, offset: Int, length: Int)
    fun readInto(byteBuffer: ByteBuffer, offset: Int, length: Int)
    fun seek(position: Long)
}
