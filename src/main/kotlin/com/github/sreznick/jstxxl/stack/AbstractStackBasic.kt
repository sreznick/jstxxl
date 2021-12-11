package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.buffer.BlockByteSemiDeque
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage
import java.util.EmptyStackException

abstract class AbstractStackBasic(
    private val storage: SyncStorage,
    bufferProvider: ByteBufferProvider
) {
    protected var currentBlocksOnDisk: Long = 0

    abstract val size: Long

    fun isEmpty(): Boolean = size == 0L

    protected abstract fun unitSize(): Int

    protected val stackTop by lazy {
        BlockByteSemiDeque(BLOCK_SIZE, 2, bufferProvider)
    }

    protected fun mustNotBeEmpty() {
        if (isEmpty()) {
            throw EmptyStackException()
        }
    }

    protected fun storeBlockToDisk() {
        stackTop.popBackBlockInto(storage)
        ++currentBlocksOnDisk
    }

    protected fun loadBlockFromDisk() {
        storage.seek(--currentBlocksOnDisk * BLOCK_SIZE)
        stackTop.pushBackBlockFrom(storage)
    }

    companion object {
        const val BLOCK_SIZE = 4096
    }
}
