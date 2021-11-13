package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.buffer.BlockByteSemiDeque
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage
import java.util.*

class IntStackBasic(val blockSizeInts: Int, val storage: SyncStorage, bufferProvider: ByteBufferProvider) {
    private val stackTop = BlockByteSemiDeque(blockSizeInts * Int.SIZE_BYTES, 2, bufferProvider)

    private var currentBlocksOnDisk: Long = 0

    val size: Long get() = currentBlocksOnDisk * blockSizeInts + stackTop.sizeInts

    fun isEmpty(): Boolean = currentBlocksOnDisk == 0L && stackTop.sizeInts == 0

    fun pushInt(item: Int) {
        if (stackTop.sizeInts == stackTop.capacityInts) storeBlockToDisk()
        stackTop.pushFrontInt(item)
    }

    fun popInt(): Int {
        mustNotBeEmpty()
        if (stackTop.sizeInts == 0) loadBlockFromDisk()
        return stackTop.popFrontInt()
    }

    private fun storeBlockToDisk() {
        stackTop.popBackBlockInto(storage)
        ++currentBlocksOnDisk
    }

    private fun loadBlockFromDisk() {
        storage.seek(--currentBlocksOnDisk * blockSizeInts * Int.SIZE_BYTES)
        stackTop.pushBackBlockFrom(storage)
    }

    private fun mustNotBeEmpty() {
        if (isEmpty()) {
            throw EmptyStackException()
        }
    }
}
