package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage

class LongStackBasic(
    storage: SyncStorage,
    bufferProvider: ByteBufferProvider
) : AbstractStackBasic(
    storage,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + stackTop.sizeLongs

    override fun unitSize(): Int = Long.SIZE_BYTES

    fun pushLong(item: Long) {
        if (stackTop.sizeLongs == stackTop.capacityLongs) storeBlockToDisk()
        stackTop.pushFrontLong(item)
    }

    fun popLong(): Long {
        mustNotBeEmpty()
        if (stackTop.isEmptyLongs()) loadBlockFromDisk()
        return stackTop.popFrontLong()
    }
}
