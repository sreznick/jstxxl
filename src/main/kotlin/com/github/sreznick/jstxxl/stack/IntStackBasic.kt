package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage

class IntStackBasic(
    storage: SyncStorage,
    bufferProvider: ByteBufferProvider
) : AbstractStackBasic(
    storage,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + stackTop.sizeInts

    override fun unitSize(): Int = Int.SIZE_BYTES

    fun pushInt(item: Int) {
        if (stackTop.sizeInts == stackTop.capacityInts) storeBlockToDisk()
        stackTop.pushFrontInt(item)
    }

    fun popInt(): Int {
        mustNotBeEmpty()
        if (stackTop.isEmptyInts()) loadBlockFromDisk()
        return stackTop.popFrontInt()
    }
}
