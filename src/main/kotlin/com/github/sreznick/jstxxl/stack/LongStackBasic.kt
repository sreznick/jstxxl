package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage

class LongStackBasic(
    fileName: String,
    bufferProvider: ByteBufferProvider
) : AbstractStackBasic(
    fileName,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + stackTop.sizeTailLongs

    override fun unitSize(): Int = Long.SIZE_BYTES

    fun pushLong(item: Long) {
        if (stackTop.sizeTailLongs == stackTop.capacityTailLongs) storeBlockToDisk()
        stackTop.pushBackLong(item)
    }

    fun popLong(): Long {
        mustNotBeEmpty()
        if (stackTop.isEmptyTailLongs()) loadBlockFromDisk()
        return stackTop.popBackLong()
    }
}
