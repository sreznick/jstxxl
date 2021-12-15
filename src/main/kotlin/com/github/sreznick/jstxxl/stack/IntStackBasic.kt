package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage

class IntStackBasic(
    fileName: String,
    bufferProvider: ByteBufferProvider
) : AbstractStackBasic(
    fileName,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + stackTop.sizeTailInts

    override fun unitSize(): Int = Int.SIZE_BYTES

    fun pushInt(item: Int) {
        if (stackTop.sizeTailInts == stackTop.capacityTailInts) storeBlockToDisk()
        stackTop.pushBackInt(item)
    }

    fun popInt(): Int {
        mustNotBeEmpty()
        if (stackTop.isEmptyTailInts()) loadBlockFromDisk()
        return stackTop.popBackInt()
    }
}
