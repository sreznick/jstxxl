package com.github.sreznick.jstxxl.queue

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider

class IntQueueBasic(
    fileName: String,
    bufferProvider: ByteBufferProvider
) : AbstractQueueBasic(
    fileName,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + queueTop.sizeHeadInts + queueTop.sizeTailInts + localDeque.size

    override fun unitSize(): Int = Int.SIZE_BYTES

    private fun isEmptyHeadAndDisk(): Boolean =
        currentBlocksOnDisk * BLOCK_SIZE / unitSize() + queueTop.sizeHeadInts == 0L

    private val localDeque = ArrayDeque<Int>()

    fun pushInt(item: Int) {
        if (isEmptyHeadAndDisk()) {
            localDeque.addLast(item)

            if (localDeque.size == queueTop.capacityTailInts) {
                while (localDeque.isNotEmpty()) {
                    queueTop.pushBackInt(localDeque.removeFirst())
                }
                storeBlockToDisk()
            }
            return
        }

        if (queueTop.sizeTailInts == queueTop.capacityTailInts) storeBlockToDisk()
        queueTop.pushBackInt(item)
    }

    fun popInt(): Int {
        mustNotBeEmpty()

        if (isEmptyHeadAndDisk()) {
            if (localDeque.isEmpty()) {
                while (!queueTop.isEmptyTailInts()) {
                    localDeque.addFirst(queueTop.popBackInt())
                }
            }
            return localDeque.removeFirst()
        }

        if (queueTop.isEmptyHeadInts()) loadBlockFromDisk()
        return queueTop.popFrontInt()
    }
}
