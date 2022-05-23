package com.github.sreznick.jstxxl.queue

import com.github.sreznick.jstxxl.blob_helper.Blob
import com.github.sreznick.jstxxl.blob_helper.BlobElement
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider

class BlobQueue(
    fileName: String,
    bufferProvider: ByteBufferProvider
) : AbstractQueueBasic(
    fileName,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + queueTop.sizeHeadBlobElement + queueTop.sizeTailBlobElement + localDeque.size

    override fun unitSize(): Int = BlobElement.SIZE_BYTES

    private fun isEmptyHeadAndDisk(): Boolean =
        currentBlocksOnDisk * BLOCK_SIZE / unitSize() + queueTop.sizeHeadBlobElement == 0L

    private val localDeque = ArrayDeque<BlobElement>()

    fun pushBlob(item: Blob) {
        for (i in item.blobElements) {
            if (isEmptyHeadAndDisk()) {
                localDeque.addLast(i)

                if (localDeque.size == queueTop.capacityTailBlobElement) {
                    while (localDeque.isNotEmpty()) {
                        queueTop.pushBackBlobElement(localDeque.removeFirst())
                    }
                    storeBlockToDisk()
                }
                continue
            }

            if (queueTop.sizeTailBlobElement == queueTop.capacityTailBlobElement) storeBlockToDisk()
            queueTop.pushBackBlobElement(i)
        }
    }

    private fun popBlobElement(): BlobElement {
        mustNotBeEmpty()

        if (isEmptyHeadAndDisk()) {
            if (localDeque.isEmpty()) {
                while (!queueTop.isEmptyTailBlobElement()) {
                    localDeque.addFirst(queueTop.popBackBlobElement())
                }
            }
            return localDeque.removeFirst()
        }

        if (queueTop.isEmptyHeadBlobElement()) loadBlockFromDisk()
        return queueTop.popFrontBlobElement()
    }

    fun popBlob(): Blob {
        val tmpBlobElement = popBlobElement()
        var amountOfData = 0
        for (i in 0 until Int.SIZE_BYTES) {
            amountOfData += tmpBlobElement.data[i].toUByte().toInt() shl (i * Byte.SIZE_BITS)
        }
        val resultBlob = Blob(amountOfData)
        resultBlob.addBlobElement(0, tmpBlobElement)
        for (i in 1 until resultBlob.amountOfBlobElements) {
            resultBlob.addBlobElement(i, popBlobElement())
        }
        return resultBlob
    }
}