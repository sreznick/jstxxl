package com.github.sreznick.jstxxl.stack

import com.github.sreznick.jstxxl.blob_helper.Blob
import com.github.sreznick.jstxxl.blob_helper.BlobElement
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider

class BlobStack(
    fileName: String,
    bufferProvider: ByteBufferProvider
) : AbstractStackBasic(
    fileName,
    bufferProvider
) {
    override val size: Long get() = currentBlocksOnDisk * BLOCK_SIZE / unitSize() + stackTop.sizeTailBlobElement

    override fun unitSize(): Int = BlobElement.SIZE_BYTES

    fun pushBlob(item: Blob) {
        for (blob_element in item.blobElements.reversed()) {
            if (stackTop.sizeTailBlobElement == stackTop.capacityTailBlobElement) storeBlockToDisk()
            stackTop.pushBackBlobElement(blob_element)
        }
    }

    private fun popBlobElement(): BlobElement {
        mustNotBeEmpty()
        if (stackTop.isEmptyTailBlobElement()) loadBlockFromDisk()
        return stackTop.popBackBlobElement()
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
