package com.github.sreznick.jstxxl.blob_helper

import java.lang.Integer.min

/**
 * Binary large object (blob) class
 * Keeps given object as an array of [BlobElement]
 * @author Nechaev Ilia
 */
class Blob {

    /**
     * Amount of [BlobElement]s
     */
    var amountOfBlobElements: Int
        private set

    /**
     * array of [BlobElement] with BLOB data
     */
    var blobElements: Array<BlobElement>
        private set

    /**
     * Amount of data stored in BLOB
     */
    var amountOfData: Int
        private set

    /**
     * Insert [BlobElement] to [blobElements] by the given index
     * @param index index to insert
     * @param element [BlobElement] to insert
     * @exception java.lang.IndexOutOfBoundsException if index are outside of array boundaries
     */
    fun addBlobElement(index: Int, element: BlobElement) {
        if (index >= amountOfBlobElements) {
            throw java.lang.IndexOutOfBoundsException("Adding BlobElement to index $index in array of size $amountOfBlobElements")
        }
        blobElements[index] = element
    }

    /**
     * Creates empty BLOB and reserve [amountOfData] memory for it
     * @param amountOfData amount of data that should be reserved
     */
    constructor(amountOfData: Int) {
        this.amountOfData = amountOfData
        amountOfBlobElements =
            if ((amountOfData + Int.SIZE_BYTES) % BlobElement.SIZE_BYTES == 0)
                (amountOfData + Int.SIZE_BYTES) / BlobElement.SIZE_BYTES
            else (amountOfData + Int.SIZE_BYTES) / BlobElement.SIZE_BYTES + 1
        blobElements = Array(amountOfBlobElements) { BlobElement(arrayOf(0)) }
    }

    /**
     * Creates Blob, based on the given object
     * @param obj object to create BLOB
     */
    constructor(obj: BlobConvertible) {
        var buffer = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        val tempArray = obj.toByteArray()
        amountOfData = tempArray.size
        amountOfBlobElements =
            if ((amountOfData + Int.SIZE_BYTES) % BlobElement.SIZE_BYTES == 0)
                    (amountOfData + Int.SIZE_BYTES) / BlobElement.SIZE_BYTES
            else (amountOfData + Int.SIZE_BYTES) / BlobElement.SIZE_BYTES + 1
        blobElements = Array(amountOfBlobElements) { BlobElement(arrayOf(0)) }
        for (i in 0 until Int.SIZE_BYTES) {
            buffer[i] = ((amountOfData ushr (8 * i)) % 256).toByte()
        }
        for (i in Int.SIZE_BYTES until (amountOfData + Int.SIZE_BYTES)) {
            if (i % BlobElement.SIZE_BYTES == 0) {
                blobElements[i / BlobElement.SIZE_BYTES - 1] = (BlobElement(buffer))
                buffer = Array(BlobElement.SIZE_BYTES) { 0 }
            }
            buffer[i % BlobElement.SIZE_BYTES] = tempArray[i - Int.SIZE_BYTES]
        }
        blobElements[amountOfBlobElements - 1] = BlobElement(buffer)
    }

    /**
     * Fill the given object with data, using [BlobConvertible.setDataFromBytes] method
     * @param blobConvertible object to fill
     */
    fun toBlobConvertible(blobConvertible: BlobConvertible) {
        val data = Array<Byte>(amountOfData) { 0 }
        for (i in 0 until min(amountOfData, BlobElement.SIZE_BYTES - Int.SIZE_BYTES)) {
            data[i] = blobElements[0].data[i + Int.SIZE_BYTES]
        }
        for (i in BlobElement.SIZE_BYTES - Int.SIZE_BYTES until amountOfData) {
            data[i] =
                blobElements[(i + Int.SIZE_BYTES) / BlobElement.SIZE_BYTES].data[(i + Int.SIZE_BYTES) % BlobElement.SIZE_BYTES]
        }
        blobConvertible.setDataFromBytes(data)
    }
}
