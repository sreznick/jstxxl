package com.github.sreznick.jstxxl.blob_helper

/**
 * Interface for converting to [Blob]
 */
interface BlobConvertible {
    /**
     * Returns object as an array of bytes
     * @return Representation of an object as an array of bytes
     */
    fun toByteArray() : Array<Byte>

    /**
     * Fill the object with the given data. All [Blob] methods use this method
     * with the same data representation as in output of
     * [BlobConvertible.toByteArray] implementation
     * @param bytes data to be set.
     */
    fun setDataFromBytes(bytes: Array<Byte>)
}