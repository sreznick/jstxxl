package com.github.sreznick.jstxxl.blob_helper

/**
 * Stores [SIZE_BYTES] bytes. Objects of this class are used in [Blob] realisation as parts of fixed length
 * @param _data array of bytes to be stored, its length should be [SIZE_BYTES]
 * @exception java.lang.Exception if length of _data doesn't equal to [SIZE_BYTES]
 */
class BlobElement(_data: Array<Byte>) {
    companion object {
        const val SIZE_BYTES = 128
    }

    var data: Array<Byte>

    init {
        if (_data.size != SIZE_BYTES){
            throw java.lang.Exception("Data contains unexpected amount of elements. ${_data.size} != $SIZE_BYTES")
        }
        data = _data
    }
}