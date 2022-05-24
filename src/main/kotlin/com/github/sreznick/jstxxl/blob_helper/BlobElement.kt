package com.github.sreznick.jstxxl.blob_helper

/**
 * Stores [SIZE_BYTES] bytes. Objects of this class are used in [Blob] realisation as parts of fixed length
 */
class BlobElement {
    companion object {
        /**
         * Amount of bytes stored in [BlobElement]
         */
        const val SIZE_BYTES = 128
    }
    var data: Array<Byte>
        private set
    /**
     * Generate [BlobElement] with data filled by zeros
     */
    constructor(){
        data = Array(128) { 0 }
    }

    /**
     * Generate [BlobElement] with given data
     * @param _data array of bytes to be stored, its length should be [SIZE_BYTES]
     * @exception java.lang.Exception if length of _data doesn't equal to [SIZE_BYTES]
     */
    constructor(_data: Array<Byte>){
        if(_data.size != SIZE_BYTES){
            throw java.lang.Exception("Data contains unexpected amount of elements. ${_data.size} != $SIZE_BYTES")
        }
        data = _data
    }
}
