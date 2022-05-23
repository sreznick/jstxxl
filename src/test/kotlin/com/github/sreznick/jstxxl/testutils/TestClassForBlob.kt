package com.github.sreznick.jstxxl.testutils

import com.github.sreznick.jstxxl.blob_helper.BlobConvertible

class TestClassForBlob() : BlobConvertible {

    var testArray: Array<Int>

    init {
        testArray = Array(0) { 0 }
    }

    override fun toByteArray(): Array<Byte> {
        val tmp = Array<Byte>(testArray.size * 4 + 4) { 0 }
        for (i in 0 until 4) {
            tmp[i] = (testArray.size shr (8 * i)).toByte()
        }
        for (i in 4 until tmp.size) {
            tmp[i] = (testArray[(i - 4) / 4] shr (8 * (i % 4))).toByte()
        }
        return tmp
    }

    override fun setDataFromBytes(bytes: Array<Byte>) {
        var arraySize = 0
        for (i in 0 until 4) {
            arraySize += bytes[i].toUByte().toInt() shl (8 * i)
        }
        testArray = Array(arraySize) { 0 }
        for (i in 4 until bytes.size) {
            testArray[(i - 4) / 4] += bytes[i].toUByte().toInt() shl (8 * (i % 4))
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as TestClassForBlob

        if (!testArray.contentEquals(other.testArray)) return false

        return true
    }

    override fun hashCode(): Int {
        return testArray.contentHashCode()
    }

    constructor(array: Array<Int>) : this() {
        testArray = array
    }
}