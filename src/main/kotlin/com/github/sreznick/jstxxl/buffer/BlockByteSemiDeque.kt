package com.github.sreznick.jstxxl.buffer

import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage
import java.nio.ByteBuffer

/**
 * A wrapper for the ByteBuffer class for internal use.
 * Does not perform additional bound checks.
 */
internal class BlockByteSemiDeque(val blockSizeBytes: Int, val capacityBlocks: Int, bufferProvider: ByteBufferProvider) {
    private val innerBuffer: ByteBuffer = bufferProvider.getBuffer(blockSizeBytes * capacityBlocks)
    private var currentHeadBlock: Int = 0
    private var currentSizeBytes: Int = 0

    val fullBlocks: Int get() = sizeBytes / blockSizeBytes
    val freeBlocks: Int get() = (capacityBytes - sizeBytes) / blockSizeBytes

    val capacityBytes: Int get() = capacityBlocks * blockSizeBytes
    val capacityShorts: Int get() = capacityBytes / Short.SIZE_BYTES
    val capacityInts: Int get() = capacityBytes / Int.SIZE_BYTES
    val capacityLongs: Int get() = capacityBytes / Long.SIZE_BYTES
    val capacityChars: Int get() = capacityBytes / Char.SIZE_BYTES
    val capacityFloats: Int get() = capacityBytes / Float.SIZE_BYTES
    val capacityDoubles: Int get() = capacityBytes / Double.SIZE_BYTES

    val sizeBytes: Int get() = currentSizeBytes
    val sizeShorts: Int get() = sizeBytes / Short.SIZE_BYTES
    val sizeInts: Int get() = sizeBytes / Int.SIZE_BYTES
    val sizeLongs: Int get() = sizeBytes / Long.SIZE_BYTES
    val sizeChars: Int get() = sizeBytes / Char.SIZE_BYTES
    val sizeFloats: Int get() = sizeBytes / Float.SIZE_BYTES
    val sizeDoubles: Int get() = sizeBytes / Double.SIZE_BYTES

    val blockSizeShorts: Int get() = blockSizeBytes / Short.SIZE_BYTES
    val blockSizeInts: Int get() = blockSizeBytes / Int.SIZE_BYTES
    val blockSizeLongs: Int get() = blockSizeBytes / Long.SIZE_BYTES
    val blockSizeChars: Int get() = blockSizeBytes / Char.SIZE_BYTES
    val blockSizeFloats: Int get() = blockSizeBytes / Float.SIZE_BYTES
    val blockSizeDoubles: Int get() = blockSizeBytes / Double.SIZE_BYTES

    private val headByte: Int get() = currentHeadBlock * blockSizeBytes
    private val headShort: Int get() = currentHeadBlock * blockSizeShorts
    private val headInt: Int get() = currentHeadBlock * blockSizeInts
    private val headLong: Int get() = currentHeadBlock * blockSizeLongs
    private val headChar: Int get() = currentHeadBlock * blockSizeChars
    private val headFloat: Int get() = currentHeadBlock * blockSizeFloats
    private val headDouble: Int get() = currentHeadBlock * blockSizeDoubles

    fun getByte(index: Int): Byte = innerBuffer.get(byteIndexToInner(index))
    fun getShort(index: Int): Short = innerBuffer.getShort(shortIndexToInner(index))
    fun getInt(index: Int): Int = innerBuffer.getInt(intIndexToInner(index))
    fun getLong(index: Int): Long = innerBuffer.getLong(longIndexToInner(index))
    fun getChar(index: Int): Char = innerBuffer.getChar(charIndexToInner(index))
    fun getFloat(index: Int): Float = innerBuffer.getFloat(floatIndexToInner(index))
    fun getDouble(index: Int): Double = innerBuffer.getDouble(doubleIndexToInner(index))

    private fun byteIndexToInner(index: Int): Int = indexToInner(index, headByte, capacityBytes)
    private fun shortIndexToInner(index: Int): Int = indexToInner(index, headShort, capacityShorts)
    private fun intIndexToInner(index: Int): Int = indexToInner(index, headInt, capacityInts)
    private fun longIndexToInner(index: Int): Int = indexToInner(index, headLong, capacityLongs)
    private fun charIndexToInner(index: Int): Int = indexToInner(index, headChar, capacityChars)
    private fun floatIndexToInner(index: Int): Int = indexToInner(index, headFloat, capacityFloats)
    private fun doubleIndexToInner(index: Int): Int = indexToInner(index, headDouble, capacityDoubles)

    fun popBackBlockInto(writer: SyncStorage) {
        mustHaveAtLeastOneFullBlock()
        writer.writeFrom(innerBuffer, headByte, blockSizeBytes)
        incHeadBlock()
        currentSizeBytes -= blockSizeBytes
    }

    fun pushBackBlockFrom(reader: SyncStorage) {
        mustHaveAtLeastOneFreeBlock()
        decHeadBlock()
        reader.readInto(innerBuffer, headByte, blockSizeBytes)
        currentSizeBytes += blockSizeBytes
    }

    fun pushFrontByte(item: Byte) {
        innerBuffer.put(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Byte.SIZE_BYTES
    }

    fun pushFrontShort(item: Short) {
        innerBuffer.putShort(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Short.SIZE_BYTES
    }

    fun pushFrontInt(item: Int) {
        innerBuffer.putInt(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Int.SIZE_BYTES
    }

    fun pushFrontLong(item: Long) {
        innerBuffer.putLong(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Long.SIZE_BYTES
    }

    fun pushFrontChar(item: Char) {
        innerBuffer.putChar(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Char.SIZE_BYTES
    }

    fun pushFrontFloat(item: Float) {
        innerBuffer.putFloat(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Float.SIZE_BYTES
    }

    fun pushFrontDouble(item: Double) {
        innerBuffer.putDouble(byteIndexToInner(sizeBytes), item)
        currentSizeBytes += Double.SIZE_BYTES
    }

    fun popFrontByte(): Byte {
        currentSizeBytes -= Byte.SIZE_BYTES
        return innerBuffer.get(byteIndexToInner(sizeBytes))
    }

    fun popFrontShort(): Short {
        currentSizeBytes -= Short.SIZE_BYTES
        return innerBuffer.getShort(byteIndexToInner(sizeBytes))
    }

    fun popFrontInt(): Int {
        currentSizeBytes -= Int.SIZE_BYTES
        return innerBuffer.getInt(byteIndexToInner(sizeBytes))
    }

    fun popFrontLong(): Long {
        currentSizeBytes -= Long.SIZE_BYTES
        return innerBuffer.getLong(byteIndexToInner(sizeBytes))
    }

    fun popFrontChar(): Char {
        currentSizeBytes -= Char.SIZE_BYTES
        return innerBuffer.getChar(byteIndexToInner(sizeBytes))
    }

    fun popFrontFloat(): Float {
        currentSizeBytes -= Float.SIZE_BYTES
        return innerBuffer.getFloat(byteIndexToInner(sizeBytes))
    }

    fun popFrontDouble(): Double {
        currentSizeBytes -= Double.SIZE_BYTES
        return innerBuffer.getDouble(byteIndexToInner(sizeBytes))
    }

    private fun incHeadBlock() {
        if (++currentHeadBlock == capacityBlocks) {
            currentHeadBlock = 0
        }
    }

    private fun decHeadBlock() {
        if (currentHeadBlock == 0) {
            currentHeadBlock = capacityBlocks
        }
        --currentHeadBlock
    }

    private fun mustHaveAtLeastOneFullBlock() {
        if (fullBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    private fun mustHaveAtLeastOneFreeBlock() {
        if (freeBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    companion object {
        private fun indexToInner(index: Int, head: Int, cap: Int): Int {
            var innerIndex = index + head
            if (innerIndex >= cap) {
                innerIndex -= cap
            }
            return innerIndex
        }
    }
}
