package com.github.sreznick.jstxxl.buffer

import com.github.sreznick.jstxxl.blob_helper.BlobElement
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage
import java.nio.ByteBuffer

/**
 * A wrapper for the ByteBuffer class for internal use.
 * Does not perform additional bound checks.
 */
class BlockByteSemiDeque(
    private val blockSizeBytes: Int,
    capacityBlocks: Int,
    bufferProvider: ByteBufferProvider
) {
    private val capacityHeadBlocks = capacityBlocks / 2
    private val capacityTailBlocks = capacityBlocks - capacityHeadBlocks

    private val headInnerBuffer: ByteBuffer = bufferProvider.getBuffer(blockSizeBytes * capacityHeadBlocks)
    private val tailInnerBuffer: ByteBuffer = bufferProvider.getBuffer(blockSizeBytes * capacityTailBlocks)

    private var currentHeadBlock: Int = 0
    private var currentTailBlock: Int = 0

    private var currentHeadSizeBytes: Int = 0
    private var currentTailSizeBytes: Int = 0

    val fullHeadBlocks: Int get() = sizeHeadBytes / blockSizeBytes
    val freeHeadBlocks: Int get() = (capacityHeadBytes - sizeHeadBytes) / blockSizeBytes
    val fullTailBlocks: Int get() = sizeTailBytes / blockSizeBytes
    val freeTailBlocks: Int get() = (capacityTailBytes - sizeTailBytes) / blockSizeBytes

    val capacityHeadBytes: Int get() = capacityHeadBlocks * blockSizeBytes
    val capacityHeadShorts: Int get() = capacityHeadBytes / Short.SIZE_BYTES
    val capacityHeadInts: Int get() = capacityHeadBytes / Int.SIZE_BYTES
    val capacityHeadLongs: Int get() = capacityHeadBytes / Long.SIZE_BYTES
    val capacityHeadChars: Int get() = capacityHeadBytes / Char.SIZE_BYTES
    val capacityHeadFloats: Int get() = capacityHeadBytes / Float.SIZE_BYTES
    val capacityHeadDoubles: Int get() = capacityHeadBytes / Double.SIZE_BYTES
    val capacityHeadBlobElement: Int get() = capacityHeadBytes / BlobElement.SIZE_BYTES

    val capacityTailBytes: Int get() = capacityTailBlocks * blockSizeBytes
    val capacityTailShorts: Int get() = capacityTailBytes / Short.SIZE_BYTES
    val capacityTailInts: Int get() = capacityTailBytes / Int.SIZE_BYTES
    val capacityTailLongs: Int get() = capacityTailBytes / Long.SIZE_BYTES
    val capacityTailChars: Int get() = capacityTailBytes / Char.SIZE_BYTES
    val capacityTailFloats: Int get() = capacityTailBytes / Float.SIZE_BYTES
    val capacityTailDoubles: Int get() = capacityTailBytes / Double.SIZE_BYTES
    val capacityTailBlobElement: Int get() = capacityTailBytes / BlobElement.SIZE_BYTES

    val sizeHeadBytes: Int get() = currentHeadSizeBytes
    val sizeHeadShorts: Int get() = sizeHeadBytes / Short.SIZE_BYTES
    val sizeHeadInts: Int get() = sizeHeadBytes / Int.SIZE_BYTES
    val sizeHeadLongs: Int get() = sizeHeadBytes / Long.SIZE_BYTES
    val sizeHeadChars: Int get() = sizeHeadBytes / Char.SIZE_BYTES
    val sizeHeadFloats: Int get() = sizeHeadBytes / Float.SIZE_BYTES
    val sizeHeadDoubles: Int get() = sizeHeadBytes / Double.SIZE_BYTES
    val sizeHeadBlobElement: Int get() = sizeHeadBytes / BlobElement.SIZE_BYTES

    val sizeTailBytes: Int get() = currentTailSizeBytes
    val sizeTailShorts: Int get() = sizeTailBytes / Short.SIZE_BYTES
    val sizeTailInts: Int get() = sizeTailBytes / Int.SIZE_BYTES
    val sizeTailLongs: Int get() = sizeTailBytes / Long.SIZE_BYTES
    val sizeTailChars: Int get() = sizeTailBytes / Char.SIZE_BYTES
    val sizeTailFloats: Int get() = sizeTailBytes / Float.SIZE_BYTES
    val sizeTailDoubles: Int get() = sizeTailBytes / Double.SIZE_BYTES
    val sizeTailBlobElement: Int get() = sizeTailBytes / BlobElement.SIZE_BYTES

    val blockSizeShorts: Int get() = blockSizeBytes / Short.SIZE_BYTES
    val blockSizeInts: Int get() = blockSizeBytes / Int.SIZE_BYTES
    val blockSizeLongs: Int get() = blockSizeBytes / Long.SIZE_BYTES
    val blockSizeChars: Int get() = blockSizeBytes / Char.SIZE_BYTES
    val blockSizeFloats: Int get() = blockSizeBytes / Float.SIZE_BYTES
    val blockSizeDoubles: Int get() = blockSizeBytes / Double.SIZE_BYTES
    val blockSizeBlobElement: Int get() = blockSizeBytes / BlobElement.SIZE_BYTES

    private val headByte: Int get() = currentHeadBlock * blockSizeBytes
    private val headShort: Int get() = currentHeadBlock * blockSizeShorts
    private val headInt: Int get() = currentHeadBlock * blockSizeInts
    private val headLong: Int get() = currentHeadBlock * blockSizeLongs
    private val headChar: Int get() = currentHeadBlock * blockSizeChars
    private val headFloat: Int get() = currentHeadBlock * blockSizeFloats
    private val headDouble: Int get() = currentHeadBlock * blockSizeDoubles
    private val headBlobElement: Int get() = currentHeadBlock * blockSizeBlobElement

    private val tailByte: Int get() = currentTailBlock * blockSizeBytes
    private val tailShort: Int get() = currentTailBlock * blockSizeShorts
    private val tailInt: Int get() = currentTailBlock * blockSizeInts
    private val tailLong: Int get() = currentTailBlock * blockSizeLongs
    private val tailChar: Int get() = currentTailBlock * blockSizeChars
    private val tailFloat: Int get() = currentTailBlock * blockSizeFloats
    private val tailDouble: Int get() = currentTailBlock * blockSizeDoubles
    private val tailBlobElement: Int get() = currentTailBlock * blockSizeBlobElement

    fun getHeadByte(index: Int): Byte = headInnerBuffer.get(byteHeadIndexToInner(index))
    fun getHeadShort(index: Int): Short = headInnerBuffer.getShort(shortHeadIndexToInner(index))
    fun getHeadInt(index: Int): Int = headInnerBuffer.getInt(intHeadIndexToInner(index))
    fun getHeadLong(index: Int): Long = headInnerBuffer.getLong(longHeadIndexToInner(index))
    fun getHeadChar(index: Int): Char = headInnerBuffer.getChar(charHeadIndexToInner(index))
    fun getHeadFloat(index: Int): Float = headInnerBuffer.getFloat(floatHeadIndexToInner(index))
    fun getHeadDouble(index: Int): Double = headInnerBuffer.getDouble(doubleHeadIndexToInner(index))
    fun getHeadBlobElement(index: Int): BlobElement {
        val innerIndex = blobElementHeadIndexToInner(index)
        val tmpArray = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        for (i in tmpArray.indices) {
            tmpArray[i] = headInnerBuffer.get(innerIndex + i)
        }
        return BlobElement(tmpArray)
    }

    fun getTailByte(index: Int): Byte = tailInnerBuffer.get(byteTailIndexToInner(index))
    fun getTailShort(index: Int): Short = tailInnerBuffer.getShort(shortTailIndexToInner(index))
    fun getTailInt(index: Int): Int = tailInnerBuffer.getInt(intTailIndexToInner(index))
    fun getTailLong(index: Int): Long = tailInnerBuffer.getLong(longTailIndexToInner(index))
    fun getTailChar(index: Int): Char = tailInnerBuffer.getChar(charTailIndexToInner(index))
    fun getTailFloat(index: Int): Float = tailInnerBuffer.getFloat(floatTailIndexToInner(index))
    fun getTailDouble(index: Int): Double = tailInnerBuffer.getDouble(doubleTailIndexToInner(index))
    fun getTailBlobElement(index: Int): BlobElement {
        val innerIndex = blobElementTailIndexToInner(index)
        val tmpArray = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        for (i in tmpArray.indices) {
            tmpArray[i] = headInnerBuffer.get(innerIndex + i)
        }
        return BlobElement(tmpArray)
    }

    fun isEmptyHeadBytes() = sizeHeadBytes == 0
    fun isEmptyHeadShorts() = sizeHeadShorts == 0
    fun isEmptyHeadInts() = sizeHeadInts == 0
    fun isEmptyHeadLongs() = sizeHeadLongs == 0
    fun isEmptyHeadChars() = sizeHeadChars == 0
    fun isEmptyHeadFloats() = sizeHeadFloats == 0
    fun isEmptyHeadDoubles() = sizeHeadDoubles == 0
    fun isEmptyHeadBlobElement() = sizeHeadBlobElement == 0

    fun isEmptyTailBytes() = sizeTailBytes == 0
    fun isEmptyTailShorts() = sizeTailShorts == 0
    fun isEmptyTailInts() = sizeTailInts == 0
    fun isEmptyTailLongs() = sizeTailLongs == 0
    fun isEmptyTailChars() = sizeTailChars == 0
    fun isEmptyTailFloats() = sizeTailFloats == 0
    fun isEmptyTailDoubles() = sizeTailDoubles == 0
    fun isEmptyTailBlobElement() = sizeTailBlobElement == 0

    private fun byteHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadBytes)
    private fun shortHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadShorts)
    private fun intHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadInts)
    private fun longHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadLongs)
    private fun charHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadChars)
    private fun floatHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadFloats)
    private fun doubleHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadDoubles)
    private fun blobElementHeadIndexToInner(index: Int): Int = indexToInner(index, 0, capacityHeadBlobElement)

    private fun byteTailIndexToInner(index: Int): Int = indexToInner(index, tailByte, capacityTailBytes)
    private fun shortTailIndexToInner(index: Int): Int = indexToInner(index, tailShort, capacityTailShorts)
    private fun intTailIndexToInner(index: Int): Int = indexToInner(index, tailInt, capacityTailInts)
    private fun longTailIndexToInner(index: Int): Int = indexToInner(index, tailLong, capacityTailLongs)
    private fun charTailIndexToInner(index: Int): Int = indexToInner(index, tailChar, capacityTailChars)
    private fun floatTailIndexToInner(index: Int): Int = indexToInner(index, tailFloat, capacityTailFloats)
    private fun doubleTailIndexToInner(index: Int): Int = indexToInner(index, tailDouble, capacityTailDoubles)
    private fun blobElementTailIndexToInner(index: Int): Int =
        indexToInner(index, tailBlobElement, capacityTailBlobElement)

    fun popBackBlockInto(writer: SyncStorage) {
        mustHaveAtLeastOneFullTailBlock()
        writer.writeFrom(tailInnerBuffer, tailByte, blockSizeBytes)
        incTailBlock()
        currentTailSizeBytes -= blockSizeBytes
    }

    fun pushBackBlockFrom(reader: SyncStorage) {
        mustHaveAtLeastOneFreeTailBlock()
        decTailBlock()
        reader.readInto(tailInnerBuffer, tailByte, blockSizeBytes)
        currentTailSizeBytes += blockSizeBytes
    }

    fun popFrontBlockInto(writer: SyncStorage) {
        TODO("Not yet implemented")
    }

    fun pushFrontBlockFrom(reader: SyncStorage) {
        mustHaveAtLeastOneFreeHeadBlock()
        reader.readInto(headInnerBuffer, headByte, blockSizeBytes)
        incHeadBlock()
        currentHeadSizeBytes += blockSizeBytes
    }

    fun pushBackByte(item: Byte) {
        tailInnerBuffer.put(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Byte.SIZE_BYTES
    }

    fun pushBackShort(item: Short) {
        tailInnerBuffer.putShort(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Short.SIZE_BYTES
    }

    fun pushBackInt(item: Int) {
        tailInnerBuffer.putInt(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Int.SIZE_BYTES
    }

    fun pushBackLong(item: Long) {
        tailInnerBuffer.putLong(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Long.SIZE_BYTES
    }

    fun pushBackChar(item: Char) {
        tailInnerBuffer.putChar(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Char.SIZE_BYTES
    }

    fun pushBackFloat(item: Float) {
        tailInnerBuffer.putFloat(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Float.SIZE_BYTES
    }

    fun pushBackDouble(item: Double) {
        tailInnerBuffer.putDouble(byteTailIndexToInner(currentTailSizeBytes), item)
        currentTailSizeBytes += Double.SIZE_BYTES
    }

    fun pushBackBlobElement(item: BlobElement) {
        for (i in item.data.indices) {
            pushBackByte(item.data[i])
        }
    }


    fun popBackByte(): Byte {
        currentTailSizeBytes -= Byte.SIZE_BYTES
        return tailInnerBuffer.get(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackShort(): Short {
        currentTailSizeBytes -= Short.SIZE_BYTES
        return tailInnerBuffer.getShort(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackInt(): Int {
        currentTailSizeBytes -= Int.SIZE_BYTES
        return tailInnerBuffer.getInt(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackLong(): Long {
        currentTailSizeBytes -= Long.SIZE_BYTES
        return tailInnerBuffer.getLong(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackChar(): Char {
        currentTailSizeBytes -= Char.SIZE_BYTES
        return tailInnerBuffer.getChar(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackFloat(): Float {
        currentTailSizeBytes -= Float.SIZE_BYTES
        return tailInnerBuffer.getFloat(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackDouble(): Double {
        currentTailSizeBytes -= Double.SIZE_BYTES
        return tailInnerBuffer.getDouble(byteTailIndexToInner(currentTailSizeBytes))
    }

    fun popBackBlobElement(): BlobElement {
        val tmpArray = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        for (i in tmpArray.indices.reversed()) {
            tmpArray[i] = popBackByte()
        }
        return BlobElement(tmpArray)
    }

    fun pushFrontByte(item: Byte) {
        TODO("Not yet implemented")
    }

    fun pushFrontShort(item: Short) {
        TODO("Not yet implemented")
    }

    fun pushFrontInt(item: Int) {
        TODO("Not yet implemented")
    }

    fun pushFrontLong(item: Long) {
        TODO("Not yet implemented")
    }

    fun pushFrontChar(item: Char) {
        TODO("Not yet implemented")
    }

    fun pushFrontFloat(item: Float) {
        TODO("Not yet implemented")
    }

    fun pushFrontDouble(item: Double) {
        TODO("Not yet implemented")
    }

    fun pushFrontBlobElement(item: BlobElement) {
        TODO("Not yet implemented")
    }

    fun popFrontByte(): Byte =
        headInnerBuffer.get(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Byte.SIZE_BYTES
        }

    fun popFrontShort(): Short =
        headInnerBuffer.getShort(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Short.SIZE_BYTES
        }

    fun popFrontInt(): Int =
        headInnerBuffer.getInt(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Int.SIZE_BYTES
        }

    fun popFrontLong(): Long =
        headInnerBuffer.getLong(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Long.SIZE_BYTES
        }

    fun popFrontChar(): Char =
        headInnerBuffer.getChar(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Char.SIZE_BYTES
        }

    fun popFrontFloat(): Float =
        headInnerBuffer.getFloat(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Float.SIZE_BYTES
        }

    fun popFrontDouble(): Double =
        headInnerBuffer.getDouble(byteHeadIndexToInner(headByte - currentHeadSizeBytes)).also {
            currentHeadSizeBytes -= Double.SIZE_BYTES
        }

    fun popFrontBlobElement(): BlobElement {
        val tmpArray = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        for (i in tmpArray.indices) {
            tmpArray[i] = popFrontByte()
        }
        return BlobElement(tmpArray)
    }

    private fun incTailBlock() {
        if (++currentTailBlock == capacityTailBlocks) {
            currentTailBlock = 0
        }
    }

    private fun decTailBlock() {
        if (currentTailBlock == 0) {
            currentTailBlock = capacityTailBlocks
        }
        --currentTailBlock
    }

    private fun incHeadBlock() {
        if (++currentHeadBlock == capacityHeadBlocks) {
            currentHeadBlock = 0
        }
    }

    private fun decHeadBlock() {
        if (currentHeadBlock == 0) {
            currentHeadBlock = capacityHeadBlocks
        }
        --currentHeadBlock
    }

    private fun mustHaveAtLeastOneFullTailBlock() {
        if (fullTailBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    private fun mustHaveAtLeastOneFreeTailBlock() {
        if (freeTailBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    private fun mustHaveAtLeastOneFullHeadBlock() {
        if (fullHeadBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    private fun mustHaveAtLeastOneFreeHeadBlock() {
        if (freeHeadBlocks == 0) {
            throw IndexOutOfBoundsException()
        }
    }

    companion object {
        fun indexToInner(index: Int, offset: Int, cap: Int): Int {
            val innerIndex = offset + index
            return ((innerIndex % cap) + cap) % cap
        }
    }
}
