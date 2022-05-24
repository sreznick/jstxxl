package com.github.sreznick.jstxxl.btree

import com.github.sreznick.jstxxl.blob_helper.Blob
import com.github.sreznick.jstxxl.blob_helper.BlobElement
import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer


class BlobBTree(fileName: String) : AbstractBTree<Int, Blob>(fileName) {
    override fun keySizeBytes(): Int = Int.SIZE_BYTES

    override fun valueSizeBytes(): Int = 0

    override fun readKey(byteBuffer: ByteBuffer): Int = byteBuffer.int

    override fun readValue(byteBuffer: ByteBuffer): Blob? {
        val flag = byteBuffer.get()
        if (flag.toInt() == 0){
            return null
        }
        val firstBlobElement = readBlobElement(byteBuffer)
        var amountOfData = 0
        for (i in 0 until Int.SIZE_BYTES) {
            amountOfData += firstBlobElement.data[i].toUByte().toInt() shl (i * Byte.SIZE_BITS)
        }
        val resultBlob = Blob(amountOfData)
        resultBlob.addBlobElement(0, firstBlobElement)
        for (i in 1 until resultBlob.amountOfBlobElements) {
            resultBlob.addBlobElement(i, readBlobElement(byteBuffer))
        }
        return resultBlob
    }

    private fun readBlobElement(byteBuffer: ByteBuffer): BlobElement {
        val tmpArray = Array<Byte>(BlobElement.SIZE_BYTES) { 0 }
        for (i in 0 until BlobElement.SIZE_BYTES) {
            tmpArray[i] = byteBuffer.get()
        }
        return BlobElement(tmpArray)
    }

    override fun writeValue(value: Blob?, byteBuffer: ByteBuffer) {
        if (value is Blob) {
            byteBuffer.put(1)
            for (blobElement in value.blobElements) {
                writeBlobElement(blobElement, byteBuffer)
            }
        } else {
            byteBuffer.put(0)
        }
    }

    private fun writeBlobElement(blobElement: BlobElement, byteBuffer: ByteBuffer) {
        for (byte in blobElement.data) {
            byteBuffer.put(byte)
        }
    }

    override fun writeKey(key: Int, byteBuffer: ByteBuffer) {
        byteBuffer.putInt(key)
    }

    override fun saveNode(node: Node, number: Int) {
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + number), "rw"))

        var size = 2 * Int.SIZE_BYTES
        for (i in 0 until node.numberOfEntries) {
            size += try {
                Int.SIZE_BYTES + node.entries[i]?.value!!.amountOfBlobElements * BlobElement.SIZE_BYTES + keySizeBytes() + 1
            } catch (e: java.lang.Exception) {
                Int.SIZE_BYTES + keySizeBytes() + 1
            }
        }
        val byteBuffer = ByteBuffer.allocate(size).putInt(size - 2 * Int.SIZE_BYTES).putInt(node.numberOfEntries)

        for (j in 0 until node.numberOfEntries) {
            writeKey(node.entries[j]?.key!!, byteBuffer)
            writeValue(node.entries[j]?.value, byteBuffer)
            byteBuffer.putInt(node.entries[j]?.child!!)
        }
        storage.writeFrom(byteBuffer, 0, byteBuffer.position())
        storage.close()
    }

    override fun readNode(number: Int): Node {
        val storage = RandomAccessFileSyncStorage(RandomAccessFile(File(fileName + number), "rw"))

        var size = 2 * Int.SIZE_BYTES
        var byteBuffer = ByteBuffer.allocate(size)
        storage.readInto(byteBuffer, 0, size)
        val amountOfData = byteBuffer.int
        val numberOfKeys = byteBuffer.int
        size = amountOfData
        byteBuffer = ByteBuffer.allocate(size)
        storage.readInto(byteBuffer, 0, size)
        storage.close()

        val node = Node(numberOfKeys)

        for (i in 0 until numberOfKeys) {
            val key = readKey(byteBuffer)
            val value = readValue(byteBuffer)
            val next = byteBuffer.int
            node.entries[i] = Entry(key, value, next)
        }
        return node
    }
}
