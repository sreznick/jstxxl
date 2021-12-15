package com.github.sreznick.jstxxl.queue

import com.github.sreznick.jstxxl.buffer.BlockByteSemiDeque
import com.github.sreznick.jstxxl.platformdeps.bytebuffer.ByteBufferProvider
import com.github.sreznick.jstxxl.platformdeps.storage.RandomAccessFileSyncStorage
import com.github.sreznick.jstxxl.platformdeps.storage.SyncStorage
import java.io.File
import java.io.RandomAccessFile

abstract class AbstractQueueBasic(
    private val queueFileName: String,
    bufferProvider: ByteBufferProvider
) {
    protected var currentBlocksOnDisk: Long = 0

    private var poppedBytes: Long = 0

    abstract val size: Long

    fun isEmpty(): Boolean = size == 0L

    protected fun mustNotBeEmpty() {
        if (isEmpty()) {
            throw NoSuchElementException()
        }
    }

    protected abstract fun unitSize(): Int

    protected val queueTop by lazy {
        BlockByteSemiDeque(BLOCK_SIZE, 4, bufferProvider)
    }

    private val queueOfStorages = ArrayDeque<SyncStorage>(listOf(RandomAccessFileSyncStorage(RandomAccessFile("0$queueFileName", "rw"))))

    private var counterHeadFiles = 0
    private var counterTailFiles = 1

    private fun addStorage() {
        queueOfStorages.add(RandomAccessFileSyncStorage(RandomAccessFile(counterTailFiles.toString() + queueFileName, "rw")))
        counterTailFiles++
    }

    protected fun storeBlockToDisk() {
        val position = poppedBytes + currentBlocksOnDisk * BLOCK_SIZE
        if (position >= MAX_FILE_SIZE * queueOfStorages.size) {
            addStorage()
        }
        val storage = queueOfStorages.last()

        storage.seek(position % MAX_FILE_SIZE)
        queueTop.popBackBlockInto(storage)
        currentBlocksOnDisk++
    }

    private fun deleteStorage() {
        queueOfStorages.removeFirst()
        poppedBytes -= MAX_FILE_SIZE

        File(counterHeadFiles.toString() + queueFileName).delete()
        counterHeadFiles++
    }

    protected fun loadBlockFromDisk() {
        if (poppedBytes >= MAX_FILE_SIZE) {
            deleteStorage()
        }
        val storage = queueOfStorages.first()

        storage.seek(poppedBytes)
        queueTop.pushFrontBlockFrom(storage)
        poppedBytes += BLOCK_SIZE
        currentBlocksOnDisk--
    }

    companion object {
        const val BLOCK_SIZE = 4096
        const val MAX_FILE_SIZE = 4 * BLOCK_SIZE
    }
}
