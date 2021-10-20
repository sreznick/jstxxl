package com.github.sreznick.jstxxl.platformdeps.bytebuffer

import java.nio.ByteBuffer

class AllocatingByteBufferProvider(private val allocateDirect: Boolean) : ByteBufferProvider {
    override fun getBuffer(capacity: Int): ByteBuffer =
            if (allocateDirect) {
                ByteBuffer.allocateDirect(capacity)
            } else {
                ByteBuffer.allocate(capacity)
            }

}
