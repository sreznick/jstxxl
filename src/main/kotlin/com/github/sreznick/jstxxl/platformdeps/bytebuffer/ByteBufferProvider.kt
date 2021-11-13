package com.github.sreznick.jstxxl.platformdeps.bytebuffer

import java.nio.ByteBuffer

interface ByteBufferProvider {
    fun getBuffer(capacity: Int): ByteBuffer
}
