package com.github.sreznick.jstxxl.testutils

import kotlin.random.Random

object TestUtils {
    private const val RANDOM_SEED: Long = 8550528944245072046L

    fun randomLongToInt(index: Long): Int = Random(RANDOM_SEED xor index).nextInt()
}
