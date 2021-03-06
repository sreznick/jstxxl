package com.github.sreznick.jstxxl.testutils

import kotlin.random.Random

object TestUtils {
    private const val RANDOM_SEED: Long = 8550528944245072046L

    fun randomIntToInt(index: Int): Int = Random(RANDOM_SEED xor index.toLong()).nextInt()

    fun randomLongToInt(index: Long): Int = Random(RANDOM_SEED xor index).nextInt()

    fun randomLongToLong(index: Long): Long = Random(RANDOM_SEED xor index).nextLong()
}
