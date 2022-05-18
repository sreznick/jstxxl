package com.github.sreznick.jstxxl.btreap

abstract class AbstractBTreapPriorityQueue<Key : Comparable<Key>>(
    fileName: String,
    nodeCapacity: Int
) : AbstractBTreap<Key>(fileName, nodeCapacity) {
    private var head: Key? = null
        get() {
            if (field == null)
                field = super.first()

            return field
        }

    override fun add(key: Key) {
        if (head == null || key < head!!)
            head = key

        super.add(key)
    }

    fun poll(): Key? {
        val result = head

        if (result != null)
            remove(result)

        head = null
        return result
    }

    fun peek() = head
}