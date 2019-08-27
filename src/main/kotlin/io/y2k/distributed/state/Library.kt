package io.y2k.distributed.state

import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.Serializable

interface Socket {
    suspend fun write(data: ByteArray)
    suspend fun read(): ByteArray
}

interface Updater<T> {
    suspend fun update(f: (T) -> T)
    suspend fun <R> read(f: (T) -> Pair<T, R>): R
}

object Library {

    suspend fun <T : Any> startClient(socket: Socket, init: T): Updater<T> =
        object : Updater<T> {

            var state = init

            override suspend fun update(f: (T) -> T) {
                val data = computeDelta(state, f(state))
                socket.write(data)
            }

            override suspend fun <R> read(f: (T) -> Pair<T, R>): R {
                val bytes = socket.read()
                state = updateState(state, bytes)
                val (_, r) = f(state)
                return r
            }
        }

    private fun <T : Any> computeDelta(oldState: T, newState: T): ByteArray {
        val c = oldState::class.java
        val changedMethods = c.methods
            .filter { it.name.startsWith("get") }
            .filter { it(oldState) != it(newState) }

        val changes =
            changedMethods
                .map { Diff(it.name, serialize(it(newState))) }

        return serialize(changes)
    }

    private fun serialize(x: Any): ByteArray {
        val r = ByteArrayOutputStream()
        ObjectOutputStream(r).use { it.writeObject(x) }
        return r.toByteArray()
    }

    private fun <T> updateState(oldState: T, bytes: ByteArray): T {
        TODO()
    }

    class Diff(val method: String, val content: ByteArray) : Serializable
}
