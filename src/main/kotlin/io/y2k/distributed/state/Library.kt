package io.y2k.distributed.state

import java.io.*
import kotlin.reflect.full.instanceParameter
import kotlin.reflect.full.memberFunctions

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
                val (newState, r) = f(state)

                if (newState != state) update { newState }

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
                .map { method ->
                    val p = method.name.replace("get", "")
                    val x = p[0].toLowerCase() + p.substring(1)
                    Diff(x, serialize(method(newState)))
                }

        return serialize(changes)
    }

    private fun serialize(x: Any?): ByteArray {
        val r = ByteArrayOutputStream()
        ObjectOutputStream(r).use { it.writeObject(x) }
        return r.toByteArray()
    }

    private fun <T : Any> updateState(oldState: T, bytes: ByteArray): T {
        val diffs = deserialize<List<Diff>>(bytes)

        val copy = oldState::class.memberFunctions.first { it.name == "copy" }
        val instanceParam = copy.instanceParameter!!

        val args = diffs
            .map { x ->
                copy.parameters.first { it.name == x.method } to
                    deserialize<Any>(x.content)
            }
            .plus(instanceParam to oldState)
            .toMap()

        @Suppress("UNCHECKED_CAST")
        return copy.callBy(args) as T
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> deserialize(bytes: ByteArray): T {
        val s = ByteArrayInputStream(bytes)
        return s.use { ObjectInputStream(it).readObject() as T }
    }

    class Diff(val method: String, val content: ByteArray) : Serializable
}
