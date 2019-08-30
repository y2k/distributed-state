package io.y2k.distributed.state

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.io.*
import java.util.concurrent.atomic.AtomicReference
import kotlin.reflect.full.instanceParameter
import kotlin.reflect.full.memberFunctions

interface Socket {
    suspend fun write(data: ByteArray)
    suspend fun read(): ByteArray
}

interface Updater<T> {
    suspend fun update(f: (T) -> T)
}

object Library {

    suspend fun <T : Any> startClient(socket: Socket, init: T): Updater<T> =
        object : Updater<T> {

            var state_ = LocalState(init, Clock(0, 0))

            val resultChannel = Channel<DiffMsg.Result>()
            val result2Channel = Channel<Unit>()

            init {
                GlobalScope.launch {
                    while (true) {
                        when (val x = socket.readRemote()) {
                            is DiffMsg.Update -> tryApplyUpdate(x)
                            is DiffMsg.Result -> {
                                resultChannel.send(x)
                                result2Channel.receive()
                            }
                        }
                    }
                }
            }

            private suspend fun tryApplyUpdate(x: DiffMsg.Update) {
                val x1 =
                    changeState {
                        if (x.clock == clock.invert()) {
                            state = updateState(state, x.diff)
                            clock = clock.inc()

                            DiffMsg.Result(clock, true)
                        } else {
                            DiffMsg.Result(clock, false)
                        }
                    }
                socket.writeRemote(x1)
            }

            override suspend fun update(f: (T) -> T) {
                while (true) {
                    val (changes, newState, c) = changeState {
                        val newState = f(state)
                        Triple(computeDelta(state, newState), newState, clock)
                    }
                    if (changes.isEmpty()) return
                    socket.writeRemote(DiffMsg.Update(changes, c))

                    val remote = resultChannel.receive()
                    if (remote.success) {
                        changeState {
                            state = newState
                            clock = remote.clock.invert()
                        }
                    }
                    result2Channel.send(Unit)

                    if (remote.success) return
                    delay(100)
                }
            }

            fun <R> changeState(f: LocalState<T>.() -> R): R =
                synchronized(state_) { state_.f() }
        }

    private suspend fun Socket.writeRemote(x: DiffMsg) = write(serialize(x))
    private suspend fun Socket.readRemote(): DiffMsg = deserialize(read())

    private fun <T : Any> computeDelta(oldState: T, newState: T): List<Diff> {
        val clazz = oldState::class.java
        val changedMethods = clazz.methods
            .filter { it.name.startsWith("get") }
            .filter { it(oldState) != it(newState) }

        return changedMethods
            .map { method ->
                val p = method.name.replace("get", "")
                val x = p[0].toLowerCase() + p.substring(1)
                Diff(x, serialize(method(newState)))
            }
    }

    private fun <T : Any> updateState(oldState: T, diffs: List<Diff>): T {
        val copy = oldState::class.memberFunctions.first { it.name == "copy" }
        val instanceParam = copy.instanceParameter!!

        val args = diffs
            .map { x ->
                copy.parameters.first { it.name == x.method } to deserialize<Any>(x.content)
            }
            .plus(instanceParam to oldState)
            .toMap()

        @Suppress("UNCHECKED_CAST")
        return copy.callBy(args) as T
    }

    private fun serialize(x: Any?): ByteArray {
        val r = ByteArrayOutputStream()
        ObjectOutputStream(r).use { it.writeObject(x) }
        return r.toByteArray()
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> deserialize(bytes: ByteArray): T {
        val s = ByteArrayInputStream(bytes)
        return s.use { ObjectInputStream(it).readObject() as T }
    }

    private class LocalState<T>(var state: T, var clock: Clock)
    private data class Clock(val local: Long, val remote: Long) : Serializable
    private class Diff(val method: String, val content: ByteArray) : Serializable
    private sealed class DiffMsg : Serializable {
        class Update(val diff: List<Diff>, val clock: Clock) : DiffMsg()
        class Result(val clock: Clock, val success: Boolean) : DiffMsg()
    }

    private fun Clock.invert() = Clock(remote, local)
    private fun Clock.inc() = Clock(remote + 1, local + 1)
}

suspend fun <T, R> Updater<T>.read(f: (T) -> Pair<T, R>): R {
    val r = AtomicReference<R>()
    update { val (x, y) = f(it);r.set(y);x }
    return r.get()
}
