package io.y2k.distributed.state

import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.io.*
import java.util.concurrent.ThreadLocalRandom
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

            private suspend fun tryApplyUpdate(msg: DiffMsg.Update) {
                val result =
                    changeState {
                        if (clock.local == msg.clock.remote) {
                            state = updateState(state, msg.diff)
                            clock = Clock(clock.local + 1, msg.clock.local)
                            DiffMsg.Result(clock, true)
                        } else {
                            DiffMsg.Result(clock, false)
                        }
                    }
                socket.writeRemote(result)
            }

            private val lock = Mutex()
            private val random = ThreadLocalRandom.current()

            override suspend fun update(f: (T) -> T) {
                var delayMs = random.nextDouble(25.0, 75.0)
                while (true)
                    lock.withLock {
                        val (delta, newState, c) = changeState {
                            val newState = f(state)
                            val delta = computeDelta(state, newState)
                            if (delta.isNotEmpty())
                                clock = clock.incLocal()
                            Triple(delta, newState, clock)
                        }
                        if (delta.isEmpty()) return
                        socket.writeRemote(DiffMsg.Update(delta, c))

                        val remote = resultChannel.receive()
                        changeState {
                            if (remote.success)
                                state = newState
                            clock = clock.copy(remote = remote.clock.local)
                        }
                        result2Channel.send(Unit)

                        if (remote.success) return

                        delay(delayMs.toLong())
                        delayMs *= random.nextDouble(1.5, 2.5)
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

    private fun Clock.incLocal() = copy(local = local + 1)
}

suspend fun <T, R> Updater<T>.read(f: (T) -> Pair<T, R>): R {
    val r = AtomicReference<R>()
    update { val (x, y) = f(it);r.set(y);x }
    return r.get()
}
