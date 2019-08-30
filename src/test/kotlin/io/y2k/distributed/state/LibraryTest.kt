@file:Suppress("EXPERIMENTAL_API_USAGE")

package io.y2k.distributed.state

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.runBlocking
import java.io.Serializable
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTest {

    private val random = ThreadLocalRandom.current()
    private val socket = object {
        private val fromClient = Channel<ByteArray>(Channel.UNLIMITED)
        private val fromServer = Channel<ByteArray>(Channel.UNLIMITED)

        fun mkServer(): Socket = object : Socket {
            override suspend fun write(data: ByteArray) = fromServer.send(data)
            override suspend fun read(): ByteArray = fromClient.receive()
        }

        fun mkClient(): Socket = object : Socket {
            override suspend fun write(data: ByteArray) = fromClient.send(data)
            override suspend fun read(): ByteArray = fromServer.receive()
        }
    }

    @Test(timeout = 3000)
    fun testBlocking() = runBlocking {
        data class State(val a: String = "", val secret: String? = null, val b: String = "")

        val client = Library.startClient(socket.mkClient(), State())
        val server = Library.startClient(socket.mkServer(), State())

        client.update { it.copy(secret = "X") }
        client.update { it.copy(secret = "Y") }
    }

    @Test(timeout = 5000)
    fun testSendAndReceive() = runBlocking {
        data class User(val a: String, val b: Int, val c: String?) : Serializable
        data class State(val users: Map<String, User?> = emptyMap())

        val client = Library.startClient(socket.mkClient(), State())
        val server = Library.startClient(socket.mkServer(), State())

        repeat(10) {
            suspend fun assertSync(u1: Updater<State>, u2: Updater<State>) {
                val expected1 = mapOf(random.nextLong().toString() to null)
                u1.update { db -> db.copy(users = expected1) }

                val reqUsers = u2.read { db -> db to db.users }

                assertEquals(expected1, reqUsers)

                val expected =
                    reqUsers.mapValues { User(random.nextDouble().toString(), random.nextInt(), null) }

                u2.update { it.copy(users = it.users.mapValues { expected[it.key] }) }

                val actual = u1.read { it to it.users }

                assertEquals(1, actual.size)
                assertEquals(expected, actual, "" + reqUsers)
            }

            assertSync(client, server)
            assertSync(server, client)
        }
    }

    @Test(timeout = 5000)
    fun `test nullable`() = runBlocking {
        data class State(val a: String = "", val secret: String? = null, val b: String = "")

        val client = Library.startClient(socket.mkClient(), State())
        val server = Library.startClient(socket.mkServer(), State())

        repeat(10) {
            suspend fun assertSync(u1: Updater<State>, u2: Updater<State>) {
                val secret = Math.random().toString()
                u1.update { db -> db.copy(secret = secret) }
                val actual = u2.read { db -> db.copy(secret = null) to db.secret }
                assertEquals(secret, actual)

                val actual2 = u1.read { db -> db to db.secret }
                assertEquals(null, actual2)
            }

            assertSync(client, server)
            assertSync(server, client)
        }
    }

    @Test(timeout = 5000)
    fun test() = runBlocking {
        data class State(val a: String = "", val secret: String = "", val b: String = "")

        val client = Library.startClient(socket.mkClient(), State())
        val server = Library.startClient(socket.mkServer(), State())

        repeat(10) {
            suspend fun assertSync(u1: Updater<State>, u2: Updater<State>) {
                val secret = Math.random().toString()
                u1.update { db -> db.copy(secret = secret) }
                val actual = u2.read { db -> db.copy(secret = "") to db.secret }
                assertEquals(secret, actual)

                val actual2 = u1.read { db -> db to db.secret }
                assertEquals("", actual2)
            }

            assertSync(client, server)
            assertSync(server, client)
        }
    }
}
