@file:Suppress("EXPERIMENTAL_API_USAGE")

package io.y2k.distributed.state

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import java.io.Serializable
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTest {

    private val random = ThreadLocalRandom.current()
    private val socket = object : Socket {
        private val buffer = Channel<ByteArray>(1)
        override suspend fun write(data: ByteArray) = buffer.send(data)
        override suspend fun read(): ByteArray = buffer.receive()
    }


    @Test(timeout = 5000)
    fun testSendAndReceive() = runBlockingTest {
        data class User(val a: String, val b: Int, val c: String?) : Serializable
        data class State(val users: Map<String, User?> = emptyMap())

        val socket = object : Socket {
            private val buffer = Channel<ByteArray>(1)
            override suspend fun write(data: ByteArray) = buffer.send(data)
            override suspend fun read(): ByteArray = buffer.receive()
        }

        val client = Library.startClient(socket, State())
        val server = Library.startClient(socket, State())

        repeat(1) {
            suspend fun assertSync(u1: Updater<State>, u2: Updater<State>) {
                u1.update { db -> db.copy(users = mapOf("alex" to null)) }

                val reqUsers = u2.read { db -> db to db.users}

                assertEquals(mapOf("alex" to null), reqUsers)

                val expected =
                    reqUsers.mapValues { User(random.nextDouble().toString(), random.nextInt(), null) }

                u2.update { it.copy(users = it.users.mapValues { expected[it.key] }) }

                val actual = u1.read { it to it.users }

                assertEquals(1, actual.size)
                assertEquals(expected, actual, "" + reqUsers)
            }

            assertSync(client, server)
//            assertSync(server, client)
        }
    }

    @Test(timeout = 5000)
    fun `test nullable`() = runBlockingTest {
        data class State(val a: String = "", val secret: String? = null, val b: String = "")

        val client = Library.startClient(socket, State())
        val server = Library.startClient(socket, State())

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
    fun test() = runBlockingTest {
        data class State(val a: String = "", val secret: String = "", val b: String = "")

        val client = Library.startClient(socket, State())
        val server = Library.startClient(socket, State())

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
