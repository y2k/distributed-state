@file:Suppress("EXPERIMENTAL_API_USAGE")

package io.y2k.distributed.state

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTest {

    @Test(timeout = 5000)
    fun `test nullable`() = runBlockingTest {
        data class State(val secret: String? = null)

        val socket = object : Socket {
            private val buffer = Channel<ByteArray>(1)
            override suspend fun write(data: ByteArray) = buffer.send(data)
            override suspend fun read(): ByteArray = buffer.receive()
        }

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
        data class State(val secret: String = "")

        val socket = object : Socket {
            private val buffer = Channel<ByteArray>(1)
            override suspend fun write(data: ByteArray) = buffer.send(data)
            override suspend fun read(): ByteArray = buffer.receive()
        }

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
