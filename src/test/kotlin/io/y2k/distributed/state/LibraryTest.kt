package io.y2k.distributed.state

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.test.runBlockingTest
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTest {

    @Test(timeout = 2000)
    fun testSomeLibraryMethod() = runBlockingTest {
        val socket = object : Socket {
            private val buffer = Channel<ByteArray>(1)
            override suspend fun write(data: ByteArray) = buffer.send(data)
            override suspend fun read(): ByteArray = buffer.receive()
        }

        val client = Library.startClient(socket, State())
        val server = Library.startClient(socket, State())

        for (i in 1..10) {
            val secret = Math.random().toString()
            client.update { db -> db.copy(secret = secret) }
            val actual = server.read { db -> db to db.secret }
            assertEquals(secret, actual)
        }
    }

    data class State(val secret: String = "")
}
