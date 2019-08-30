@file:Suppress("EXPERIMENTAL_API_USAGE")

package io.y2k.distributed.state

import io.y2k.distributed.state.common.SocketFactory
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import java.io.Serializable
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.Ignore
import kotlin.test.Test
import kotlin.test.assertEquals

class LibraryTest {

    private val random = ThreadLocalRandom.current()
    private val socket = SocketFactory()

    @Ignore
    @Test(timeout = 5000)
    fun `test race conditions`() = runBlocking {
        data class State(val a: Int = 0, val b: Int = 0) : Serializable
        initEnvironment(State()) { local, remote ->
            val iterations = 100
            local.update { State(0, 0) }

            fun increaseAsync(u: Updater<State>, f: (State) -> State) = async {
                repeat(iterations) {
                    u.update { f(it) }
                    delay(random.nextLong(0, 20))
                }
            }

            listOf(
                increaseAsync(local) { it.copy(a = it.a + 1) },
                increaseAsync(remote) { it.copy(b = it.b + 1) },
                increaseAsync(local) { it.copy(a = it.a + 1, b = it.b + 1) },
                increaseAsync(remote) { it.copy(a = it.a + 1, b = it.b + 1) }
            ).awaitAll()

            assertEquals(State(3 * iterations, 3 * iterations), local.read { it to it })
            assertEquals(State(3 * iterations, 3 * iterations), remote.read { it to it })
        }
    }

    @Test(timeout = 5000)
    fun testSendAndReceive() = runBlocking {
        data class User(val a: String, val b: Int, val c: String?) : Serializable
        data class State(val users: Map<String, User?> = emptyMap())

        initEnvironment(State()) { local, remote ->
            val expected1 = mapOf(random.nextLong().toString() to null)
            local.update { db -> db.copy(users = expected1) }

            val reqUsers = remote.read { db -> db to db.users }

            assertEquals(expected1, reqUsers)

            val expected =
                reqUsers.mapValues { User(random.nextDouble().toString(), random.nextInt(), null) }

            remote.update { it.copy(users = it.users.mapValues { expected[it.key] }) }

            val actual = local.read { it to it.users }

            assertEquals(1, actual.size)
            assertEquals(expected, actual, "" + reqUsers)
        }
    }

    @Test(timeout = 5000)
    fun `test nullable`() = runBlocking {
        data class State(val a: String = "", val secret: String? = null, val b: String = "")

        initEnvironment(State()) { local, remote ->
            val secret = random.nextDouble().toString()
            local.update { db -> db.copy(secret = secret) }
            val actual = remote.read { db -> db.copy(secret = null) to db.secret }
            assertEquals(secret, actual)

            val actual2 = local.read { db -> db to db.secret }
            assertEquals(null, actual2)
        }
    }

    @Test(timeout = 5000)
    fun test() = runBlocking {
        data class State(val a: String = "", val secret: String = "", val b: String = "")

        initEnvironment(State()) { locale, remote ->
            val secret = random.nextDouble().toString()
            locale.update { db -> db.copy(secret = secret) }
            val actual = remote.read { db -> db.copy(secret = "") to db.secret }
            assertEquals(secret, actual)

            val actual2 = locale.read { db -> db to db.secret }
            assertEquals("", actual2)
        }
    }

    private suspend fun <State : Any> initEnvironment(initState: State, f: suspend (Updater<State>, Updater<State>) -> Unit) {
        val server = Library.startClient(socket.mkServer(), initState)
        val client = Library.startClient(socket.mkClient(), initState)

        repeat(10) {
            suspend fun assertSync(u1: Updater<State>, u2: Updater<State>) {
                f(u1, u2)
            }
            assertSync(client, server)
            assertSync(server, client)
        }
    }

    @ExperimentalStdlibApi
    @Test(timeout = 5000)
    fun `socket test`() = runBlocking {
        val server = socket.mkServer()
        val client = socket.mkClient()
        client.write("hello from client".encodeToByteArray())
        server.write("hello from server".encodeToByteArray())
        assertEquals("hello from client", server.read().decodeToString())
        assertEquals("hello from server", client.read().decodeToString())
    }

    @Test(timeout = 5000)
    fun testBlocking() = runBlocking {
        data class State(val a: String = "", val secret: String? = null, val b: String = "")

        val server = Library.startClient(socket.mkServer(), State())
        val client = Library.startClient(socket.mkClient(), State())

        client.update { it.copy(secret = "X") }
        client.update { it.copy(secret = "Y") }
    }
}
