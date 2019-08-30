package io.y2k.distributed.state.common

import io.y2k.distributed.state.Socket
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import okhttp3.*
import okio.ByteString
import okio.ByteString.Companion.toByteString
import org.java_websocket.handshake.ClientHandshake
import org.java_websocket.server.WebSocketServer
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom

class SocketFactory {

    private val port = ThreadLocalRandom.current().nextInt(20_000, 30_000)

    suspend fun mkServer(): Socket {
        val serverBuffer = Channel<ByteArray>(Channel.UNLIMITED)

        val openLock = CompletableDeferred<Unit>()

        val server = object : WebSocketServer(InetSocketAddress("127.0.0.1", port)) {
            override fun onError(conn: org.java_websocket.WebSocket?, ex: Exception?) = Unit
            override fun onStart() = Unit
            override fun onOpen(conn: org.java_websocket.WebSocket?, handshake: ClientHandshake?) {
                openLock.complete(Unit)
            }

            override fun onMessage(conn: org.java_websocket.WebSocket?, message: String) = Unit
            override fun onClose(conn: org.java_websocket.WebSocket?, code: Int, reason: String?, remote: Boolean) =
                Unit

            override fun onMessage(conn: org.java_websocket.WebSocket?, message: ByteBuffer) {
                serverBuffer.offer(message.array())
            }
        }
        server.start()

        return object : Socket {
            override suspend fun read(): ByteArray = serverBuffer.receive()
            override suspend fun write(data: ByteArray) {
                openLock.await()
                server.broadcast(data)
            }
        }
    }

    fun mkClient(): Socket {
        val clientBuffer = Channel<ByteArray>(Channel.UNLIMITED)

        val r = Request.Builder().url("ws://127.0.0.1:$port/").build()
        val socket = OkHttpClient().newWebSocket(r, object : WebSocketListener() {
            override fun onClosing(webSocket: WebSocket, code: Int, reason: String) = Unit
            override fun onClosed(webSocket: WebSocket, code: Int, reason: String) = Unit
            override fun onOpen(webSocket: WebSocket, response: Response) = Unit
            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) = t.printStackTrace()

            override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                clientBuffer.offer(bytes.toByteArray())
            }
        })

        return object : Socket {
            override suspend fun write(data: ByteArray) {
                socket.send(data.toByteString())
            }

            override suspend fun read(): ByteArray = clientBuffer.receive()
        }
    }
}
