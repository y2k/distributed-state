#### Dependency

[![](https://jitpack.io/v/y2k/distributed-state.svg)](https://jitpack.io/#y2k/distributed-state)

### Examples

#### Client
```kotlin
data class State(
    val messageQueue: List<Message> = emptyList(), 
    val totalMessages: Int = 0
)

val client = startClient(socket, State())

suspend fun sendMessageToUser(id: UserId, text: String) {
    client.update { db ->
        db.copy(db.messageQueue + Message(id, text))
    }
}
```

#### Server
```kotlin
val server = startClient(socket, State())

suspend fun onUpdate() {
    val messages =
        server.read { db ->
            db.copy(messageQueue = emptyList()) to db.messageQueue
        }

    messages.forEach { doSendMessage(it) }

    server.update { db ->
        db.copy(totalMessages = db.totalMessages + messages.size)
    }
}
```
