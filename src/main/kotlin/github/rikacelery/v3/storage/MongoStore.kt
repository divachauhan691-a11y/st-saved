package github.rikacelery.v3.storage

import github.rikacelery.v3.data.Room
import github.rikacelery.v3.data.User
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.encodeToString
import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.ReplaceOptions
import org.bson.Document
import org.slf4j.LoggerFactory

class MongoStore(private val uri: String?) {
    private val logger = LoggerFactory.getLogger("v3.MongoStore")
    private val json = Json { ignoreUnknownKeys = true }
    private val client: MongoClient?
    private val db: MongoDatabase?

    init {
        if (uri.isNullOrBlank()) {
            client = null; db = null
            logger.info("MongoDB not configured")
        } else try {
            client = MongoClients.create(uri)
            db = client.getDatabase("st-saved")
            db.runCommand(Document("ping", 1))
            logger.info("MongoDB connected")
        } catch (e: Exception) {
            logger.error("MongoDB connection failed: ${e.message}")
            throw e
        }
    }

    fun isConnected() = client != null

    // ── Rooms ──
    suspend fun loadRooms(): List<Room> = withContext(Dispatchers.IO) {
        if (db == null) return@withContext emptyList()
        val col = db.getCollection<Document>("rooms")
        col.find().map { doc ->
            doc.remove("_id")
            json.decodeFromString<Room>(doc.toJson())
        }.toList()
    }

    suspend fun saveRooms(rooms: List<Room>) = withContext(Dispatchers.IO) {
        if (db == null) return@withContext
        val col = db.getCollection<Document>("rooms")
        col.drop()
        if (rooms.isEmpty()) return@withContext
        val docs = rooms.map { room ->
            val doc = Document.parse(json.encodeToString(room))
            doc.put("_id", room.id)
            doc
        }
        col.insertMany(docs)
        logger.info("Saved ${rooms.size} rooms to MongoDB")
    }

    suspend fun saveRoom(room: Room) = withContext(Dispatchers.IO) {
        if (db == null) return@withContext
        val col = db.getCollection<Document>("rooms")
        val doc = Document.parse(json.encodeToString(room))
        doc.put("_id", room.id)
        col.replaceOne(Document("_id", room.id), doc, ReplaceOptions().upsert(true))
    }

    suspend fun deleteRoom(roomId: Long) = withContext(Dispatchers.IO) {
        if (db == null) return@withContext
        db.getCollection<Document>("rooms").deleteOne(Document("_id", roomId))
    }

    // ── Users ──
    suspend fun loadUsers(): List<User> = withContext(Dispatchers.IO) {
        if (db == null) return@withContext emptyList()
        val col = db.getCollection<Document>("users")
        col.find().map { doc ->
            User(
                cookie = doc.getString("cookie") ?: "",
                userId = doc.getLong("userId") ?: doc.getInteger("userId")?.toLong() ?: 0L,
                username = doc.getString("username") ?: "",
                coins = doc.getLong("coins") ?: doc.getInteger("coins")?.toLong() ?: 0L
            )
        }.toList()
    }

    suspend fun saveUsers(users: List<User>) = withContext(Dispatchers.IO) {
        if (db == null) return@withContext
        val col = db.getCollection<Document>("users")
        col.drop()
        if (users.isEmpty()) return@withContext
        val docs = users.map { user ->
            Document(mapOf(
                "_id" to user.userId,
                "cookie" to user.cookie,
                "userId" to user.userId,
                "username" to user.username,
                "coins" to user.coins
            ))
        }
        col.insertMany(docs)
        logger.info("Saved ${users.size} users to MongoDB")
    }

    // ── Config KV ──
    suspend fun saveConfig(key: String, value: String) = withContext(Dispatchers.IO) {
        if (db == null) return@withContext
        val col = db.getCollection<Document>("config")
        col.replaceOne(Document("_id", key), Document("_id", key).append("value", value),
            ReplaceOptions().upsert(true))
    }

    suspend fun loadConfig(key: String): String? = withContext(Dispatchers.IO) {
        if (db == null) return@withContext null
        val col = db.getCollection<Document>("config")
        col.find(Document("_id", key)).firstOrNull()?.getString("value")
    }

    fun close() {
        try { client?.close() } catch (_: Exception) { }
    }
}
