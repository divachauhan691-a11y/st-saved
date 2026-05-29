package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.core.RequestBus
import github.rikacelery.v3.data.Room
import github.rikacelery.v3.events.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.request.forms.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock

import kotlinx.serialization.json.*
import okhttp3.OkHttpClient
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

sealed interface TelegramMsg
data class TelegramPollTick(val offset: Long) : TelegramMsg
data class TelegramSendMsg(val chatId: Long, val text: String) : TelegramMsg
data class TelegramSendFile(val chatId: Long, val file: File, val caption: String) : TelegramMsg
data class TelegramProcessedFile(val event: FileProcessed) : TelegramMsg

class TelegramBotComponent(
    private val token: String,
    private val channelId: String,
    private val allowedUsers: List<Long>,
    private val requestBus: RequestBus,
    private val publicUrl: String = "",
    eventBus: EventBus,
    parentScope: CoroutineScope
) : Actor<TelegramMsg>("TelegramBot", eventBus, parentScope) {
    private val apiUrl = "https://api.telegram.org/bot$token"
    private val uploadSemaphore = Semaphore(3)
    private val httpClient = HttpClient(OkHttp) {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        install(HttpTimeout) {
            requestTimeoutMillis = 300000
            connectTimeoutMillis = 30000
            socketTimeoutMillis = 300000
        }
        engine {
            config {
                connectTimeout(30, TimeUnit.SECONDS)
                readTimeout(300, TimeUnit.SECONDS)
                writeTimeout(300, TimeUnit.SECONDS)
                retryOnConnectionFailure(false)
            }
        }
    }
    private val pollMutex = Mutex()
    private var lastUpdateId = 0L

    val isEnabled: Boolean get() = token.isNotBlank()

    override suspend fun onStart(scope: CoroutineScope) {
        if (!isEnabled) {
            logger.warn("Telegram bot not configured (TELEGRAM_BOT_TOKEN missing)")
            return
        }
        logger.info("Telegram bot started, allowed users: $allowedUsers, channel: $channelId")
        scope.launch { pollLoop() }
        if (channelId.isNotBlank()) {
            subscribe<FileProcessed>(FileProcessed::class)
        }
        if (publicUrl.isNotBlank()) {
            scope.launch { keepAwake() }
        }
    }

    private suspend fun keepAwake() {
        val url = "${publicUrl.trimEnd('/')}/health"
        logger.info("Keep-awake pinging $url every 5 min")
        while (true) {
            delay(5.minutes)
            try {
                httpClient.get(url)
            } catch (_: Exception) { }
        }
    }

    override suspend fun wrapEvent(event: Any): TelegramMsg? = when (event) {
        is FileProcessed -> TelegramProcessedFile(event)
        else -> null
    }

    private suspend fun pollLoop() {
        while (kotlin.coroutines.coroutineContext.isActive) {
            pollMutex.withLock {
                try {
                    val response: JsonObject = httpClient.post("$apiUrl/getUpdates") {
                        setBody(buildJsonObject {
                            put("offset", lastUpdateId + 1)
                            put("timeout", 30)
                        })
                        contentType(ContentType.Application.Json)
                    }.body()
                    val result = response["result"]?.jsonArray ?: return@withLock
                    for (update in result) {
                        val updateId = update.jsonObject["update_id"]?.jsonPrimitive?.long ?: continue
                        lastUpdateId = updateId
                        handleUpdate(update.jsonObject)
                    }
                } catch (e: CancellationException) { throw e }
                catch (e: Exception) {
                    logger.warn("Telegram poll error: ${e.message}")
                    delay(5.seconds)
                }
            }
        }
    }

    private suspend fun handleUpdate(update: JsonObject) {
        val msg = update["message"]?.jsonObject ?: return
        val chatId = msg["chat"]?.jsonObject?.get("id")?.jsonPrimitive?.long ?: return
        val userId = msg["from"]?.jsonObject?.get("id")?.jsonPrimitive?.long ?: return
        val text = msg["text"]?.jsonPrimitive?.contentOrNull ?: return

        if (allowedUsers.isNotEmpty() && userId !in allowedUsers) {
            sendMessage(chatId, "⛔ Unauthorized. Your ID: $userId")
            return
        }

        val parts = text.trim().split("\\s+".toRegex())
        val cmd = parts.first().lowercase()

        try {
            when {
                cmd in listOf("/start", "/help") -> cmdHelp(chatId)
                cmd == "/list" -> cmdList(chatId)
                cmd == "/status" -> cmdStatus(chatId, parts.drop(1))
                cmd == "/add" -> cmdAdd(chatId, parts.drop(1))
                cmd == "/remove" -> cmdRemove(chatId, parts.drop(1))
                cmd == "/activate" -> cmdActivate(chatId, parts.drop(1))
                cmd == "/deactivate" -> cmdDeactivate(chatId, parts.drop(1))
                cmd == "/stop" -> cmdDeactivate(chatId, parts.drop(1))
                cmd == "/quality" -> cmdQuality(chatId, parts.drop(1))
                else -> sendMessage(chatId, "Unknown command. Use /help")
            }
        } catch (e: Exception) {
            sendMessage(chatId, "Error: ${e.message}")
        }
    }

    private suspend fun cmdHelp(chatId: Long) {
        sendMessage(chatId, """
            XhRec Bot Commands:
            /add &lt;name&gt; [quality] [active] [autopay] — Add room
            /remove &lt;name|id&gt; — Remove room
            /list — List all rooms
            /status [name] — Room status
            /activate &lt;name|id&gt; — Arm for auto-recording
            /deactivate &lt;name|id&gt; — Disarm
            /quality &lt;name|id&gt; &lt;q&gt; — Set quality
            /help — This message
        """.trimIndent())
    }

    private suspend fun cmdList(chatId: Long) {
        val rooms = requestBus.request<List<Room>>(GetRooms)
        if (rooms.isEmpty()) {
            sendMessage(chatId, "No rooms configured.")
            return
        }
        val armedIds = requestBus.request<List<Long>>(GetArmedRoomIds).toSet()
        val sessions = requestBus.request<List<RoomSession>>(GetSessions)
        val text = rooms.joinToString("\n") { r ->
            val armed = if (r.id in armedIds) "🔫" else "💤"
            val session = sessions.find { it.roomId == r.id }
            val rec = when (session?.state) {
                SessionState.Recording -> "🔴"
                SessionState.Fetching -> "🟡"
                else -> ""
            }
            val ap = if (r.autoPay) " 💰" else ""
            "$armed$rec ${r.name} (${r.id}) [${r.quality}]${ap} ${r.status ?: "unknown"}"
        }
        sendMessage(chatId, "Rooms ($rooms):\n$text")
    }

    private suspend fun cmdStatus(chatId: Long, args: List<String>) {
        val rooms = requestBus.request<List<Room>>(GetRooms)
        val target = if (args.isEmpty()) rooms else {
            val query = args.joinToString(" ")
            rooms.filter { it.name.equals(query, true) || it.id.toString() == query }
        }
        if (target.isEmpty()) {
            sendMessage(chatId, "Room not found.")
            return
        }
        val statusMap = requestBus.request<Map<Long, Map<String, Any>>>(GetRoomDetailedStatus)
        val sessions = requestBus.request<List<RoomSession>>(GetSessions)
        val text = target.joinToString("\n---\n") { r ->
            val s = sessions.find { it.roomId == r.id }
            val st = statusMap[r.id]
            val running = st?.get("running")?.let {
                (it as? Map<*, *>)?.size ?: 0
            } ?: 0
            val success = (st?.get("success") as? Number)?.toLong() ?: 0
            val failed = (st?.get("failed") as? Number)?.toLong() ?: 0
            val bytes = (st?.get("bytesWrite") as? Number)?.toLong() ?: 0
            val state = s?.state?.name ?: "Idle"
            buildString {
                appendLine("${r.name} (#${r.id})")
                appendLine("Quality: ${r.quality} | Status: ${r.status ?: "?"}")
                appendLine("State: $state")
                if (running > 0) appendLine("Downloading: $running segments")
                appendLine("Segments: $success OK / $failed failed")
                appendLine("Bytes: ${fmtBytes(bytes)}")
                if (r.timeLimit != Duration.INFINITE) appendLine("Time limit: ${r.timeLimit}")
                if (r.autoPay) appendLine("Autopay: ✅")
            }
        }
        sendMessage(chatId, text)
    }

    private suspend fun cmdAdd(chatId: Long, args: List<String>) {
        if (args.isEmpty()) {
            sendMessage(chatId, "Usage: /add <name> [quality] [active] [autopay]")
            return
        }
        val name = args[0]
        val quality = args.getOrElse(1) { "720p" }
        val active = args.getOrElse(2) { "" }.lowercase() in listOf("true", "active", "yes", "1")
        val autopay = args.getOrElse(3) { "" }.lowercase() in listOf("autopay", "true", "yes", "1")
        val resp = requestBus.request<RoomNameResponse>(AddRoom(name, quality, autoPay = autopay))
        if (active) {
            val rooms = requestBus.request<List<Room>>(GetRooms)
            val added = rooms.find { it.name == resp.name }
            if (added != null) requestBus.request<OkResponse>(ActivateRecordingCmd(added.id))
        }
        val flags = buildList {
            if (active) add("active")
            if (autopay) add("autopay")
        }
        sendMessage(chatId, "✅ Room added: ${resp.name}${if (flags.isNotEmpty()) " (${flags.joinToString(", ")})" else ""}")
    }

    private suspend fun cmdRemove(chatId: Long, args: List<String>) {
        val id = resolveRoomId(args) ?: run {
            sendMessage(chatId, "Usage: /remove <name|id>"); return
        }
        requestBus.request<OkResponse>(RemoveRoom(id))
        sendMessage(chatId, "✅ Room removed: $id")
    }

    private suspend fun cmdActivate(chatId: Long, args: List<String>) {
        val id = resolveRoomId(args) ?: run {
            sendMessage(chatId, "Usage: /activate <name|id>"); return
        }
        requestBus.request<OkResponse>(ActivateRecordingCmd(id))
        sendMessage(chatId, "✅ Room activated (armed): $id")
    }

    private suspend fun cmdDeactivate(chatId: Long, args: List<String>) {
        val id = resolveRoomId(args) ?: run {
            sendMessage(chatId, "Usage: /deactivate <name|id>"); return
        }
        requestBus.request<OkResponse>(DeactivateCmd(id))
        sendMessage(chatId, "✅ Room deactivated: $id")
    }

    private suspend fun cmdQuality(chatId: Long, args: List<String>) {
        if (args.size < 2) {
            sendMessage(chatId, "Usage: /quality <name|id> <quality>"); return
        }
        val id = resolveRoomId(listOf(args[0])) ?: run {
            sendMessage(chatId, "Room not found: ${args[0]}"); return
        }
        val q = args.drop(1).joinToString(" ")
        requestBus.request<OkResponse>(SetRoomQuality(id, q))
        sendMessage(chatId, "✅ Quality set to $q for room $id")
    }

    private suspend fun resolveRoomId(args: List<String>): Long? {
        if (args.isEmpty()) return null
        val query = args[0]
        query.toLongOrNull()?.let { return it }
        val rooms = requestBus.request<List<Room>>(GetRooms)
        return rooms.firstOrNull { it.name.equals(query, true) }?.id
    }

    private suspend fun sendMessage(chatId: Long, text: String) {
        try {
            httpClient.post("$apiUrl/sendMessage") {
                setBody(buildJsonObject {
                    put("chat_id", chatId)
                    put("text", text)
                    put("parse_mode", "HTML")
                })
                contentType(ContentType.Application.Json)
            }
        } catch (e: Exception) {
            logger.error("Failed to send Telegram message: ${e.message}")
        }
    }

    suspend fun sendFileToChannel(file: File, caption: String) {
        val chatId = channelId.toLongOrNull() ?: return
        try {
            if (file.length() > 45_000_000) {
                sendMessage(chatId, "✂️ Splitting ${file.name} (${fmtBytes(file.length())}) into parts...")
                val parts = splitVideo(file)
                if (parts.isEmpty()) {
                    sendMessage(chatId, "⚠️ Split failed, trying URL fallback for ${file.name}...")
                    sendViaUrl(chatId, file, caption)
                    return
                }
                sendMessage(chatId, "⬆️ Uploading ${parts.size} parts...")
                var ok = 0; var fail = 0
                coroutineScope {
                    parts.mapIndexed { i, part ->
                        async {
                            uploadSemaphore.acquire()
                            try {
                                val partCaption = "$caption\nPart ${i + 1}/${parts.size}"
                                uploadVideoDirectly(chatId, part, partCaption)
                                ok++
                            } catch (e: Exception) {
                                fail++
                                logger.error("Part ${i+1} failed: ${e.message}")
                                sendMessage(chatId, "⚠️ Part ${i+1} failed (${e.message})")
                            } finally {
                                uploadSemaphore.release()
                                part.delete()
                            }
                        }
                    }.awaitAll()
                }
                val msg = buildString {
                    append("✅ ${ok}/${parts.size} parts uploaded")
                    if (fail > 0) append(" ($fail failed)")
                    append(" for ${file.name}")
                }
                sendMessage(chatId, msg)
            } else {
                uploadVideoDirectly(chatId, file, caption)
            }
            logger.info("Processed {} for Telegram channel {}", file.name, channelId)
        } catch (e: Exception) {
            logger.error("Failed to upload file to Telegram: ${e.message}")
        }
    }

    private suspend fun sendViaUrl(chatId: Long, file: File, caption: String) {
        if (publicUrl.isBlank()) return
        fixMeta(file)
        val fileUrl = "${publicUrl.trimEnd('/')}/tmpfiles/${file.name}"
        try {
            val resp = httpClient.post("$apiUrl/sendVideo") {
                setBody(buildJsonObject {
                    put("chat_id", chatId)
                    put("video", fileUrl)
                    put("caption", caption)
                })
                contentType(ContentType.Application.Json)
            }
            if (!resp.status.isSuccess()) {
                logger.error("URL send failed: ${resp.status} ${resp.bodyAsText()}")
            }
        } catch (e: Exception) {
            logger.error("URL send error: ${e.message}")
        }
    }

    private fun fixMeta(file: File) {
        try {
            val tmp = File(file.parentFile, ".${file.name}.meta")
            val pb = ProcessBuilder(
                "ffmpeg", "-y", "-i", file.absolutePath,
                "-map", "0", "-c", "copy",
                "-movflags", "+faststart",
                tmp.absolutePath
            )
            pb.redirectErrorStream(true)
            if (pb.start().waitFor(60, TimeUnit.SECONDS) && tmp.exists() && tmp.length() > 0) {
                tmp.renameTo(file)
            }
            tmp.delete()
        } catch (_: Exception) { }
    }

    private suspend fun uploadVideoDirectly(chatId: Long, file: File, caption: String) {
        val resp = httpClient.submitFormWithBinaryData(
            url = "$apiUrl/sendVideo",
            formData = formData {
                append("chat_id", chatId)
                append("caption", caption)
                append("video", file.readBytes(), Headers.build {
                    append(HttpHeaders.ContentType, "video/mp4")
                    append(HttpHeaders.ContentDisposition, "filename=\"${file.name}\"")
                })
            }
        )
        if (!resp.status.isSuccess()) {
            val body = resp.bodyAsText()
            logger.error("Telegram sendVideo returned ${resp.status}: $body")
        }
    }

    private fun splitVideo(input: File, targetSize: Long = 35_000_000): List<File> {
        try {
            if (input.length() <= targetSize) return listOf(input)

            val prefix = File(input.parentFile, "${input.nameWithoutExtension}_part_")
            val pattern = "${prefix.absolutePath}%03d.mp4"

            val pb = ProcessBuilder(
                "ffmpeg", "-y", "-i", input.absolutePath,
                "-map", "0",
                "-c", "copy", "-f", "segment",
                "-segment_size", "${targetSize / 1_000_000}M",
                "-reset_timestamps", "1",
                pattern
            )
            pb.redirectErrorStream(true)
            if (!pb.start().waitFor(300, TimeUnit.SECONDS)) {
                logger.warn("Split ffmpeg timed out")
                return listOf(input)
            }

            val parts = input.parentFile.listFiles()
                ?.filter { it.name.startsWith("${input.nameWithoutExtension}_part_") && it.name.endsWith(".mp4") }
                ?.sortedBy { it.name }
                ?: emptyList()

            if (parts.isEmpty()) return emptyList()

            for (part in parts) {
                val tmp = File(part.parentFile, ".${part.name}.fix")
                val fixPb = ProcessBuilder(
                    "ffmpeg", "-y", "-i", part.absolutePath,
                    "-map", "0", "-c", "copy",
                    "-movflags", "+faststart",
                    tmp.absolutePath
                )
                fixPb.redirectErrorStream(true)
                if (fixPb.start().waitFor(60, TimeUnit.SECONDS) && tmp.exists() && tmp.length() > 0) {
                    tmp.renameTo(part)
                }
                tmp.delete()
            }

            return parts.filter { it.exists() && it.length() > 0 }
        } catch (e: Exception) {
            logger.error("Split failed: ${e.message}")
            return emptyList()
        }
    }

    private fun fmtBytes(n: Long): String = when {
        n >= 1_000_000_000 -> "%.1f GB".format(n / 1_000_000_000.0)
        n >= 1_000_000 -> "%.1f MB".format(n / 1_000_000.0)
        n >= 1_000 -> "%.1f KB".format(n / 1_000.0)
        else -> "$n B"
    }

    override suspend fun handle(msg: TelegramMsg) {
        when (msg) {
            is TelegramSendMsg -> sendMessage(msg.chatId, msg.text)
            is TelegramSendFile -> sendFileToChannel(msg.file, msg.caption)
            is TelegramPollTick -> {}
            is TelegramProcessedFile -> {
                val file = msg.event.file
                if (!file.exists() || file.length() == 0L) return
                val roomName = try {
                    requestBus.request<RoomNameResponse>(GetRoomName(msg.event.roomId)).name
                } catch (_: Exception) { "unknown" }
                val caption = buildString {
                    appendLine("🎬 $roomName (#${msg.event.roomId})")
                    appendLine("📁 ${file.name}")
                }
                sendFileToChannel(file, caption.trimEnd())
            }
        }
    }
}
