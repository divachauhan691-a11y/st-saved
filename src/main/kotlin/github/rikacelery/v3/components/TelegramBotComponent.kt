package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.core.RequestBus
import github.rikacelery.v3.data.Room
import github.rikacelery.v3.events.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.okhttp.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.request.forms.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.*
import java.io.File
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
    private val httpClient = HttpClient(OkHttp) {
        install(ContentNegotiation) { json(Json { ignoreUnknownKeys = true }) }
        install(io.ktor.client.plugins.HttpTimeout) {
            requestTimeoutMillis = 300000
            socketTimeoutMillis = 300000
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
            /add &lt;name&gt; [quality] [active] — Add room
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
            "$armed$rec ${r.name} (${r.id}) [${r.quality}] ${r.status ?: "unknown"}"
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
            }
        }
        sendMessage(chatId, text)
    }

    private suspend fun cmdAdd(chatId: Long, args: List<String>) {
        if (args.isEmpty()) {
            sendMessage(chatId, "Usage: /add <name> [quality] [active]")
            return
        }
        val name = args[0]
        val quality = args.getOrElse(1) { "720p" }
        val active = args.getOrElse(2) { "" }.lowercase() in listOf("true", "active", "yes", "1")
        val resp = requestBus.request<RoomNameResponse>(AddRoom(name, quality))
        if (active) {
            val rooms = requestBus.request<List<Room>>(GetRooms)
            val added = rooms.find { it.name == resp.name }
            if (added != null) requestBus.request<OkResponse>(ActivateRecordingCmd(added.id))
        }
        sendMessage(chatId, "✅ Room added: ${resp.name}${if (active) " (active)" else ""}")
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
                if (parts.isNotEmpty()) {
                    for ((i, part) in parts.withIndex()) {
                        val partCaption = "$caption\nPart ${i + 1}/${parts.size}"
                        uploadVideo(chatId, part, partCaption)
                        part.delete()
                    }
                    sendMessage(chatId, "✅ ${parts.size} parts uploaded for ${file.name}")
                } else {
                    sendMessage(chatId, "⚠️ Failed to split ${file.name}")
                }
            } else {
                uploadVideo(chatId, file, caption)
            }
            logger.info("Processed {} for Telegram channel {}", file.name, channelId)
        } catch (e: Exception) {
            logger.error("Failed to upload file to Telegram: ${e.message}")
        }
    }

    private suspend fun uploadVideo(chatId: Long, file: File, caption: String) {
        httpClient.submitFormWithBinaryData(
            url = "$apiUrl/sendVideo",
            formData = formData {
                append("chat_id", chatId)
                append("caption", caption)
                append("video", File(file.absolutePath).readBytes(), Headers.build {
                    append(HttpHeaders.ContentType, "video/mp4")
                    append(HttpHeaders.ContentDisposition, "filename=\"${file.name}\"")
                })
            }
        )
    }

    private fun splitVideo(input: File): List<File> {
        try {
            val totalSize = input.length()
            val targetSize = 42_000_000L
            val numParts = ((totalSize + targetSize - 1) / targetSize).toInt()
            if (numParts <= 1) return listOf(input)

            val probe = ProcessBuilder(
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "csv=p=0",
                input.absolutePath
            ).also { it.redirectErrorStream(true) }.start()
            val durStr = probe.inputStream.bufferedReader().readText().trim()
            probe.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)
            val totalDur = durStr.toDoubleOrNull() ?: return listOf(input)
            val segDur = totalDur / numParts

            val prefix = File(input.parentFile, "${input.nameWithoutExtension}_part_")
            val pattern = "${prefix.absolutePath}%03d.mp4"
            val pb = ProcessBuilder(
                "ffmpeg", "-y", "-i", input.absolutePath,
                "-c", "copy", "-f", "segment",
                "-segment_time", segDur.toString(),
                "-reset_timestamps", "1",
                "-avoid_negative_ts", "make_zero",
                pattern
            )
            pb.redirectErrorStream(true)
            val proc = pb.start()
            proc.waitFor(300, java.util.concurrent.TimeUnit.SECONDS)

            val parts = (0 until numParts).mapNotNull { i ->
                val f = File(prefix.absolutePath + "%03d.mp4".format(i))
                f.takeIf { it.exists() && it.length() > 0 }
            }
            return parts
        } catch (e: Exception) {
            logger.error("Split failed: ${e.message}")
            return listOf(input)
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
