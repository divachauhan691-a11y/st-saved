package github.rikacelery.v3.components

import github.rikacelery.v3.core.Actor
import github.rikacelery.v3.core.DataChannel
import github.rikacelery.v3.core.EventBus
import github.rikacelery.v3.data.StreamData
import github.rikacelery.v3.data.StreamEnd
import github.rikacelery.v3.data.StreamEvent
import github.rikacelery.v3.data.StreamStart
import github.rikacelery.v3.events.EndReason
import github.rikacelery.v3.events.FileReady
import github.rikacelery.v3.events.WriterFatal
import github.rikacelery.v3.hooks.WriterHook
import kotlinx.coroutines.*
import java.io.File
import java.io.FileOutputStream
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

sealed interface WriterMsg

class ActiveFile(
    var file: File,
    var eventFile: File,
    var fos: FileOutputStream,
    var eventFos: FileOutputStream,
    val roomId: Long,
    val roomName: String,
    val startTime: Instant,
    var bytesWritten: Long = 0,
    var segmentIndex: Int = 0,
    var initBytes: ByteArray? = null
) {
    fun dispose() {
        try { fos.close() } catch (_: Exception) { }
        try { eventFos.close() } catch (_: Exception) { }
        file.delete()
        eventFile.delete()
    }
}

class WriterComponent(
    private val dataChannel: DataChannel,
    private val tmpDir: File,
    private val hooks: List<WriterHook> = emptyList(),
    private val segmentSize: Long = 35_000_000,
    eventBus: EventBus,
    parentScope: CoroutineScope
) : Actor<WriterMsg>("WriterComponent", eventBus, parentScope) {

    private val files = ConcurrentHashMap<Long, ActiveFile>()
    private val timeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HHmmss")
        .withZone(ZoneId.systemDefault())

    override suspend fun onStart(scope: CoroutineScope) {
        scope.launch {
            while (isActive) {
                when (val msg = dataChannel.receive()) {
                    is StreamStart -> handleStreamStart(msg)
                    is StreamData -> handleStreamData(msg)
                    is StreamEnd -> handleStreamEnd(msg)
                    is StreamEvent -> handleStreamEvent(msg)
                }
            }
        }
    }

    override suspend fun handle(msg: WriterMsg) {}

    private suspend fun handleStreamStart(msg: StreamStart) {
        val path = partPath(msg.roomName, msg.startTime, 0)
        try {
            val file = File(path)
            file.parentFile?.mkdirs()
            val eventFile = File("$path.event")
            withContext(Dispatchers.IO) {
                files[msg.roomId] = ActiveFile(
                    file = file, eventFile = eventFile,
                    fos = FileOutputStream(file), eventFos = FileOutputStream(eventFile),
                    roomId = msg.roomId, roomName = msg.roomName, startTime = msg.startTime
                )
            }
            logger.info("Opened file: $path")
        } catch (e: Exception) {
            logger.error("Failed to open file for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private suspend fun handleStreamData(msg: StreamData) {
        val active = files[msg.roomId] ?: return
        try {
            var data = msg.data
            hooks.forEach { data = it.beforeWrite(msg.roomId, data) }

            if (active.initBytes == null) {
                active.initBytes = data.copyOf()
            }

            if (active.bytesWritten >= segmentSize && active.initBytes != null) {
                active.fos.close()
                active.eventFos.close()

                val elapsed = Duration.between(active.startTime, Instant.now()).toMillis()
                active.segmentIndex++
                val segIdx = active.segmentIndex
                val segName = segFileName(active.roomName, active.startTime, segIdx, elapsed)
                val segFile = File(tmpDir, segName)
                active.file.renameTo(segFile)

                val segEventFile = File(tmpDir, "$segName.event")
                active.eventFile.renameTo(segEventFile)
                if (segEventFile.length() == 0L) segEventFile.delete()

                val newPath = partPath(active.roomName, active.startTime, segIdx)
                val newFile = File(newPath)
                val newEventFile = File("$newPath.event")
                val newFos = FileOutputStream(newFile)
                val newEventFos = FileOutputStream(newEventFile)
                newFos.write(active.initBytes!!)
                active.file = newFile
                active.eventFile = newEventFile
                active.fos = newFos
                active.eventFos = newEventFos
                active.bytesWritten = active.initBytes!!.size.toLong()
                logger.info("Segment {} closed ({}MB), opening {}...", segIdx, active.bytesWritten / 1_000_000, newPath)

                // remux closed segment asynchronously to normalize timestamps
                val roomId = active.roomId
                scope.launch {
                    val remuxed = File(tmpDir, ".$segName.remux.mp4")
                    try {
                        val pb = ProcessBuilder(
                            "ffmpeg", "-y", "-i", segFile.absolutePath,
                            "-c", "copy", "-movflags", "+faststart",
                            remuxed.absolutePath
                        )
                        pb.redirectErrorStream(true)
                        if (pb.start().waitFor(60, TimeUnit.SECONDS) && remuxed.exists() && remuxed.length() > 0) {
                            remuxed.renameTo(segFile)
                        }
                    } catch (_: Exception) { }
                    remuxed.delete()
                    eventBus.publish(FileReady(roomId, segFile, EndReason.StreamEnd))
                    logger.info("Segment {} remuxed + published: {} ({}MB)", segIdx, segName, segFile.length() / 1_000_000)
                }
            }

            withContext(Dispatchers.IO) {
                active.fos.write(data)
            }
            active.bytesWritten += data.size
        } catch (e: Exception) {
            logger.error("Failed to write data for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private suspend fun handleStreamEnd(msg: StreamEnd) {
        val active = files.remove(msg.roomId) ?: return
        // quick close — never block here
        val segIdx = active.segmentIndex
        val roomId = active.roomId
        val roomName = active.roomName
        val startTime = active.startTime
        try {
            if (active.bytesWritten < 1024) {
                active.dispose()
                return
            }
            active.fos.close()
            active.eventFos.close()
        } catch (e: Exception) {
            active.dispose()
            return
        }
        val elapsed = Duration.between(startTime, Instant.now()).toMillis()
        val finalName = segFileName(roomName, startTime, segIdx, elapsed)
        val finalFile = File(tmpDir, finalName)
        active.file.renameTo(finalFile)
        val finalEvent = File(tmpDir, "$finalName.event")
        active.eventFile.renameTo(finalEvent)
        if (finalEvent.length() == 0L) finalEvent.delete()
        if (finalFile.length() == 0L) { finalFile.delete(); return }
        hooks.forEach { it.afterFileClosed(roomId, finalFile) }

        // remux + publish async
        scope.launch {
            val remuxed = File(tmpDir, ".$finalName.remux.mp4")
            try {
                val pb = ProcessBuilder(
                    "ffmpeg", "-y", "-i", finalFile.absolutePath,
                    "-c", "copy", "-movflags", "+faststart",
                    remuxed.absolutePath
                )
                pb.redirectErrorStream(true)
                if (pb.start().waitFor(60, TimeUnit.SECONDS) && remuxed.exists() && remuxed.length() > 0) {
                    remuxed.renameTo(finalFile)
                }
            } catch (_: Exception) { }
            remuxed.delete()
            eventBus.publish(FileReady(roomId, finalFile, msg.reason))
            logger.info("Closed + remuxed: ${finalFile.absolutePath}, reason=${msg.reason}")
        }
    }

    private suspend fun handleStreamEvent(msg: StreamEvent) {
        val active = files[msg.roomId] ?: return
        try {
            withContext(Dispatchers.IO) {
                active.eventFos.write((msg.eventJson + "\n").toByteArray())
            }
        } catch (e: Exception) {
            logger.error("Failed to write event for room ${msg.roomId}: ${e.message}", e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            files.remove(msg.roomId)?.dispose()
        }
    }

    private fun partPath(roomName: String, startTime: Instant, segIdx: Int): String {
        val ts = timeFormatter.format(startTime)
        return "${tmpDir.absolutePath}/${roomName}-$ts-seg${"%03d".format(segIdx)}.part"
    }

    private fun segFileName(roomName: String, startTime: Instant, segIdx: Int, durationMs: Long): String {
        val ts = timeFormatter.format(startTime)
        val dur = formatDurationHM(durationMs)
        return "${roomName}-$ts-seg${"%03d".format(segIdx)}-$dur.mp4"
    }

    private fun formatDurationHM(ms: Long): String {
        val h = ms / 3600_000
        val m = (ms % 3600_000) / 60_000
        val s = (ms % 60_000) / 1000
        return if (h > 0) "${h}h${m}m${s}s" else if (m > 0) "${m}m${s}s" else "${s}s"
    }
}
