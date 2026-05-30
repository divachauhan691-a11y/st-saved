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
import java.io.InputStream
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

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
    var initBytes: ByteArray? = null,
    var initBuf: ByteArray = byteArrayOf(),
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
    private val remuxSemaphore = Semaphore(2)
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
        val active = files[msg.roomId] ?: run {
            logger.warn("handleStreamData: no active file for room {}", msg.roomId)
            return
        }
        try {
            var data = msg.data
            hooks.forEach { data = it.beforeWrite(msg.roomId, data) }

            // collect complete init segment (ftyp+moov) from first data chunks
            val iBuf = active.initBuf
            if (active.initBytes == null && iBuf.size < 128 * 1024) {
                val buf = iBuf + data
                val initEnd = findInitSegmentSize(buf)
                if (initEnd > 0) {
                    active.initBytes = buf.copyOfRange(0, initEnd)
                    active.initBuf = byteArrayOf()
                    logger.info("Captured init segment: {} bytes", initEnd)
                } else if (buf.size >= 128 * 1024) {
                    active.initBytes = buf // fallback full buffer
                    active.initBuf = byteArrayOf()
                    logger.warn("Init segment fallback at {} bytes", buf.size)
                } else {
                    active.initBuf = buf
                }
            }

            // write data
            withContext(Dispatchers.IO) {
                active.fos.write(data)
                active.fos.flush()
            }

            active.bytesWritten += data.size

            // use actual file size from disk (most reliable)
            val fileLen = try { active.file.length() } catch (_: Exception) { 0L }

            // log every ~1MB or when close to split
            if (active.bytesWritten % 500_000 < data.size || fileLen >= segmentSize * 0.8) {
                logger.info("bytes: written={}, fileLen={}, dataSize={}, segIdx={}",
                    active.bytesWritten, fileLen, data.size, active.segmentIndex)
            }

            if (fileLen >= segmentSize) {
                val closeTime = Instant.now()
                val prevFile = active.file
                val prevLen = fileLen
                active.fos.close()
                active.eventFos.close()

                val elapsed = Duration.between(active.startTime, closeTime).toMillis()
                active.segmentIndex++
                val segIdx = active.segmentIndex
                val segName = segFileName(active.roomName, active.startTime, segIdx, elapsed)
                val segFile = File(tmpDir, segName)
                val renamed = active.file.renameTo(segFile)
                if (!renamed) {
                    logger.warn("renameTo failed: {} -> {}", active.file, segFile)
                }

                val segEventFile = File(tmpDir, "$segName.event")
                active.eventFile.renameTo(segEventFile)
                if (segEventFile.length() == 0L) segEventFile.delete()

                val newPath = partPath(active.roomName, active.startTime, segIdx)
                val newFile = File(newPath)
                val newEventFile = File("$newPath.event")
                val initData = active.initBytes ?: byteArrayOf()
                val newFos = FileOutputStream(newFile)
                val newEventFos = FileOutputStream(newEventFile)
                newFos.write(initData)
                active.file = newFile
                active.eventFile = newEventFile
                active.fos = newFos
                active.eventFos = newEventFos
                active.bytesWritten = initData.size.toLong()
                logger.info("SPLIT: segIdx={}, prevLen={}MB, newFile={}", segIdx, prevLen / 1_000_000, newPath)

                // remux closed segment asynchronously to normalize timestamps
                val roomId = active.roomId
                scope.launch(Dispatchers.IO + NonCancellable) {
                    remuxSemaphore.withPermit {
                        val remuxed = File(tmpDir, ".$segName.remux.mp4")
                        var remuxOk = false
                        try {
                            val pb = ProcessBuilder(
                                "ffmpeg", "-y", "-fflags", "+genpts", "-reset_timestamps", "1",
                                "-i", segFile.absolutePath,
                                "-c", "copy", "-movflags", "+faststart",
                                remuxed.absolutePath
                            )
                            pb.redirectErrorStream(true)
                            val proc = pb.start()
                            val output = proc.inputStream.bufferedReader().readText()
                            val exited = proc.waitFor(60, TimeUnit.SECONDS)
                            remuxOk = exited && remuxed.exists() && remuxed.length() > 0
                            if (remuxOk) {
                                remuxed.renameTo(segFile)
                            } else if (!exited) {
                                logger.warn("Remux timed out for {}", segName)
                            } else {
                                val errPreview = output.take(500).replace("\n", " | ")
                                logger.warn("Remux failed for {} (exit={}, output={}B, err={})", segName,
                                    proc.exitValue(), remuxed.length(), errPreview)
                            }
                        } catch (e: Exception) {
                            logger.warn("Remux exception for {}: {}", segName, e.message)
                        }
                        remuxed.delete()
                        if (remuxOk) {
                            logger.info("REPUBLISH: segIdx={}, file={} ({}MB)", segIdx, segName, segFile.length() / 1_000_000)
                        } else {
                            logger.warn("Publishing un-remuxed {} (timestamps may be wrong)", segName)
                        }
                        eventBus.publish(FileReady(roomId, segFile, EndReason.StreamEnd))
                    }
                }
            }
        } catch (e: Exception) {
            logger.error("handleStreamData error for room {}: {}", msg.roomId, e.message, e)
            eventBus.publish(WriterFatal(msg.roomId, e.message ?: "Unknown error"))
            val removed = files.remove(msg.roomId)
            if (removed != null) {
                logger.warn("Removed room {} from active files due to error", msg.roomId)
                removed.dispose()
            }
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
        scope.launch(Dispatchers.IO + NonCancellable) {
            remuxSemaphore.withPermit {
                val remuxed = File(tmpDir, ".$finalName.remux.mp4")
                var remuxOk = false
                try {
                    val pb = ProcessBuilder(
                        "ffmpeg", "-y", "-fflags", "+genpts", "-reset_timestamps", "1",
                        "-i", finalFile.absolutePath,
                        "-c", "copy", "-movflags", "+faststart",
                        remuxed.absolutePath
                    )
                    pb.redirectErrorStream(true)
                    val proc = pb.start()
                    val output = proc.inputStream.bufferedReader().readText()
                    val exited = proc.waitFor(60, TimeUnit.SECONDS)
                    remuxOk = exited && remuxed.exists() && remuxed.length() > 0
                    if (remuxOk) {
                        remuxed.renameTo(finalFile)
                    } else if (!exited) {
                        logger.warn("End remux timed out for {}", finalName)
                    } else {
                        val errPreview = output.take(500).replace("\n", " | ")
                        logger.warn("End remux failed for {} (exit={}, output={}B, err={})", finalName,
                            proc.exitValue(), remuxed.length(), errPreview)
                    }
                } catch (e: Exception) {
                    logger.warn("End remux exception for {}: {}", finalName, e.message)
                }
                remuxed.delete()
                if (remuxOk) {
                    logger.info("Closed + remuxed: ${finalFile.absolutePath}, reason=${msg.reason}")
                } else {
                    logger.warn("Publishing un-remuxed {}", finalName)
                }
                eventBus.publish(FileReady(roomId, finalFile, msg.reason))
            }
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

    private fun findInitSegmentSize(buf: ByteArray): Int {
        var offset = 0
        while (offset + 8 <= buf.size) {
            val size = ((buf[offset].toInt() and 0xFF) shl 24) or
                       ((buf[offset + 1].toInt() and 0xFF) shl 16) or
                       ((buf[offset + 2].toInt() and 0xFF) shl 8) or
                       (buf[offset + 3].toInt() and 0xFF)
            val boxEnd = if (size == 1) {
                if (offset + 16 > buf.size) return 0
                val hi = ((buf[offset + 8].toLong() and 0xFF) shl 56) or
                         ((buf[offset + 9].toLong() and 0xFF) shl 48) or
                         ((buf[offset + 10].toLong() and 0xFF) shl 40) or
                         ((buf[offset + 11].toLong() and 0xFF) shl 32) or
                         ((buf[offset + 12].toLong() and 0xFF) shl 24) or
                         ((buf[offset + 13].toLong() and 0xFF) shl 16) or
                         ((buf[offset + 14].toLong() and 0xFF) shl 8) or
                         (buf[offset + 15].toLong() and 0xFF)
                offset + hi.toInt()
            } else offset + size
            if (boxEnd > buf.size) return 0
            val type = String(buf, offset + 4, 4, Charsets.ISO_8859_1)
            if (type == "moov") return boxEnd
            offset = boxEnd
        }
        return 0
    }
}
