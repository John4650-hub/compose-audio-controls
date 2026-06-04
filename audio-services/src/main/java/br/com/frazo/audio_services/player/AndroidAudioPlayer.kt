package br.com.frazo.audio_services.player

import android.media.AudioFormat
import android.media.AudioManager
import android.media.AudioTrack
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.File
import java.io.FileInputStream
import java.util.*
import com.theeasiestway.opus.Constants
import com.theeasiestway.opus.Opus

class AndroidAudioPlayer(
    private val dispatcher: CoroutineDispatcher = Dispatchers.Default
) : AudioPlayer {

    private val UPDATE_DATA_INTERVAL_MILLIS = 200L

    private var audioTrack: AudioTrack? = null
    private val _audioPlayingData =
        MutableStateFlow(AudioPlayingData(AudioPlayerStatus.NOT_INITIALIZED, 0, 0))
    private val audioPlayingData = _audioPlayingData.asStateFlow()
    private var flowJob: Job? = null
    private var currentUniqueId: String? = null

    // Opus codec instance for decoding
    private val codec = Opus()

    override fun start(file: File): Flow<AudioPlayingData> {
        stop()

        val SAMPLE_RATE = Constants.SampleRate._48000()
        val CHANNELS = Constants.Channels.mono()
        val channelConfig = AudioFormat.CHANNEL_OUT_MONO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        val bufferSize = AudioTrack.getMinBufferSize(SAMPLE_RATE.v, channelConfig, audioFormat)

        codec.decoderInit(SAMPLE_RATE, CHANNELS)
        audioTrack = AudioTrack(
            AudioManager.STREAM_MUSIC,
            SAMPLE_RATE.v,
            channelConfig,
            audioFormat,
            bufferSize,
            AudioTrack.MODE_STREAM
        )

        currentUniqueId = UUID.randomUUID().toString()
        audioTrack?.play()

        // Background thread: read encoded Opus packets → decode → play PCM
        val fis = FileInputStream(file)
        flowJob = CoroutineScope(dispatcher).launch {
            val encodedBuffer = ByteArray(bufferSize)
            while (isActive && audioTrack?.playState == AudioTrack.PLAYSTATE_PLAYING) {
                val read = fis.read(encodedBuffer)
                if (read <= 0) break

                val decoded = codec.decode(encodedBuffer, encodedBuffer.size)
                if (decoded != null) {
                    audioTrack?.write(decoded, 0, decoded.size)
                }
            }
            fis.close()
            stop()
        }

        _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PLAYING)

        // Flow updates for duration/elapsed
        CoroutineScope(dispatcher).launch {
            currentUniqueId?.let {
                startFlowing(it).collectLatest { _audioPlayingData.value = it }
            }
        }

        return audioPlayingData
    }

    override fun pause() {
        audioTrack?.pause()
        _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PAUSED)
    }

    override fun resume() {
        audioTrack?.play()
        _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PLAYING)
    }

    override fun stop() {
        flowJob?.cancel()
        audioTrack?.stop()
        audioTrack?.release()
        audioTrack = null

        // Release decoder
        codec.decoderRelease()

        _audioPlayingData.value = AudioPlayingData(AudioPlayerStatus.NOT_INITIALIZED, 0, 0)
    }

    override fun seek(position: Long) {
        // Seeking in Opus stream requires indexed packets or custom container.
        // Not implemented here.
    }

    private fun startFlowing(uniqueId: String): Flow<AudioPlayingData> {
        return flow {
            while (true) {
                if (audioTrack == null) return@flow
                if (uniqueId != currentUniqueId) return@flow

                val newData = _audioPlayingData.value.copy(
                    duration = 0, // Opus stream duration requires container metadata
                    elapsed = audioTrack?.playbackHeadPosition?.toLong() ?: 0
                )
                emit(newData)
                delay(UPDATE_DATA_INTERVAL_MILLIS)
            }
        }
    }
}

