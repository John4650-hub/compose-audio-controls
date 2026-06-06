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
    private var resumeOffset:Long?=null
    private val _audioPlayingData =
        MutableStateFlow(AudioPlayingData(AudioPlayerStatus.NOT_INITIALIZED, 0, 0))
    private val audioPlayingData = _audioPlayingData.asStateFlow()
    private var flowJob: Job? = null
    private var currentUniqueId: String? = null

    // Opus codec instance for decoding
    private val codec = Opus()
    private var bufferSize =0
    override fun start(file: File): Flow<AudioPlayingData> {
        stop()
        //start opusFile
        codec.openFile(file.absolutePath)
        val SAMPLE_RATE = Constants.SampleRate._48000()
        val channelConfig = AudioFormat.CHANNEL_OUT_MONO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        bufferSize = AudioTrack.getMinBufferSize(SAMPLE_RATE.v, channelConfig, audioFormat)

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
        launchDecodingLoop()
        _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PLAYING)

        // Flow updates for duration/elapsed
        CoroutineScope(dispatcher).launch {
            currentUniqueId?.let {
                startFlowing(it).collectLatest { _audioPlayingData.value = it }
            }
        }

        return audioPlayingData
    }

    private fun launchDecodingLoop() {
    flowJob = CoroutineScope(dispatcher).launch {
        while (isActive && audioTrack?.playState == AudioTrack.PLAYSTATE_PLAYING) {
            val decoded = codec.decodeChunk(bufferSize)
            if (decoded != null) {
                audioTrack?.write(decoded, 0, decoded.size)
            } else {
                break
            }
        }
        if(resumeOffset==null){
            stop()
          }
    }
}

    override fun pause() {
      resumeOffset=codec.getPosition()
        audioTrack?.stop()
        _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PAUSED)
    }

   override fun resume() {
    audioTrack?.play()
    launchDecodingLoop()
    resumeOffset?.let{
      seek(it)
      resumeOffset=null
    }
    _audioPlayingData.value = _audioPlayingData.value.copy(status = AudioPlayerStatus.PLAYING)
}
 

    override fun stop() {
        flowJob?.cancel()
        audioTrack?.stop()
        audioTrack?.release()
        audioTrack = null
        resumeOffset=null

        // Release decoder
        codec.closeFile()

        _audioPlayingData.value = AudioPlayingData(AudioPlayerStatus.NOT_INITIALIZED, 0, 0)
    }

    override fun seek(position: Long) {
        // Seeking in Opus stream
        if(_audioPlayingData.value.status!=AudioPlayerStatus.NOT_INITIALIZED){
                codec.seekMs(position)
            }
    }

    private fun startFlowing(uniqueId: String): Flow<AudioPlayingData> {
        return flow {
            while (true) {
                if (audioTrack == null) return@flow
                if (uniqueId != currentUniqueId) return@flow

                val newData = _audioPlayingData.value.copy(
                    duration = codec.getDuration(), // Opus stream duration requires container metadata
                    elapsed = codec.getPosition()
                )
                emit(newData)
                delay(UPDATE_DATA_INTERVAL_MILLIS)
            }
        }
    }
}

