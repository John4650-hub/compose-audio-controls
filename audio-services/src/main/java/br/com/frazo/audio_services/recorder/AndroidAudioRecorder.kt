package br.com.frazo.audio_services.recorder
import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import java.io.File
import java.io.FileOutputStream
import java.util.UUID
import com.theeasiestway.opus.Constants
import com.theeasiestway.opus.Opus

class AndroidAudioRecorder(
    private val dispatcher: CoroutineDispatcher
) : AudioRecorder {

    private val UPDATE_DATA_INTERVAL_MILLIS = 50L

    private var recorder: AudioRecord? = null
    private var recordingThread: Thread? = null
    private var audioRecordingDataFlowID: String? = null
    private val _audioRecordingData =
        MutableStateFlow<AudioRecordingData>(AudioRecordingData.NotStarted)
    private val audioRecordingData = _audioRecordingData.asStateFlow()

    private var isPaused = false

    // Opus codec instance
    private val codec = Opus()

    override fun startRecording(outputFile: File): Flow<AudioRecordingData> {
        stopRecording()

        val SAMPLE_RATE = Constants.SampleRate._48000()
        val CHANNELS = Constants.Channels.stereo()
        val APPLICATION = Constants.Application.audio()
        val FRAME_SIZE = Constants.FrameSize._120()

        // Init encoder/decoder
        codec.encoderInit(SAMPLE_RATE, CHANNELS, APPLICATION)
        codec.decoderInit(SAMPLE_RATE, CHANNELS)

        // Optional encoder setup
        val COMPLEXITY = Constants.Complexity.instance(10)
        val BITRATE = Constants.Bitrate.max()
        codec.encoderSetComplexity(COMPLEXITY)
        codec.encoderSetBitrate(BITRATE)

        val channelConfig = AudioFormat.CHANNEL_IN_STEREO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        val bufferSize = AudioRecord.getMinBufferSize(SAMPLE_RATE.v, channelConfig, audioFormat)

        recorder = AudioRecord(
            MediaRecorder.AudioSource.MIC,
            SAMPLE_RATE.v,
            channelConfig,
            audioFormat,
            bufferSize
        )

        val fos = FileOutputStream(outputFile)
        recorder?.startRecording()

        // Background thread: capture PCM → encode → write encoded packets
        recordingThread = Thread {
            val pcmBuffer = ByteArray(bufferSize)
            while (recorder?.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                if (!isPaused) {
                    val read = recorder?.read(pcmBuffer, 0, pcmBuffer.size) ?: 0
                    if (read > 0) {
                        // Encode PCM into Opus
                        val encoded = codec.encode(pcmBuffer, FRAME_SIZE)
                        if (encoded != null) {
                            fos.write(encoded)
                        }
                    }
                }
            }
            fos.close()
        }.apply { start() }

        UUID.randomUUID().toString().also { uuid ->
            audioRecordingDataFlowID = uuid
            _audioRecordingData.value = AudioRecordingData.Recording(0, 0)
            CoroutineScope(dispatcher).launch {
                startFlowingAudioRecordingData(uuid)
                    .collectLatest { _audioRecordingData.value = it }
            }
        }

        return audioRecordingData
    }

    override fun stopRecording() {
        audioRecordingDataFlowID = null
        recorder?.stop()
        recorder?.release()
        recorder = null
        recordingThread?.interrupt()
        recordingThread = null

        // Release Opus resources
        codec.encoderRelease()
        codec.decoderRelease()

        _audioRecordingData.value = AudioRecordingData.NotStarted
    }

    override fun pause() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Recording) {
            isPaused = true
            _audioRecordingData.value = AudioRecordingData.Paused(
                currentData.elapsedTime,
                0
            )
        }
    }

    override fun resume() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Paused) {
            isPaused = false
            _audioRecordingData.value = AudioRecordingData.Recording(
                currentData.elapsedTime,
                0
            )
        }
    }

    private fun startFlowingAudioRecordingData(flowId: String): Flow<AudioRecordingData> {
        return flow {
            while (true) {
                if (flowId != audioRecordingDataFlowID) break
                val currentData = _audioRecordingData.value
                if (currentData is AudioRecordingData.NotStarted) break
                if (currentData is AudioRecordingData.Recording) {
                    emit(
                        AudioRecordingData.Recording(
                            currentData.elapsedTime + UPDATE_DATA_INTERVAL_MILLIS,
                            0
                        )
                    )
                }
                delay(UPDATE_DATA_INTERVAL_MILLIS)
            }
        }
    }
}

