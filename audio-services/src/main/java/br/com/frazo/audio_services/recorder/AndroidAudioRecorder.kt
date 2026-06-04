package br.com.frazo.audio_services.recorder
import android.annotation.SuppressLint
import android.media.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
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

    // Noise reduction

    override fun startRecording(outputFile: File): Flow<AudioRecordingData> {
        stopRecording()

        val SAMPLE_RATE = Constants.SampleRate._48000()
        val CHANNELS = Constants.Channels.mono()
        val APPLICATION = Constants.Application.audio()
        val DEF_FRAME_SIZE = Constants.FrameSize._480()

        // Calculate chunk size and frame size correctly

        val CHUNK_SIZE = DEF_FRAME_SIZE.v * CHANNELS.v
        val FRAME_SIZE_SHORT = Constants.FrameSize.fromValue(CHUNK_SIZE / CHANNELS.v) 
        val channelConfig = AudioFormat.CHANNEL_IN_MONO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        var bufferSize = AudioRecord.getMinBufferSize(SAMPLE_RATE.v, channelConfig, audioFormat)//in bytes

        if (bufferSize == AudioRecord.ERROR || bufferSize == AudioRecord.ERROR_BAD_VALUE) {
                bufferSize = SAMPLE_RATE.v * 2;
            }
           val fos =  FileOutputStream(outputFile)
            // Init encoder/decoder/opusenc
        codec.encoderInit(SAMPLE_RATE, CHANNELS, APPLICATION)
        codec.decoderInit(SAMPLE_RATE, CHANNELS)
        codec.oggEncoderInit(outputFile.absolutePath+"1",SAMPLE_RATE.v,CHANNELS.v,0)
        // Optional encoder setup
        codec.encoderSetComplexity(Constants.Complexity.instance(10))
        codec.encoderSetBitrate(Constants.Bitrate.max())

        @SuppressLint("MissingPermission")
        recorder = AudioRecord(
            MediaRecorder.AudioSource.VOICE_RECOGNITION,
            SAMPLE_RATE.v,
            channelConfig,
            audioFormat,
            bufferSize
        )

        recorder?.startRecording()

        // Background thread: capture PCM → encode → write encoded packets
        recordingThread = Thread {
            val shortBuffer = ShortArray(bufferSize/2)
            while (recorder?.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                if (!isPaused) {
                    val read = recorder?.read(shortBuffer, 0, shortBuffer.size) ?: 0
                    if (read > 0) {
                      fos.write(codec.convert(shortBuffer),0,read)
                      codec.writeChunk(shortBuffer,CHANNELS.v)
                        /*val encoded = codec.encode(shortBuffer, FRAME_SIZE_SHORT)
                        if (encoded != null) {
                            codec.writeChunk(encoded,CHANNELS.v)
                        }*/
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
        codec.closeOggEncoder()


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

