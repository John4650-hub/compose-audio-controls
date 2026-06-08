package br.com.frazo.audio_services.recorder
import android.annotation.SuppressLint
import android.media.*
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import java.io.File
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.BlockingQueue
import com.theeasiestway.opus.Constants
import com.theeasiestway.opus.Opus

class AndroidAudioRecorder(
    private val dispatcher: CoroutineDispatcher
) : AudioRecorder {

    private val UPDATE_DATA_INTERVAL_MILLIS = 50L

    private var recorder: AudioRecord? = null
    private var shortBuffer:ShortArray?=null
    private var recordingThread: Thread? = null
    private var consumerThread: Thread? = null
    private var audioRecordingDataFlowID: String? = null
    private val _audioRecordingData =
        MutableStateFlow<AudioRecordingData>(AudioRecordingData.NotStarted)
    private val audioRecordingData = _audioRecordingData.asStateFlow()

    private var isPaused = false

    // Opus codec instance
    private val codec = Opus()
    private val DENOISE=true

    private val frameQueue: BlockingQueue<ShortArray> = LinkedBlockingQueue()
    private val POISON_PILL = ShortArray(0)
    
    override fun startRecording(outputFile: File): Flow<AudioRecordingData> {
        stopRecording()
        if(DENOISE){codec.denoiserInit()}
        val SAMPLE_RATE = Constants.SampleRate._48000().v
        val FRAME_SIZE=Constants.FrameSize._480().v
        val CHANNELS = Constants.Channels.mono()
        val APPLICATION = Constants.Application.audio()
        val channelConfig = AudioFormat.CHANNEL_IN_MONO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        var bufferSize = AudioRecord.getMinBufferSize(SAMPLE_RATE, channelConfig, audioFormat)//in bytes
            // opusenc
        codec.oggEncoderInit(outputFile.absolutePath,SAMPLE_RATE,CHANNELS.v,0)

        @SuppressLint("MissingPermission")
        recorder = AudioRecord(
            MediaRecorder.AudioSource.VOICE_RECOGNITION,
            SAMPLE_RATE,
            channelConfig,
            audioFormat,
            bufferSize
        )

        recorder?.startRecording()

        // Background thread: capture PCM → encode → write encoded packets
        val carryBuffer = ShortArray(FRAME_SIZE)
        var carryIndex = 0

// Producer: capture audio
recordingThread = Thread {
    val shortBuffer = ShortArray(bufferSize / 2)
    while (recorder?.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
        val read = recorder?.read(shortBuffer, 0, shortBuffer.size) ?: 0
        if (read > 0) {
            var offset = 0
            while (offset < read) {
                val needed = FRAME_SIZE - carryIndex
                val toCopy = minOf(needed, read - offset)

                // Copy into carryBuffer
                System.arraycopy(shortBuffer, offset, carryBuffer, carryIndex, toCopy)
                carryIndex += toCopy
                offset += toCopy

                if (carryIndex == FRAME_SIZE) {
                    // enqueue one full frame
                    frameQueue.put(carryBuffer.copyOf())
                    carryIndex = 0
                }
            }
        }
    }

    //After loop ends: pad remaining samples with silence , don't discard
    if (carryIndex > 0) {
        for (i in carryIndex until FRAME_SIZE) {
            carryBuffer[i] = 0 // silence padding
        }
        frameQueue.put(carryBuffer.copyOf())
        carryIndex = 0
    }
}.apply { start() }

    // Consumer: denoise + encode
    consumerThread=Thread {
    while (true) {
        val frame = frameQueue.take()
        if (frame.isEmpty()){
          break
        }
        codec.writeChunk(frame, CHANNELS.v, FRAME_SIZE,DENOISE)
  }
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
        frameQueue.put(POISON_PILL)
        recorder = null
        recordingThread?.interrupt()
        consumerThread?.interrupt()
        recordingThread = null
        consumerThread=null

        // Release Opus resources
        codec.closeOggEncoder()
        //Release rnnoise
        if(DENOISE){codec.denoiserDestroy()}

        _audioRecordingData.value = AudioRecordingData.NotStarted
    }

    override fun pause() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Recording) {
            isPaused = true
            _audioRecordingData.value = AudioRecordingData.Paused(
                currentData.elapsedTime,
                shortBuffer?.let{codec.getAmplitude(codec.convert(it)!!
            )
          }?:0
        )
        }
    }

    override fun resume() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Paused) {
            isPaused = false
            _audioRecordingData.value = AudioRecordingData.Recording(
                currentData.elapsedTime,
                shortBuffer?.let{codec.getAmplitude(codec.convert(it)!!)
          }?:0
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
                            shortBuffer?.let{codec.getAmplitude(codec.convert(it)!!
                        )
                }?:0
              )
            )
            }
                delay(UPDATE_DATA_INTERVAL_MILLIS)
            }
        }
    }
}

