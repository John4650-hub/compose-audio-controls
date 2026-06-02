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

        val SAMPLE_RATE = Constants.SampleRate._48000() // Assuming 48kHz as per Code 1's context
        val CHANNELS = Constants.Channels.mono() // Focusing on mono for simplicity based on original setup
        val APPLICATION = Constants.Application.audio() // Ensure application type is set correctly

        // --- Opus Configuration based on constants ---
        val FRAME_SIZE = Constants.FrameSize._120() // Example frame size (e.g., 120 samples)

        // Init encoder/decoder
        codec.encoderInit(SAMPLE_RATE, CHANNELS, APPLICATION)
        codec.decoderInit(SAMPLE_RATE, CHANNELS)

        // Optional encoder setup
        val COMPLEXITY = Constants.Complexity.instance(10)
        val BITRATE = Constants.Bitrate.max()
        codec.encoderSetComplexity(COMPLEXITY)
        codec.encoderSetBitrate(BITRATE)

        val channelConfig = AudioFormat.CHANNEL_IN_MONO
        val audioFormat = AudioFormat.ENCODING_PCM_16BIT
        // Determine buffer size based on the required settings
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

        // Ensure the file stream is flushed/opened correctly before thread starts reading
        fos.flush()


        // Background thread: capture PCM → encode → write encoded packets
        recordingThread = Thread {
            // Use a fixed buffer size for reliable reading operations
            val pcmBuffer = ByteArray(bufferSize)
            var totalFramesEncoded = 0

            while (recorder?.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                if (!isPaused) {
                    // Read data from the hardware
                    val readSize = recorder?.read(pcmBuffer, 0, pcmBuffer.size) ?: 0

                    if (readSize > 0) {
                        // Encode PCM into Opus
                        // We must ensure the input buffer size matches what the codec expects for encoding
                        val encoded = codec.encode(pcmBuffer, FRAME_SIZE)

                        if (encoded != null) {
                            try {
                                fos.write(encoded)
                                totalFramesEncoded++
                            } catch (e: java.io.IOException) {
                                // Handle write failure gracefully if necessary, though usually fatal in this context
                                break
                            }
                        }
                    }
                } else {
                    // If paused, we still need to wait for the stop signal or resume command
                    delay(100)
                }
            }

            // Final flush and state update upon stopping
            try {
                fos.flush()
            } catch (e: Exception) { /* ignore */ }

        }.apply { name = "OpusRecordingThread" } // Naming thread for debugging
        recordingThread?.start()


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
        // 1. Stop recording hardware access immediately
        recorder?.stop()

        // 2. Interrupt the background thread to stop reading/writing
        recordingThread?.interrupt()

        // 3. Release resources
        recorder?.release()
        recorder = null

        // 4. Release Opus resources
        codec.encoderRelease()
        codec.decoderRelease()

        audioRecordingDataFlowID = null
        recordingThread = null

        _audioRecordingData.value = AudioRecordingData.NotStarted
    }

    override fun pause() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Recording) {
            isPaused = true
            // Update state to Paused, keeping elapsed time the same
            _audioRecordingData.value = AudioRecordingData.Paused(
                currentData.elapsedTime,
                0 // Reset tracking for paused state
            )
        }
    }

    override fun resume() {
        val currentData = _audioRecordingData.value
        if (currentData is AudioRecordingData.Paused) {
            isPaused = false
            // Resume recording from the exact point it was paused
            _audioRecordingData.value = AudioRecordingData.Recording(
                currentData.elapsedTime,
                0 // Reset tracking for resumption
            )
        }
    }

    private fun startFlowingAudioRecordingData(flowId: String): Flow<AudioRecordingData> {
        return flow {
            while (true) {
                // Check if the flow ID is still valid and recording is active
                if (flowId != audioRecordingDataFlowID || _audioRecordingData.value !is AudioRecordingData.Recording) break

                val currentData = _audioRecordingData.value
                if (currentData is AudioRecordingData.Recording) {
                    // Emit updated time, only updating the timestamp in this flow
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
