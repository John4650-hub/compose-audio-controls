package br.com.frazo.audio_services.recorder

import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.flow.*
import java.io.FileOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import com.theeasiestway.opus.Constants
import com.theeasiestway.opus.Opus

/**
 * Very small wrapper that records from the microphone,
 * encodes the PCM data to Opus and writes the raw Opus bytes
 * into a file.
 */
class AndroidAudioRecorder(
    private val dispatcher: CoroutineDispatcher
) : AudioRecorder {

    /** Interval used only for the UI Flow – it doesn’t touch audio. */
    private val UPDATE_INTERVAL_MS = 50L

    private var recorder: AudioRecord? = null
    private var recordThread: Thread? = null
    private var recordingJobId: String? = null

    /* ------------------------------------------------------------------ */
    /* State flow used by the UI – not relevant for the codec logic.       */
    /* ------------------------------------------------------------------ */

    private val _audioRecordingData =
        MutableStateFlow<AudioRecordingData>(AudioRecordingData.NotStarted)

    override fun startRecording(outputFile: File): Flow<AudioRecordingData> {
        stopRecording()   // make sure we’re clean

        /* ------------------------------------------------------------------ */
        /* ----- 1️⃣  Initialise Opus encoder --------------------------------*/
        /* ------------------------------------------------------------------ */

        val sampleRate = Constants.SampleRate._48000()
        val channels   = Constants.Channels.mono()          // mono only in this example
        val application = Constants.Application.audio()

        /* Encoder parameters – copy from the working demo */
        val frameSize  = Constants.FrameSize._120()      // samples / channel (120, 240 …)

        codec.encoderInit(sampleRate, channels, application)
        codec.decoderInit(sampleRate, channels)

        codec.encoderSetComplexity(Constants.Complexity.instance(10))
        codec.encoderSetBitrate(Constants.Bitrate.max())

        /* ------------------------------------------------------------------ */
        /* ----- 2️⃣  Configure Android AudioRecord --------------------------*/
        /* ------------------------------------------------------------------ */

        val channelConfig = AudioFormat.CHANNEL_IN_MONO
        val audioFormat   = AudioFormat.ENCODING_PCM_16BIT

        // smallest buffer that will not cause overruns
        val minBufSize = AudioRecord.getMinBufferSize(
            sampleRate.v,
            channelConfig,
            audioFormat
        )
        recorder = AudioRecord(
            MediaRecorder.AudioSource.MIC,
            sampleRate.v,
            channelConfig,
            audioFormat,
            minBufSize
        )

        /* ------------------------------------------------------------------ */
        /* ----- 3️⃣  Start recording and encoding loop ---------------------*/
        /* ------------------------------------------------------------------ */

        val fos = FileOutputStream(outputFile)
        recorder?.startRecording()

        recordThread = Thread {
            // We'll read exactly `frameSize` samples every iteration
            val shortBuffer = ShortArray(frameSize)
            var finished = false

            while (recorder?.recordingState == AudioRecord.RECORDSTATE_RECORDING) {
                val readSamples = recorder?.read(shortBuffer, 0, frameSize) ?: 0
                if (readSamples <= 0) continue        // nothing to do in this cycle

                /* Encode the short[] → raw Opus bytes */
                val encodedShorts = codec.encode(shortBuffer, frameSize)
                if (!encodedShorts.isNullOrEmpty()) {
                    /*
                     * The library returns a ShortArray that represents little‑endian
                     * 16 bit PCM of the encoded data. Convert it to a byte[] before
                     * writing to disk.
                     */
                    val rawOpus = shortArrayToByteArray(encodedShorts)
                    fos.write(rawOpus)
                }
            }

            /* ----- 4️⃣   Cleanup ---------------------------------------*/
            fos.close()
            finished = true
        }.apply { start() }

        /* ------------------------------------------------------------------ */
        /* ----- 5️⃣  Start Flow that keeps track of elapsed time ----------*/
        /* ------------------------------------------------------------------ */

        val flowId = java.util.UUID.randomUUID().toString()
        recordingJobId?.let { old -> _audioRecordingData.value = AudioRecordingData.NotStarted }
        recordingJobId = flowId
        _audioRecordingData.value = AudioRecordingData.Recording(0, 0)

        CoroutineScope(dispatcher).launch {
            startFlowingAudioRecordingData(flowId)
                .takeWhile { it is AudioRecordingData.Recording } // stop when finished
                .collectLatest { _audioRecordingData.value = it }
        }

        return _audioRecordingData.asStateFlow()
    }

    override fun stopRecording() {
        recordingJobId = null

        recorder?.stop()
        recorder?.release()
        recorder = null

        recordThread?.interrupt()
        recordThread = null

        codec.encoderRelease()
        codec.decoderRelease()

        _audioRecordingData.value = AudioRecordingData.NotStarted
    }

    /* ------------------------------------------------------------------ */
    /* -----  Pause / Resume – purely UI helpers ------------------------*/
    /* ------------------------------------------------------------------ */

    private var isPaused = false

    override fun pause() {
        val now = _audioRecordingData.value
        if (now is AudioRecordingData.Recording) {
            isPaused = true
            _audioRecordingData.value =
                AudioRecordingData.Paused(now.elapsedTime, 0)
        }
    }

    override fun resume() {
        val now = _audioRecordingData.value
        if (now is AudioRecordingData.Paused) {
            isPaused = false
            _audioRecordingData.value =
                AudioRecordingData.Recording(now.elapsedTime, 0)
        }
    }

    /* ------------------------------------------------------------------ */
    /* -----  Flow that emits elapsed time while recording ------------*/
    /* ------------------------------------------------------------------ */

    private fun startFlowingAudioRecordingData(flowId: String): Flow<AudioRecordingData> = flow {
        var elapsed = 0L
        while (true) {
            if (flowId != recordingJobId) break
            val cur = _audioRecordingData.value
            if (cur !is AudioRecordingData.Recording) break

            if (!isPaused) elapsed += UPDATE_INTERVAL_MS

            emit(AudioRecordingData.Recording(elapsed, 0))
            delay(UPDATE_INTERVAL_MS)
        }
    }

    /* ------------------------------------------------------------------ */
    /* -----  Helper: convert ShortArray → little‑endian ByteArray --------*/
    /* ------------------------------------------------------------------ */

    private fun shortArrayToByteArray(arr: ShortArray): ByteArray =
        ByteBuffer.allocate(arr.size * 2).order(ByteOrder.LITTLE_ENDIAN)
            .putShorts(*arr)
            .array()
}
