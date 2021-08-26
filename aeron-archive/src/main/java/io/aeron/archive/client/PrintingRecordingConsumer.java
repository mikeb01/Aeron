package io.aeron.archive.client;

public class PrintingRecordingConsumer implements RecordingDescriptorConsumer
{
    private RecordingDescriptorConsumer delegate;

    public PrintingRecordingConsumer(final RecordingDescriptorConsumer delegate)
    {
        this.delegate = delegate;
    }

    public void onRecordingDescriptor(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long startTimestamp,
        final long stopTimestamp,
        final long startPosition,
        final long stopPosition,
        final int initialTermId,
        final int segmentFileLength,
        final int termBufferLength,
        final int mtuLength,
        final int sessionId,
        final int streamId,
        final String strippedChannel,
        final String originalChannel, final String sourceIdentity)
    {
        System.out.println("controlSessionId = " + controlSessionId + ", correlationId = " + correlationId + ", recordingId = " + recordingId + ", startTimestamp = " + startTimestamp + ", stopTimestamp = " + stopTimestamp + ", startPosition = " + startPosition + ", stopPosition = " + stopPosition + ", initialTermId = " + initialTermId + ", segmentFileLength = " + segmentFileLength + ", termBufferLength = " + termBufferLength + ", mtuLength = " + mtuLength + ", sessionId = " + sessionId + ", streamId = " + streamId + ", strippedChannel = " + strippedChannel + ", originalChannel = " + originalChannel + ", sourceIdentity = " + sourceIdentity);
        delegate.onRecordingDescriptor(controlSessionId, correlationId, recordingId, startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength, termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity);
    }
}
