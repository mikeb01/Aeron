package io.aeron.cluster;

import io.aeron.archive.client.RecordingDescriptorConsumer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class RecordingDescriptorCollector implements RecordingDescriptorConsumer
{
    private final ArrayList<RecordingDescriptor> descriptors = new ArrayList<>();
    private final Deque<RecordingDescriptor> pool = new ArrayDeque<>();

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
        final String originalChannel,
        final String sourceIdentity)
    {
        RecordingDescriptor recordingDescriptor = pool.pollLast();
        if (null == recordingDescriptor)
        {
            recordingDescriptor = new RecordingDescriptor();
        }

        descriptors.add(recordingDescriptor.set(
            controlSessionId,
            correlationId,
            recordingId,
            startTimestamp,
            stopTimestamp,
            startPosition,
            stopPosition,
            initialTermId,
            segmentFileLength,
            termBufferLength,
            mtuLength,
            sessionId,
            streamId,
            strippedChannel,
            originalChannel,
            sourceIdentity));
    }

    public RecordingDescriptorCollector reset()
    {
        for (int i = descriptors.size(); -1 < --i;)
        {
            pool.addLast(descriptors.remove(i).reset());
        }

        return this;
    }

    public List<RecordingDescriptor> descriptors()
    {
        return descriptors;
    }
}
