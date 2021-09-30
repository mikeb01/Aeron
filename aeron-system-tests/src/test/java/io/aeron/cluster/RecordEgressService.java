package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;

final class RecordEgressService implements ClusteredService
{
    static final String NOTIFICATIONS_CHANNEL =
        "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|ssc=true|tag=notifications";
    static final int NOTIFICATIONS_STREAM_ID = 10001;
    static final String REPLAY_CHANNEL = "aeron:ipc";
    static final int REPLAY_STREAM_ID = 10002;
    private static final int BEGIN_MSG_TYPE = 1;
    private static final int END_MSG_TYPE = 2;
    private static final int APP_MSG_TYPE = 3;


    private AeronArchive archive;
    private ConcurrentPublication notificationsPub;
    private long logPosition;
    private final MutableDirectBuffer notificationMessage = new UnsafeBuffer(new byte[1024]);
    private Cluster cluster;

    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        archive = AeronArchive.connect(cluster.context().archiveContext().clone());
        notificationsPub = cluster.aeron().addPublication(NOTIFICATIONS_CHANNEL, NOTIFICATIONS_STREAM_ID);
        logPosition = setupRecording(archive);
    }

    private long setupRecording(final AeronArchive archive)
    {
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector();
        if (0 == archive.listRecordingsForUri(0, 1, NOTIFICATIONS_CHANNEL, NOTIFICATIONS_STREAM_ID, collector))
        {
            archive.startRecording(NOTIFICATIONS_CHANNEL, NOTIFICATIONS_STREAM_ID, SourceLocation.LOCAL);
            return 0;
        }
        else
        {
            final RecordingDescriptor descriptor = collector.descriptors().get(0);
            final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(descriptor.termBufferLength);

            int activeTermId = LogBufferDescriptor.computeTermIdFromPosition(descriptor.stopPosition, positionBitsToShift, descriptor.initialTermId);

            final MutableLong lastEndPosition = new MutableLong(Aeron.NULL_VALUE);
            final MutableLong logPosition = new MutableLong(Aeron.NULL_VALUE);
            try (Subscription replaySub = archive.context().aeron().addSubscription(
                REPLAY_CHANNEL, REPLAY_STREAM_ID))
            {
                do
                {
                    final long termStartPosition = LogBufferDescriptor.computePosition(
                        activeTermId,
                        0,
                        positionBitsToShift,
                        descriptor.initialTermId);
                    final long termFinishPosition = LogBufferDescriptor.computePosition(
                        activeTermId + 1,
                        0,
                        positionBitsToShift,
                        descriptor.initialTermId);

                    final long replayPosition = Math.max(termStartPosition, descriptor.startPosition);
                    final long limitPosition = Math.min(termFinishPosition, descriptor.stopPosition);

                    if (replayPosition < descriptor.stopPosition)
                    {
                        final long replaySessionId = archive.startReplay(
                            descriptor.recordingId,
                            replayPosition,
                            limitPosition - replayPosition,
                            replaySub.channel(),
                            replaySub.streamId());
                        final int imageSessionId = (int)(replaySessionId & 0xFFFFFFFFL);

                        Image replayImage;
                        while (null == (replayImage = replaySub.imageBySessionId(imageSessionId)))
                        {
                            Tests.yield();
                        }

                        while (!replayImage.isEndOfStream())
                        {
                            replayImage.poll(
                                (buffer, offset, len, header) ->
                                {
                                    final long recordedLogPosition = buffer.getLong(0);
                                    final int messageType = buffer.getInt(8);

                                    if (END_MSG_TYPE == messageType)
                                    {
                                        lastEndPosition.set(header.position());
                                        logPosition.set(recordedLogPosition);
                                    }
                                },
                                10);
                        }
                    }

                    activeTermId--;
                }
                while (activeTermId >= descriptor.initialTermId && Aeron.NULL_VALUE == lastEndPosition.get());
            }

            if (lastEndPosition.get() < descriptor.stopPosition)
            {
                archive.truncateRecording(descriptor.recordingId, lastEndPosition.get());
                // TODO: Wait for recording signals to indicate deletion is complete.
            }
            else if (lastEndPosition.get() > descriptor.stopPosition)
            {
                throw new RuntimeException("Should not be possible");
            }

            archive.extendRecording(
                descriptor.recordingId, NOTIFICATIONS_CHANNEL, NOTIFICATIONS_STREAM_ID, SourceLocation.LOCAL);

            return logPosition.get();
        }
    }

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {

    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {

    }

    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        if (logPosition < header.position())
        {
            notificationMessage.putLong(0, header.position());
            notificationMessage.putInt(8, BEGIN_MSG_TYPE);

            cluster.idleStrategy().reset();
            while (notificationsPub.offer(notificationMessage, 0, 12) < 0)
            {
                cluster.idleStrategy().idle();
            }

            for (int i = 0; i < 5; i++)
            {
                notificationMessage.putInt(8, 0);
                final int strLength = notificationMessage.putStringAscii(12, "App message: " + i);

                cluster.idleStrategy().reset();
                while (notificationsPub.offer(notificationMessage, 0, 12 + strLength) < 0)
                {
                    cluster.idleStrategy().idle();
                }
            }

            notificationMessage.putLong(0, header.position());
            notificationMessage.putInt(8, RecordEgressTest.END_MARKER);

            cluster.idleStrategy().reset();
            while (notificationsPub.offer(notificationMessage, 0, 12) < 0)
            {
                cluster.idleStrategy().idle();
            }
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {

    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        // TODO: Write an additional begin/end pair to the publication and wait for the archive to
        // TODO: complete the write.
    }

    public void onRoleChange(final Cluster.Role newRole)
    {

    }

    public void onTerminate(final Cluster cluster)
    {

    }
}
