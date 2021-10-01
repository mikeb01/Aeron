package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ControlResponsePoller;
import io.aeron.archive.client.PrintingRecordingConsumer;
import io.aeron.archive.client.RecordingSignalAdapter;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

@ExtendWith(InterruptingTestCallback.class)
public class RecordEgressTest
{
    public static final int END_MARKER = 2;
    public static final int BEGIN_MARKER = 1;

    @Test
    @InterruptAfter(20)
    void name()
    {
        try (MediaDriver mediaDriver = MediaDriver.launch(new MediaDriver.Context().dirDeleteOnStart(true));
            Archive archive = Archive.launch(new Archive.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()).deleteArchiveOnStart(true));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(new AeronArchive.Context().aeron(aeron).ownsAeronClient(false)))
        {
            final String channel = "aeron:udp?endpoint=localhost:9999|term-length=64k";
            final int streamId = 10001;
            final String message = "This is a message that is somewhat long so that we can have at least some reasonable amount of data in the buffer when we send it";
            final String begin = "--BEGIN--";
            final String end = "--END--";
            final MutableDirectBuffer messageBuffer = new ExpandableArrayBuffer(1024);

            long recordingSessionId = Aeron.NULL_VALUE;

            try (final Publication pub = aeron.addPublication(channel, streamId);
                final Subscription sub = aeron.addSubscription(channel, streamId);)
            {
                Tests.awaitConnected(pub);
                Tests.awaitConnected(sub);

                do
                {
                    final int termId = LogBufferDescriptor.computeTermIdFromPosition(pub.position(), pub.positionBitsToShift(), pub.initialTermId());
                    if (Aeron.NULL_VALUE == recordingSessionId && 20000 < pub.position() && pub.position() < 30000)
                    {
                        recordingSessionId = aeronArchive.startRecording(channel, streamId, SourceLocation.LOCAL);
                    }

                    final int beginLength = messageBuffer.putStringAscii(0, begin);
                    while (pub.offer(messageBuffer, 0, beginLength, (termBuffer, termOffset, frameLength) -> BEGIN_MARKER) < 0)
                    {
                        Tests.yield();
                    }

                    final int messageLength = messageBuffer.putStringAscii(0, message);
                    for (int i = 0; i < 5; i++)
                    {
                        final MutableLong position = new MutableLong(0);
                        final Supplier<String> errorMsg = () -> "Failed with position=" + position;
                        while ((position.value = pub.offer(messageBuffer, 0, messageLength, (termBuffer, termOffset, frameLength) -> 0)) < 0)
                        {
                            Tests.yieldingIdle(errorMsg);
                            sub.poll((buffer, offset, len, header) -> {}, 20);
                        }
                    }

                    if (Math.abs(pub.initialTermId() - termId) > 2)
                    {
                        break;
                    }

                    final int endLength = messageBuffer.putStringAscii(0, end);
                    while (pub.offer(messageBuffer, 0, endLength, (termBuffer, termOffset, frameLength) -> END_MARKER) < 0)
                    {
                        Tests.yield();
                    }

                    sub.poll((buffer, offset, len, header) -> {}, 20);
                }
                while (true);
            }

            aeronArchive.stopRecording(recordingSessionId);

            final RecordingDescriptorCollector recordingDescriptorCollector = new RecordingDescriptorCollector();
            final int count = aeronArchive.listRecordings(0, 20, recordingDescriptorCollector.reset());
            assertThat(count, greaterThan(0));

            final RecordingDescriptor descriptor = recordingDescriptorCollector.descriptors().get(0);

            System.out.println("Start: " + descriptor.startPosition + ", Stop: " + descriptor.stopPosition);

            final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(descriptor.termBufferLength);
            int activeTermId = LogBufferDescriptor.computeTermIdFromPosition(descriptor.stopPosition, positionBitsToShift, descriptor.initialTermId);

            long position;
            final MutableLong lastEndPosition = new MutableLong(Aeron.NULL_VALUE);
            try (Subscription replaySub = aeron.addSubscription("aeron:ipc", 10002))
            {
                do
                {
                    position = LogBufferDescriptor.computePosition(
                        activeTermId,
                        0,
                        positionBitsToShift,
                        descriptor.initialTermId);
                    System.out.println("activeTerm: " + activeTermId + ", position: " + position);

                    long replayPosition = Math.max(position, descriptor.startPosition);

                    if (position < descriptor.stopPosition)
                    {
                        final long replayLength = Math.min(
                            descriptor.termBufferLength, descriptor.stopPosition - replayPosition);
                        final long replaySessionId = aeronArchive.startReplay(
                            descriptor.recordingId, replayPosition, replayLength, replaySub.channel(), replaySub.streamId());
                        final int imageSessionId = (int) (0xFFFFFF_FFFFFFFFL & replaySessionId);

                        Image image;
                        while (null == (image = replaySub.imageBySessionId(imageSessionId)))
                        {
                            Tests.yield();
                        }

                        while (!image.isEndOfStream())
                        {
                            image.poll(
                                (buffer, offset, len, header) ->
                                {
                                    System.out.println(buffer.getStringAscii(offset));
                                    if (header.reservedValue() == END_MARKER)
                                    {
                                        lastEndPosition.set(header.position());
                                    }
                                },
                                10);
                        }
                    }

                    activeTermId--;
                }
                while (descriptor.startPosition < position && position <= descriptor.stopPosition && Aeron.NULL_VALUE == lastEndPosition.get());
            }

            System.out.println("Truncate to: " + lastEndPosition);

            final String responseEndpoint = aeronArchive.controlResponsePoller().subscription().resolvedEndpoint();
            final String resolvedControlResponse = new ChannelUriStringBuilder(aeronArchive.context().controlResponseChannel())
                .endpoint(responseEndpoint)
                .build();

            try (Subscription controlSubscription = aeron.addSubscription(
                resolvedControlResponse, aeronArchive.context().controlResponseStreamId()))
            {
                final MutableBoolean recordingTruncated = new MutableBoolean(false);

                final RecordingSignalAdapter adapter = new RecordingSignalAdapter(
                    aeronArchive.controlSessionId(),
                    (controlSessionId, correlationId, relevantId, code, errorMessage) -> {},
                    (controlSessionId, correlationId, recordingId, subscriptionId, position1, signal) ->
                    {
                        System.out.println("Recording: " + recordingId + ", signal: " + signal);
                        if (recordingId == descriptor.recordingId && RecordingSignal.DELETE == signal)
                        {
                            recordingTruncated.set(true);
                        }
                    },
//                    aeronArchive.controlResponsePoller().subscription(),
                    controlSubscription,
                    10);

//                aeronArchive.extendRecording(descriptor.recordingId, channel, streamId, SourceLocation.LOCAL);

                aeronArchive.truncateRecording(0, lastEndPosition.get());

                while (!recordingTruncated.get())
                {
                    adapter.poll();
                    Tests.yield();
                }
            }
        }
    }

}
