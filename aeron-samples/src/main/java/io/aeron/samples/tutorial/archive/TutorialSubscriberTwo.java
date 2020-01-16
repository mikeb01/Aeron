package io.aeron.samples.tutorial.archive;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.Agent;

public class TutorialSubscriberTwo
    implements Agent, AvailableImageHandler, UnavailableImageHandler, RecordingDescriptorConsumer
{
    public static void main(final String[] args)
    {
        // tag::connect[]
        try (MediaDriver mediaDriver = MediaDriver.launchEmbedded())                  // <1>
        {
            final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
            final AeronArchive.Context aArchiveCtx = new AeronArchive.Context();

            try (Aeron aeron = Aeron.connect(aeronCtx);                               // <2>
                AeronArchive aeronArchive = AeronArchive.connect(aArchiveCtx.aeron(aeron).ownsAeronClient(true)))
            {
                final DescriptorHandler descriptorHandler = new DescriptorHandler(aeronArchive);
                aeronArchive.listRecordings(0, Integer.MAX_VALUE, descriptorHandler); // <3>
            }
        }
        // end::connect[]
    }

    @Override
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

    }

    @Override
    public int doWork() throws Exception
    {
        return 0;
    }

    @Override
    public String roleName()
    {
        return "Replay Messages";
    }

    @Override
    public void onAvailableImage(final Image image)
    {

    }

    @Override
    public void onUnavailableImage(final Image image)
    {

    }

    private static class DescriptorHandler implements RecordingDescriptorConsumer
    {
        private final AeronArchive aeronArchive;

        DescriptorHandler(final AeronArchive aeronArchive)
        {
            this.aeronArchive = aeronArchive;
        }

        @Override
        // tag::handler[]
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
            System.out.printf(
                "Channel: %s, Stream: %d, start: %d, stop: %d%n",
                strippedChannel, streamId, startPosition, stopPosition);

            if (-1 == stopPosition)                                                // <1>
            {
                System.out.println("Recording is still open, skipping for now...");
                return;
            }

            final MutableLong lastPosition = new MutableLong(0);
            final Subscription replay = aeronArchive.replay(
                recordingId, startPosition, stopPosition - startPosition,          // <2>
                "aeron:udp?endpoint=localhost:5555", 10001);                       // <3>

            do
            {
                replay.poll((buffer, offset, length, header) ->                    // <4>
                {
                    final String message = buffer.getStringAscii(offset);
                    System.out.println(message + ", header position: " + header.position());
                    lastPosition.value = header.position();                        // <5>
                }, 100);
            }
            while (lastPosition.value < stopPosition);
        }
        // end::handler[]
    }
}
