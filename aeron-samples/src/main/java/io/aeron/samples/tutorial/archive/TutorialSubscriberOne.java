package io.aeron.samples.tutorial.archive;

import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.driver.MediaDriver;
import org.agrona.collections.MutableLong;

public class TutorialSubscriberOne
{
    public static void main(String[] args)
    {
        // tag::connect[]
        try (MediaDriver mediaDriver = MediaDriver.launchEmbedded())                  // <1>
        {
            final Aeron.Context aeronCtx = new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName());
            final AeronArchive.Context aArchiveCtx = new AeronArchive.Context();

            try (final Aeron aeron = Aeron.connect(aeronCtx);
                 final AeronArchive aeronArchive = AeronArchive.connect(
                     aArchiveCtx.aeron(aeron).ownsAeronClient(true)))                 // <2>
            {
                final DescriptorHandler descriptorHandler = new DescriptorHandler(aeronArchive);
                aeronArchive.listRecordings(0, Integer.MAX_VALUE, descriptorHandler); // <3>
            }
        }
        // end::connect[]
    }

    private static class DescriptorHandler implements RecordingDescriptorConsumer
    {
        private final AeronArchive aeronArchive;

        DescriptorHandler(AeronArchive aeronArchive)
        {
            this.aeronArchive = aeronArchive;
        }

        @Override
        // tag::handler[]
        public void onRecordingDescriptor(
            long controlSessionId, long correlationId, long recordingId,
            long startTimestamp, long stopTimestamp, long startPosition, long stopPosition,
            int initialTermId, int segmentFileLength, int termBufferLength, int mtuLength,
            int sessionId, int streamId,
            String strippedChannel, String originalChannel, String sourceIdentity)
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
                replay.poll(                                                       // <4>
                    (buffer, offset, length, header) ->
                    {
                        final String message = buffer.getStringAscii(offset);
                        System.out.println(message + ", header position: " + header.position());
                        lastPosition.value = header.position();                    // <5>
                    }, 100);
            }
            while (lastPosition.value < stopPosition);
        }
        // end::handler[]
    }
}
