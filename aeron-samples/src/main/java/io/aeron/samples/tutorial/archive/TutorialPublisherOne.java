package io.aeron.samples.tutorial.archive;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.TimeoutException;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class TutorialPublisherOne
{
    public static void main(final String[] args)
    {
        // tag::connect[]
        final int numMessages = 10;
        final Aeron.Context aeronCtx = new Aeron.Context();
        final AeronArchive.Context aArchiveCtx = new AeronArchive.Context();

        try (Aeron aeron = Aeron.connect(aeronCtx);
            AeronArchive archiveClient = AeronArchive.connect(aArchiveCtx.aeron(aeron)))  // <1>
        {
            try (Publication pub = archiveClient.addRecordedPublication(
                "aeron:udp?endpoint=localhost:4444", 10000))                              // <2>
            {
                publishMessages(pub, numMessages);
            }
        }
        // end::connect[]
    }

    // tag::publication[]
    private static void checkDeadline(final long deadLineMs, final long result) throws TimeoutException
    {
        if (System.currentTimeMillis() > deadLineMs)
        {
            throw new TimeoutException(
                "Failed to deliver message within deadline, last result code: " + result,
                AeronException.Category.ERROR);
        }
    }

    private static void publishMessages(final Publication pub, final int numMessages)
    {
        final MutableDirectBuffer buffer = new UnsafeBuffer(new byte[1024]);
        for (int i = 0; i < numMessages; i++)
        {
            final String message = "Hello " + i;
            final int length = buffer.putStringAscii(0, message);
            final long deadLineMs = System.currentTimeMillis() + 10_000;
            long result;
            do
            {
                result = pub.offer(buffer, 0, length);
                checkDeadline(deadLineMs, result);
            }
            while (result < 0);

            System.out.println("Publish message: " + i);
        }
    }
    // end::publication[]
}
