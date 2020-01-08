package io.aeron.samples.tutorial.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;

import java.util.HashMap;

// tag::new_service[]
public class StringCountClusteredService implements ClusteredService
// end::new_service[]
{
    private final HashMap<String, MutableInteger> receviedStrings = new HashMap<>();
    private final ExpandableArrayBuffer egressMessageBuffer = new ExpandableArrayBuffer(4);

    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        while (null != snapshotImage && !snapshotImage.isEndOfStream())  // <1>
        {
            final int fragmentsPolled = snapshotImage.poll((buffer, offset, length, header) ->
            {
                final int count = buffer.getInt(offset);
                final String receivedString = buffer.getStringAscii(offset + BitUtil.SIZE_OF_INT);
                receviedStrings.put(receivedString, new MutableInteger(count));
            }, 10);

            cluster.idle(fragmentsPolled);                              // <2>
        }
    }
    // end::start[]

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
        final String receivedString = buffer.getStringUtf8(offset);
        MutableInteger mutableInteger = receviedStrings.computeIfAbsent(receivedString, s -> new MutableInteger(0));
        mutableInteger.value++;

        egressMessageBuffer.putInt(0, mutableInteger.value);

        // TODO: how does this work under replay?
        while (session.offer(egressMessageBuffer, 0, BitUtil.SIZE_OF_INT) < 0)
        {
            if (Thread.currentThread().isInterrupted())
            {
                break;
            }
            Thread.yield();
        }
    }

    public void onTimerEvent(final long correlationId, final long timestamp)
    {

    }

    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {

    }

    public void onRoleChange(final Cluster.Role newRole)
    {

    }

    public void onTerminate(final Cluster cluster)
    {

    }
}
