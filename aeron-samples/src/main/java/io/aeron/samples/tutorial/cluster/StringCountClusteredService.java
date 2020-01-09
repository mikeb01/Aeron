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
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.collections.MutableReference;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

// tag::new_service[]
public class StringCountClusteredService implements ClusteredService
// end::new_service[]
{
    public static final int APPLICATION_HEADER_LENGTH = 2 * BitUtil.SIZE_OF_INT;

    private final MutableDirectBuffer egressMessageBuffer = new ExpandableDirectByteBuffer(4);
    private final MutableDirectBuffer snapshotHeaderBuffer = new ExpandableDirectByteBuffer(8);
    private final MutableDirectBuffer snapshotMessageBuffer = new ExpandableDirectByteBuffer(1024 * 1024);
    private final MutableDirectBuffer snapshotLoadBuffer = new ExpandableDirectByteBuffer(1024 * 1024);

    // tag::state[]
    private final HashMap<String, MutableInteger> receivedStrings = new HashMap<>();
    // end::state[]
    private Cluster cluster;

    // tag::start[]
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        if (null != snapshotImage)
        {
            loadSnapshot(cluster, snapshotImage);
        }
    }
    // end::start[]

    public void onSessionOpen(final ClientSession session, final long timestamp)
    {

    }

    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {

    }

    // tag::message[]
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset);
        final String key = buffer.getStringUtf8(offset + BitUtil.SIZE_OF_LONG);
        final MutableInteger mutableInteger = receivedStrings.computeIfAbsent(key, s -> new MutableInteger(0));
        mutableInteger.value++;

        egressMessageBuffer.putLong(0, correlationId);
        egressMessageBuffer.putInt(BitUtil.SIZE_OF_LONG, mutableInteger.value);

        if (null != session)
        {
            while (session.offer(egressMessageBuffer, 0, BitUtil.SIZE_OF_LONG + BitUtil.SIZE_OF_INT) < 0)
            {
                cluster.idle();
            }
        }
    }
    // end::message[]

    public void onTimerEvent(final long correlationId, final long timestamp)
    {

    }

    // tag::takeSnapshot[]
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
        final MutableInteger position = new MutableInteger(0);

        snapshotMessageBuffer.putInt(position.value, receivedStrings.size());
        position.value += BitUtil.SIZE_OF_INT;
        receivedStrings.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(e ->
        {
            final int encodedLength = encodeStringUtf8(snapshotMessageBuffer, position.value, e.getKey());
            position.value += encodedLength;
            snapshotMessageBuffer.putInt(position.value, e.getValue().value);
            position.value += BitUtil.SIZE_OF_INT;
        });

        final int totalMessageLength = position.value;
        final int maxMessageLength = snapshotPublication.maxPayloadLength() - APPLICATION_HEADER_LENGTH;

        int offset = 0;
        while (offset < totalMessageLength)
        {
            snapshotHeaderBuffer.putInt(0, offset);
            snapshotHeaderBuffer.putInt(BitUtil.SIZE_OF_INT, totalMessageLength);

            final int length = Math.min(maxMessageLength, totalMessageLength - offset);

            while (snapshotPublication.offer(
                snapshotHeaderBuffer, 0, APPLICATION_HEADER_LENGTH,
                snapshotMessageBuffer, offset, length) < 0)
            {
                cluster.idle();
            }

            offset += length;
        }
    }
    // end::takeSnapshot[]

    // tag::loadSnapshot[]
    private void loadSnapshot(final Cluster cluster, final Image snapshotImage)
    {
        final MutableInteger messageOffset = new MutableInteger(0);
        final MutableInteger messageLength = new MutableInteger(0);
        final MutableInteger totalLength = new MutableInteger(0);

        while (!snapshotImage.isEndOfStream())  // <1>
        {
            final int fragmentsPolled = snapshotImage.poll((buffer, offset, length, header) ->
            {
                messageOffset.value = buffer.getInt(offset);
                totalLength.value = buffer.getInt(offset + BitUtil.SIZE_OF_INT);
                messageLength.value = length - APPLICATION_HEADER_LENGTH;

                snapshotLoadBuffer.putBytes(
                    messageOffset.value, buffer, offset + APPLICATION_HEADER_LENGTH, messageLength.value);
            }, 10);

            if (0 < totalLength.value && messageOffset.value + messageLength.value  == totalLength.value)
            {
                int position = 0;
                final int numberOfEntries = snapshotLoadBuffer.getInt(position);
                final MutableReference<String> keyReference = new MutableReference<>();

                position += BitUtil.SIZE_OF_INT;
                while (position < totalLength.value)
                {
                    final int encodedLength = decodeStringUtf8(snapshotLoadBuffer, position, keyReference::set);
                    position += encodedLength;
                    int count = snapshotLoadBuffer.getInt(position);
                    position += BitUtil.SIZE_OF_INT;

                    receivedStrings.put(keyReference.ref, new MutableInteger(count));
                }

                assert receivedStrings.size() == numberOfEntries : "Sanity check size of map";

                break;
            }

            cluster.idle(fragmentsPolled);      // <2>
        }

        assert messageOffset.value + messageLength.value == totalLength.value :
            "Sanity check that snapshot is complete";
    }
    // end::loadSnapshot[]

    public void onRoleChange(final Cluster.Role newRole)
    {

    }

    public void onTerminate(final Cluster cluster)
    {

    }

    private int encodeStringUtf8(final MutableDirectBuffer buffer, final int offset, final String s)
    {
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        buffer.putInt(offset, bytes.length);
        buffer.putBytes(offset + BitUtil.SIZE_OF_INT, bytes);
        return BitUtil.SIZE_OF_INT + bytes.length;
    }

    private int decodeStringUtf8(
        final DirectBuffer buffer,
        final int offset,
        final Consumer<String> consumer)
    {
        final int length = buffer.getInt(offset);
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset + BitUtil.SIZE_OF_INT, bytes);
        consumer.accept(new String(bytes, StandardCharsets.UTF_8));
        return BitUtil.SIZE_OF_INT + length;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        { return true; }
        if (o == null || getClass() != o.getClass())
        { return false; }
        final StringCountClusteredService that = (StringCountClusteredService)o;
        return receivedStrings.equals(that.receivedStrings);
    }

    public int hashCode()
    {
        return Objects.hash(receivedStrings);
    }

    public String toString()
    {
        return "StringCountClusteredService{" +
               "receivedStrings=" + receivedStrings +
               '}';
    }
}
