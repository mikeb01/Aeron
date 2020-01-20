package io.aeron.samples.tutorial.cluster;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.OneToOneRingBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.samples.tutorial.cluster.BasicAuctionClusteredService.*;
import static io.aeron.samples.tutorial.cluster.BasicAuctionClusteredServiceNode.CLIENT_FACING_PORT_OFFSET;
import static io.aeron.samples.tutorial.cluster.BasicAuctionClusteredServiceNode.calculatePort;

// tag::client[]
public class BasicAuctionClusterClient implements EgressListener
// end::client[]
{
    private final MutableDirectBuffer actionBidBuffer = new ExpandableDirectByteBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final OneToOneRingBuffer inputRequests = new OneToOneRingBuffer(new UnsafeBuffer(new byte[1024]));
    private final long customerId;

    private long correlationId = ThreadLocalRandom.current().nextLong();
    private Thread consoleThread = null;

    public BasicAuctionClusterClient(final long customerId)
    {
        this.customerId = customerId;
    }

    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
        final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long currentPrice = buffer.getLong(offset + PRICE_OFFSET);
        final boolean bidSucceed = 0 != buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

        printOutput(
            "SessionMessage(" + clusterSessionId + "," + correlationId + "," +
            customerId + "," + currentPrice + "," + bidSucceed + ")");
    }

    public void sessionEvent(
        final long correlationId,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        printOutput(
            "SessionEvent(" + correlationId + "," + leadershipTermId + "," +
            leaderMemberId + "," + code + "," + detail + ")");
    }

    public void newLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String memberEndpoints)
    {
        printOutput("New Leader(" + clusterSessionId + "," + leadershipTermId + "," + leaderMemberId + ")");
    }

    private void bidInAuction(final AeronCluster aeronCluster)
    {
        long keepAliveDeadlineMs = 0;

        while (!Thread.currentThread().isInterrupted())
        {
            int work = inputRequests.read((msgTypeId, buffer, index, length) ->
            {
                final long price = buffer.getLong(index);
                sendBid(aeronCluster, price);
            });

            final long currentTimeMs = System.currentTimeMillis();

            if (keepAliveDeadlineMs <= currentTimeMs)
            {
                aeronCluster.sendKeepAlive();
                keepAliveDeadlineMs = currentTimeMs + 1_000;
            }

            work += aeronCluster.pollEgress();

            idleStrategy.idle(work);
        }
    }

    private void sendBid(final AeronCluster aeronCluster, final long price)
    {
        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, correlationId++);
        actionBidBuffer.putLong(CUSTOMER_ID_OFFSET, customerId);
        actionBidBuffer.putLong(PRICE_OFFSET, price);

        while (aeronCluster.offer(actionBidBuffer, 0, BID_MESSAGE_LENGTH) < 0)
        {
            idleStrategy.idle(aeronCluster.pollEgress());
        }

        printOutput("Sent (" + (correlationId - 1) + "," + customerId + "," + price + ")");
    }

    public void startConsoleReader(final ThreadFactory threadFactory)
    {
        consoleThread = threadFactory.newThread(() ->
        {
            final MutableDirectBuffer inputRequest = new ExpandableDirectByteBuffer();

            final BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
            try
            {
                printOutput("");

                while (!Thread.currentThread().isInterrupted())
                {
                    final String line;
                    if (null == (line = in.readLine()))
                    {
                        break;
                    }

                    try
                    {
                        final long price = Long.parseLong(line.trim());
                        inputRequest.putLong(0, price);
                        inputRequests.write(1, inputRequest, 0, BitUtil.SIZE_OF_LONG);
                    }
                    catch (final NumberFormatException e)
                    {
                        printOutput("Invalid number: " + line);
                    }
                }
            }
            catch (final IOException e)
            {
                e.printStackTrace();
            }
        });

        consoleThread.start();
    }

    public static String clientFacingMembers(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private void printOutput(final String message)
    {
        System.out.println(message);
        System.out.print("$ ");
    }

    public static void main(final String[] args)
    {
        final int customerId = Integer.parseInt(System.getProperty("aeron.tutorial.cluster.customerId")); // <1>

        final String clusterMembers = clientFacingMembers(Arrays.asList("localhost", "localhost", "localhost"));
        final BasicAuctionClusterClient client = new BasicAuctionClusterClient(customerId);

        client.startConsoleReader(target ->
        {
            final Thread thread = new Thread(target);
            thread.setDaemon(true);
            return thread;
        });

        // tag::connect[]
        final int egressPort = 19000 + customerId;

        try (
            MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()  // <1>
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
            AeronCluster aeronCluster = AeronCluster.connect(new AeronCluster.Context()
                .egressListener(client)                                                     // <2>
                .egressChannel("aeron:udp?endpoint=localhost:" + egressPort)                // <3>
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")                                                // <4>
                .clusterMemberEndpoints(clusterMembers)))                                   // <5>
        {
        // end::connect[]
            client.bidInAuction(aeronCluster);
        }
    }
}
