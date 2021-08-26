/*
 * Copyright 2014-2021 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.PrintingRecordingConsumer;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import io.aeron.samples.cluster.ClusterConfig;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;

import static io.aeron.cluster.service.Cluster.Role.LEADER;

public class LateJoinFromLeaderLogTest
{

    public static final int PORT_BASE = 9000;

    @Test
    @Timeout(30)
    void shouldCatchUpFromLog()
    {
        final ClusterNode[] clusterNodes = new ClusterNode[3];
        final List<String> hostnames = Arrays.asList("localhost", "localhost", "localhost");
        final String ingressChannel = "aeron:udp?term-length=64k";

        for (int i = 0, n = clusterNodes.length; i < n; i++)
        {
            int nodeId = i;
            final ClusterConfig config = ClusterConfig.create(nodeId, hostnames, 9000, new Sequencer("Sequencer"));
            config.consensusModuleContext()
                .ingressChannel(ingressChannel)
                .deleteDirOnStart(true);

            clusterNodes[i] = new ClusterNode(config);
        }

        for (ClusterNode clusterNode : clusterNodes)
        {
            clusterNode.start();
        }

        int leaderId = findLeader(clusterNodes, Aeron.NULL_VALUE);

        final MutableDirectBuffer message = new UnsafeBuffer(new byte[64]);
        final String ingressEndpoints = ClusterConfig.ingressEndpoints(hostnames, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET);

        try (
            final MediaDriver clientDriverA = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                    .threadingMode(ThreadingMode.SHARED)
                    .sharedIdleStrategy(new BackoffIdleStrategy()));
            final AeronCluster sendingClient = AeronCluster.connect(
                new AeronCluster.Context()
                    .aeronDirectoryName(clientDriverA.aeronDirectoryName())
                    .ingressChannel(ingressChannel)
                    .ingressEndpoints(ingressEndpoints));
            final SequencerClient earlyClient = new SequencerClient(
                "Early", "localhost", sendingClient.context().aeron(), ingressChannel, hostnames);
            final SequencerClient lateClient = new SequencerClient(
                "Late", "localhost", sendingClient.context().aeron(), ingressChannel, hostnames);
        )
        {
            earlyClient.start();

            sleep(1000);

            for (int i = 0; i < 10; i++)
            {
                int length = message.putStringAscii(0, "Hello World - " + i);

                while (0 > sendingClient.offer(message, 0, length))
                {
                    sendingClient.pollEgress();
                    sleep(1);
                }

                // Simulate some other client...
                earlyClient.poll();
            }

            while (earlyClient.lastMessageId < 9)
            {
                earlyClient.poll();
                sendingClient.pollEgress();
                sleep(1);
            }

            clusterNodes[leaderId].close();

            findLeader(clusterNodes, leaderId);

            lateClient.start();
            sleep(100);

            for (int i = 10; i < 20; i++)
            {
                long deadlineMs = System.currentTimeMillis() + 200;
                while (System.currentTimeMillis() < deadlineMs)
                {
                    lateClient.poll();
                    earlyClient.poll();
                    sleep(1);
                }

                int length = message.putStringAscii(0, "Hello World - " + i);

                while (0 > sendingClient.offer(message, 0, length))
                {
                    sendingClient.pollEgress();
                    sleep(1);
                }

            }

            while (earlyClient.lastMessageId < 19 || lateClient.lastMessageId < 19)
            {
                earlyClient.poll();
                lateClient.poll();
                sleep(1);
            }
        }
    }

    private static int findLeader(final ClusterNode[] clusterNodes, int skipNodeId)
    {
        int leaderId = Aeron.NULL_VALUE;
        while (Aeron.NULL_VALUE == leaderId)
        {
            for (ClusterNode clusterNode : clusterNodes)
            {
                final ConsensusModule.Context context = clusterNode.clusterConfig.consensusModuleContext();
                final int nodeId = context.clusterMemberId();

                if (skipNodeId == nodeId)
                {
                    continue;
                }

                if (LEADER == Cluster.Role.get(context.clusterNodeRoleCounter()))
                {
                    leaderId = nodeId;
                    System.out.println("Found leader: " + nodeId);
                    break;
                }
            }

            sleep(1);
        }

        return leaderId;
    }

    private static class ClusterNode implements AutoCloseable
    {
        private final ClusterConfig clusterConfig;
        private ClusteredMediaDriver clusteredMediaDriver;
        private ClusteredServiceContainer container;

        public ClusterNode(ClusterConfig config)
        {
            this.clusterConfig = config;
        }

        public void start()
        {
            clusteredMediaDriver = ClusteredMediaDriver.launch(
                clusterConfig.mediaDriverContext(),
                clusterConfig.archiveContext(),
                clusterConfig.consensusModuleContext());
            container = ClusteredServiceContainer.launch(
                clusterConfig.clusteredServiceContext());
        }

        public void close()
        {
            CloseHelper.closeAll(container, clusteredMediaDriver);
        }
    }

    private static class Sequencer implements ClusteredService
    {
        public static final String SEQUENCER_CHANNEL = "aeron:udp?endpoint=224.10.9.9:10001";
        public static final int SEQUENCER_STREAM_ID = 10000;
        private final ExpandableArrayBuffer outboundHeader = new ExpandableArrayBuffer();
        private final String name;

        private Cluster cluster;
        private long messageId;
        private Publication outboundPublication;

        public Sequencer(String name)
        {
            this.name = name;
        }

        private void log(String s)
        {
            System.out.printf("[%s] %s%n", name, s);
        }

        public void onStart(final Cluster cluster, final Image snapshotImage)
        {
            this.cluster = cluster;
            if (null == snapshotImage)
            {
                this.messageId = 0;
            }
            else
            {
                loadSnapshot(snapshotImage);
            }

            outboundPublication = cluster.aeron().addPublication(SEQUENCER_CHANNEL, SEQUENCER_STREAM_ID);
        }

        private void loadSnapshot(final Image snapshotImage)
        {
            while (!snapshotImage.isEndOfStream())
            {
                snapshotImage.poll(
                    (buffer, offset, length, header) ->
                    {
                        messageId = buffer.getLong(offset);
                    },
                    10);
            }
        }

        public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header)
        {
            long messageId = this.messageId;
            this.messageId++;

            if (LEADER == cluster.role())
            {
                outboundHeader.putLong(0, messageId);
                outboundHeader.putLong(8, header.position());

                int retryLimit = 5;
                cluster.idleStrategy().reset();
                do
                {
                    long position = outboundPublication.offer(
                        outboundHeader, 0, 16,
                        buffer, offset, length);

                    if (0 < position)
                    {
                        log("Relayed, message: " + messageId + ", position: " + header.position());
                        break;
                    }
                    else if (Publication.ADMIN_ACTION != position && Publication.BACK_PRESSURED != position)
                    {
                        log(
                            "Dropped, message: " + messageId + ", position: " + header.position() + ", reason: " + position);
                        break;
                    }
                    else
                    {
                        log("Retry: " + position);
                        cluster.idleStrategy().idle();
                    }
                }
                while (-1 != --retryLimit);
            }
        }

        public void onSessionOpen(final ClientSession session, final long timestamp)
        {
        }

        public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
        {

        }

        public void onTimerEvent(final long correlationId, final long timestamp)
        {

        }

        public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
        {
            MutableDirectBuffer buffer = new UnsafeBuffer(new byte[8]);
            buffer.putLong(0, messageId);
            cluster.idleStrategy().reset();
            while (snapshotPublication.offer(buffer, 0, buffer.capacity()) < 0)
            {
                cluster.idleStrategy().idle();
            }
        }

        public void onRoleChange(final Cluster.Role newRole)
        {

        }

        public void onTerminate(final Cluster cluster)
        {

        }
    }

    private static class SequencerClient implements AutoCloseable
    {
        public static final int REPLAY_STREAM_ID = 99999;
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
        private final SessionMessageHeaderDecoder sessionMessageHeaderDecoder = new SessionMessageHeaderDecoder();
        private final String name;
        private String clientHostname;
        private final Aeron aeron;
        private final String ingressChannel;
        private final List<String> hostnames;
        private final String replayChannel;
        private AeronCluster clusterClient;
        private Subscription sequencerSubscription = null;
        private Subscription logReplaySubscription = null;
        private long lastMessageId = -1;
        private long lastPosition = 0;
        private long catchupMessageId = -1;
        private long catchupPosition = 0;
        private AeronArchive[] archiveClients;

        public SequencerClient(
            final String name,
            final String clientHostname, // Must be a name that maps to a network reachable address for this host
            final Aeron aeron,
            final String ingressChannel,
            final List<String> hostnames)
        {
            this.name = name;
            this.clientHostname = clientHostname;
            this.aeron = aeron;
            this.ingressChannel = ingressChannel;
            this.hostnames = hostnames;
            replayChannel = new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(clientHostname + ":19001")
                .build();
        }

        private void log(String s)
        {
            System.out.printf("[%s] %s%n", name, s);
        }

        public void start()
        {
            final String ingressEndpoints = ClusterConfig.ingressEndpoints(hostnames, PORT_BASE, ClusterConfig.CLIENT_FACING_PORT_OFFSET);

            try
            {
                clusterClient = AeronCluster.connect(
                    new AeronCluster.Context()
                        .aeron(aeron)
                        .ownsAeronClient(false)
                        .ingressChannel(ingressChannel)
                        .ingressEndpoints(ingressEndpoints));

                sequencerSubscription = clusterClient.context().aeron()
                    .addSubscription(Sequencer.SEQUENCER_CHANNEL, Sequencer.SEQUENCER_STREAM_ID);

                archiveClients = new AeronArchive[hostnames.size()];
            }
            catch (RuntimeException ex)
            {
                close();
                throw ex;
            }
        }

        public int poll()
        {
            int workDone = 0;
            if (null != sequencerSubscription)
            {
                workDone += sequencerSubscription.poll(this::liveFragment, 20);
            }

            if (null != logReplaySubscription)
            {
                workDone += logReplaySubscription.poll(this::replayFragment, 20);
            }

            workDone += clusterClient.pollEgress();

            return workDone;
        }

        public void close()
        {
            CloseHelper.quietCloseAll(logReplaySubscription, sequencerSubscription, clusterClient);
            CloseHelper.quietCloseAll(archiveClients);
        }

        public void liveFragment(DirectBuffer buffer, int offset, int length, Header header)
        {
            long messageId = buffer.getLong(offset);
            long position = buffer.getLong(offset + 8);

            if (lastMessageId + 1 == messageId && lastPosition < position)
            {
                lastMessageId = messageId;
                lastPosition = position;
                log("Live: " + messageId + "/" + position);

                processApplicationMessage(buffer.getStringAscii(offset + 16));

                if (null != logReplaySubscription)
                {
                    closeReplay();
                }
            }
            else if (position == lastPosition)
            {
                lastMessageId = messageId;
                log("Live (drop/updating message id)");
            }
            else if (lastMessageId + 1 < messageId)
            {
                catchupPosition = position;
                catchupMessageId = messageId;

                startArchiveReplay(lastPosition, position);
                log("Live (drop and catchup): " + messageId + "/" + position);
            }
            else
            {
                log("Live (drop): " + messageId + "/" + position);
            }
        }

        private void processApplicationMessage(final String message)
        {
            log("Processing application message: " + message);
        }

        public void replayFragment(DirectBuffer buffer, int offset, int length, Header header)
        {
            messageHeaderDecoder.wrap(buffer, offset);

            // Only process session messages, timer messages will need to be handled as well.
            if (messageHeaderDecoder.templateId() == SessionMessageHeaderDecoder.TEMPLATE_ID)
            {
                sessionMessageHeaderDecoder.wrapAndApplyHeader(buffer, offset, messageHeaderDecoder);

                final String message = buffer.getStringAscii(
                    offset + messageHeaderDecoder.encodedLength() + sessionMessageHeaderDecoder.encodedLength());

                // Must only process replay messages <= catchupPosition
                // Log can be further ahead than what has reached consensus
                if (header.position() <= catchupPosition)
                {
                    lastPosition = header.position();
                    log("Replay: " + lastMessageId + "/" + lastPosition + "/" + header.sessionId());
                    // Once we have caught up, set the lastMessageId.
                    if (lastPosition == catchupPosition)
                    {
                        lastMessageId = catchupMessageId;
                        log("Replay (caught up)");


                        closeReplay();
                    }
                    processApplicationMessage(message);
                }
                // Drop all other messages.
                else
                {
                    log("Replay (drop): " + header.position());
                }
            }
        }

        private void closeReplay()
        {
            log("Closing replay");
            CloseHelper.quietClose(logReplaySubscription);
            logReplaySubscription = null;
        }

        private void startArchiveReplay(final long lastPosition, final long position)
        {
            if (null == logReplaySubscription)
            {
                log("Setting up replay");

                final int memberId = clusterClient.leaderMemberId();
                final String uri = "aeron:udp?endpoint=" + hostnames.get(memberId) + ":" + ClusterConfig.calculatePort(memberId, PORT_BASE, ClusterConfig.ARCHIVE_CONTROL_PORT_OFFSET);

                if (null == archiveClients[memberId])
                {
                    archiveClients[memberId] = AeronArchive.connect(
                        new AeronArchive.Context()
                            .aeron(clusterClient.context().aeron())
                            .controlRequestChannel(uri)
                            .controlResponseChannel("aeron:udp?endpoint=" + clientHostname + ":0")
                            .ownsAeronClient(false));
                }

                final AeronArchive archiveClient = archiveClients[memberId];

                final CaptureRecordingsConsumer captureConsumer = new CaptureRecordingsConsumer();
                captureConsumer.clear();

                final PrintingRecordingConsumer printConsumer = new PrintingRecordingConsumer(captureConsumer);
                archiveClient.listRecordingsForUri(
                    -1, Integer.MAX_VALUE, "alias=log", ConsensusModule.Configuration.logStreamId(), printConsumer);

                if (Aeron.NULL_VALUE != captureConsumer.logRecordingId)
                {
                    // TODO: If wanting to use an archive other than the leaders, you will need to check
                    // TODO: that the recording position is more up to date.
                    final long recordingPosition = archiveClient.getRecordingPosition(captureConsumer.logRecordingId);

                    logReplaySubscription = archiveClient.replay(
                        captureConsumer.logRecordingId, lastPosition, Long.MAX_VALUE, replayChannel, REPLAY_STREAM_ID,
                        image -> {},
                        // TODO: Use availability handle to detect failed replays.
                        image ->
                        {

                        });
                }
            }
        }
    }

    private static class CaptureRecordingsConsumer implements RecordingDescriptorConsumer
    {
        private long logRecordingId = Aeron.NULL_VALUE;

        public void clear()
        {
            logRecordingId = Aeron.NULL_VALUE;
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
            final String originalChannel,
            final String sourceIdentity)
        {
            if (0 == startPosition && -1 == stopPosition)
            {
                if (Aeron.NULL_VALUE == logRecordingId)
                {
                    logRecordingId = recordingId;
                }
            }
        }
    }

    static void sleep(long ms)
    {
        try
        {
            Thread.sleep(ms);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
