package io.aeron.samples.tutorial.cluster;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.CommonContext;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.MinMulticastFlowControlSupplier;
import io.aeron.driver.ThreadingMode;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.ShutdownSignalBarrier;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.aeron.Aeron.NULL_VALUE;

public class BasicAuctionClusteredServiceNode
{
    private static ErrorHandler errorHandler(final String context)
    {
        return (Throwable throwable) ->
        {
            System.err.println(context);
            throwable.printStackTrace(System.err);
        };
    }

    private static final int PORT_BASE = 9000;
    private static final int PORTS_PER_NODE = 100;
    private static final int ARCHIVE_CONTROL_REQUEST_PORT_OFFSET = 1;
    private static final int ARCHIVE_CONTROL_RESPONSE_PORT_OFFSET = 2;
    private static final int CLIENT_FACING_PORT_OFFSET = 3;
    private static final int MEMBER_FACING_PORT_OFFSET = 4;
    private static final int LOG_PORT_OFFSET = 5;
    private static final int TRANSFER_PORT_OFFSET = 6;
    private static final int LOG_CONTROL_PORT_OFFSET = 7;

    private static String udpChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
            .media("udp")
            .termLength(64 * 1024)
            .endpoint(hostname + ":" + port)
            .build();
    }

    private static String logControlChannel(final int nodeId, final String hostname, final int portOffset)
    {
        final int port = calculatePort(nodeId, portOffset);
        return new ChannelUriStringBuilder()
            .media("udp")
            .termLength(64 * 1024)
            .controlMode("manual")
            .controlEndpoint(hostname + ":" + port)
            .build();
    }

    private static int calculatePort(final int nodeId, final int offset)
    {
        return PORT_BASE + (nodeId * PORTS_PER_NODE) + offset;
    }

    public static String clusterMembers(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i);
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, CLIENT_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, MEMBER_FACING_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, LOG_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':').append(calculatePort(i, TRANSFER_PORT_OFFSET));
            sb.append(',').append(hostnames.get(i)).append(':')
                .append(calculatePort(i, ARCHIVE_CONTROL_REQUEST_PORT_OFFSET));
            sb.append('|');
        }

        return sb.toString();
    }

    public static void main(final String[] args)
    {
        if (args.length != 1)
        {
            System.err.println("Usage java " + StringCountClusteredService.class.getName() + " <node id>");
            System.exit(1);
        }

        final List<String> hostnames = Arrays.asList("localhost", "localhost", "localhost");

        final int nodeId = Integer.parseInt(args[0]);
        final String hostname = hostnames.get(nodeId);

        final String baseDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId;
        final String aeronDirName = CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver";

        final ShutdownSignalBarrier barrier = new ShutdownSignalBarrier();

        final MediaDriver.Context mediaDriverContext = new MediaDriver.Context()
            .aeronDirectoryName(aeronDirName)
            .threadingMode(ThreadingMode.SHARED)
            .termBufferSparseFile(true)
            .multicastFlowControlSupplier(new MinMulticastFlowControlSupplier())
            .terminationHook(barrier::signal)
            .errorHandler(BasicAuctionClusteredServiceNode.errorHandler("Media Driver"));

        final Archive.Context archiveContext = new Archive.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveDir(new File(baseDirName, "archive"))
            .controlChannel(udpChannel(nodeId, "localhost", ARCHIVE_CONTROL_REQUEST_PORT_OFFSET))
            .localControlChannel("aeron:ipc?term-length=64k")
            .recordingEventsEnabled(false)
            .threadingMode(ArchiveThreadingMode.SHARED);

        final AeronArchive.Context aeronArchiveContext = new AeronArchive.Context()
            .controlRequestChannel(archiveContext.controlChannel())
            .controlRequestStreamId(archiveContext.controlStreamId())
            .controlResponseChannel(udpChannel(nodeId, "localhost", ARCHIVE_CONTROL_RESPONSE_PORT_OFFSET))
            .aeronDirectoryName(aeronDirName);

        final ConsensusModule.Context consensusModuleContext = new ConsensusModule.Context()
            .errorHandler(errorHandler("Consensus Module"))
            .clusterMemberId(nodeId)
            .clusterMembers(clusterMembers(hostnames))
            .appointedLeaderId(NULL_VALUE)
            .aeronDirectoryName(aeronDirName)
            .clusterDir(new File(baseDirName, "consensus-module"))
            .ingressChannel("aeron:udp?term-length=64k")
            .logChannel(logControlChannel(nodeId, hostname, LOG_CONTROL_PORT_OFFSET))
            .archiveContext(aeronArchiveContext.clone());

        final ClusteredServiceContainer.Context clusteredServiceContext = new ClusteredServiceContainer.Context()
            .aeronDirectoryName(aeronDirName)
            .archiveContext(aeronArchiveContext.clone())
            .clusterDir(new File(baseDirName, "service"))
            .clusteredService(new BasicAuctionClusteredService())
            .errorHandler(errorHandler("Clustered Service"));

        try (ClusteredMediaDriver clusteredMediaDriver =
            ClusteredMediaDriver.launch(mediaDriverContext, archiveContext, consensusModuleContext);
            ClusteredServiceContainer container = ClusteredServiceContainer.launch(clusteredServiceContext))
        {
            System.out.println("[" + nodeId + "] Started Cluster Node on " + hostname + "...");
            barrier.await();
            System.out.println("[" + nodeId + "] Exiting");
        }
    }
}
