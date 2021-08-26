package io.aeron;

import io.aeron.archive.Archive;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.driver.MediaDriver;
import io.aeron.test.Tests;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;

import java.io.File;

public class DuplicateReplayTest
{



    @Test
    void replayTwiceOnSameSubscription()
    {
        final MutableDirectBuffer message = new UnsafeBuffer(new byte[1024]);

        try (MediaDriver mediaDriver = MediaDriver.launchEmbedded(
                new MediaDriver.Context()
                    .dirDeleteOnStart(true)
                    .spiesSimulateConnection(true));
            Archive archive = Archive.launch(
                new Archive.Context()
                    .deleteArchiveOnStart(true)
                    .archiveDirectoryName("myarchive")
                    .aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
            AeronArchive aeronArchive = AeronArchive.connect(
                new AeronArchive.Context()
                    .aeron(aeron)
                    .ownsAeronClient(false));)
        {
            final String channel = "aeron:udp?endpoint=localhost:9999";
            final int streamId = 1001;
            final Publication publication = aeronArchive.addRecordedPublication(channel, streamId);
            final String replaySubChannel = "aeron:udp?endpoint=localhost:0";
            final int replayStreamId = 1002;
            final Subscription subscription = aeron.addSubscription(replaySubChannel, replayStreamId);

            long position = 0;
            for (int i = 0; i < 20; i++)
            {
                final int length = message.putStringAscii(0, "Hello World - " + i);
                while (publication.offer(message, 0, length) < 0)
                {
                    Tests.sleep(1);
                }

                position = publication.position();
            }

//            long recordingId;
//            while (Aeron.NULL_VALUE == (recordingId = aeronArchive.findLastMatchingRecording(0, channel, 1001, publication.sessionId())))
//            {
//                Tests.sleep(1);
//            }

            long recordingId;
            for (;;)
            {
                try
                {
                    recordingId = aeronArchive.findLastMatchingRecording(0, "endpoint=localhost:9999", streamId, publication.sessionId());
                    break;
                }
                catch (ArchiveException ignore)
                {
                    Tests.sleep(1);
                }
            }

            while (position > aeronArchive.getRecordingPosition(recordingId))
            {
                Tests.sleep(1);
            }

            System.out.println("Recording position: " + position);

            ChannelUri replayUri = ChannelUri.parse(replaySubChannel);
            replayUri.put(CommonContext.ENDPOINT_PARAM_NAME, subscription.resolvedEndpoint());
            final String replaySubResolvedChannel = replayUri.toString();

            {
                final long replaySession = aeronArchive.startReplay(recordingId, 0, Long.MAX_VALUE, replaySubResolvedChannel, replayStreamId);
                final int relaySessionId = (int)(0xFFFFFFFFL & replaySession);

                final MutableLong replayedPosition = new MutableLong(0);
                while (replayedPosition.get() < position)
                {
                    final Image image = subscription.imageBySessionId(relaySessionId);

                    if (null != image)
                    {
                        subscription.poll(
                            (buffer, offset, length, header) ->
                            {
                                System.out.println("Replay position: " + header.position());
                                replayedPosition.set(header.position());
                            },
                            30);
                    }
                    Tests.sleep(1);
                }
                aeronArchive.stopReplay(replaySession);

                for (;;)
                {
                    final Image image = subscription.imageBySessionId(relaySessionId);
                    if (null == image || image.isEndOfStream())
                    {
                        System.out.println("Finished with image: " + image);
                        break;
                    }
                    else
                    {
                        image.poll((buffer, offset, length, header) -> {}, 100);
                    }
                }
            }

            {
                final long replaySession = aeronArchive.startReplay(recordingId, 0, Long.MAX_VALUE, replaySubResolvedChannel, replayStreamId);
                final int relaySessionId = (int)(0xFFFFFFFFL & replaySession);

                final MutableLong replayedPosition = new MutableLong(0);
                while (replayedPosition.get() < position)
                {
                    final Image image = subscription.imageBySessionId(relaySessionId);

                    if (null != image)
                    {
                        subscription.poll(
                            (buffer, offset, length, header) ->
                            {
                                System.out.println("Replay position: " + header.position());
                                replayedPosition.set(header.position());
                            },
                            30);
                    }
                    Tests.sleep(1);
                }
                aeronArchive.stopReplay(replaySession);
            }
        }


    }
}
