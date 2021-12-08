/*
 * Copyright 2014-2021 Real Logic Limited.
 * Copyright 2015 Kaazing Corporation
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
package io.aeron.samples;
import io.aeron.*;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.RecordingSignalConsumer;
import io.aeron.archive.codecs.RecordingSignal;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.samples.archive.RecordingDescriptor;
import io.aeron.samples.archive.RecordingDescriptorCollector;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.CountersReader;


@SuppressWarnings("checkstyle")
class PersistentPublication implements RecordingSignalConsumer
{

    private final String notificationChannel;
    private final int notificationStream;
    private final String replayChannel;
    private final int replayStream;
    private final Long2ObjectHashMap<RecordingSignal> recordingSignalByCorrelationId = new Long2ObjectHashMap<>();
    private final Cluster cluster;
    private final HWMarkedPublication publication;
    private final int positionCounterId;
    private final AeronArchive archive;

    PersistentPublication(
        final String notificationChannel,
        final int notificationStream,
        final String replayChannel,
        final int replayStream,
        final Cluster cluster)
    {
        this.notificationChannel = notificationChannel;
        this.notificationStream = notificationStream;
        this.replayChannel = replayChannel;
        this.replayStream = replayStream;
        this.archive = AeronArchive.connect(
            cluster.context().archiveContext().clone().idleStrategy(cluster.idleStrategy()).recordingSignalConsumer(this));
        this.cluster = cluster;
        this.publication = addPublication(cluster.idleStrategy());
        this.positionCounterId = findPositionCounter(publication, cluster.idleStrategy());
    }

    HWMarkedPublication setup()
    {
        return publication;
    }

    private int findPositionCounter(
        final HWMarkedPublication publication,
        final IdleStrategy idleStrategy)
    {
        idleStrategy.reset();

        final CountersReader countersReader = cluster.context().aeron().countersReader();
        int counterId;
        while (Aeron.NULL_VALUE == (counterId = RecordingPos.findCounterIdBySession(
            countersReader, publication.getPublication().sessionId())))
        {
            idleStrategy.idle();
        }

        return counterId;
    }

    @SuppressWarnings("checkstyle:MethodLength")
    private HWMarkedPublication addPublication(final IdleStrategy idleStrategy)
    {
        idleStrategy.reset();
        final RecordingDescriptorCollector collector = new RecordingDescriptorCollector(1);

        if (0 == archive.listRecordingsForUri(0, 1, notificationChannel, notificationStream, collector.reset()))
        {
            final ExclusivePublication exclusivePublication =
                archive.context().aeron().addExclusivePublication(notificationChannel, notificationStream);

            archive.startRecording(notificationChannel, notificationStream, SourceLocation.LOCAL);
            return new HWMarkedPublication(exclusivePublication, -1);
        }
        else
        {
            final RecordingDescriptor descriptor = collector.descriptors().get(0);

            if (descriptor.stopPosition() <= 0)
            {
                final String channelUri = new ChannelUriStringBuilder(notificationChannel)
                    .initialPosition(descriptor.stopPosition(), descriptor.initialTermId(), descriptor.termBufferLength())
                    .build();

                final ExclusivePublication exclusivePublication =
                    archive.context().aeron().addExclusivePublication(channelUri, notificationStream);

                archive.extendRecording(
                    descriptor.recordingId(), notificationChannel, notificationStream, SourceLocation.LOCAL);

                return new HWMarkedPublication(exclusivePublication, -1);
            }

            final int positionBitsToShift = LogBufferDescriptor.positionBitsToShift(descriptor.termBufferLength());

            int activeTermId = LogBufferDescriptor.computeTermIdFromPosition(
                descriptor.stopPosition(), positionBitsToShift, descriptor.initialTermId());

            final LogPositionScanner logPositionScanner = new LogPositionScanner(Aeron.NULL_VALUE);
            try (Subscription replaySub = archive.context().aeron().addSubscription(replayChannel, replayStream))
            {
                idleStrategy.reset();
                do
                {
                    final long termStartPosition = LogBufferDescriptor.computePosition(
                        activeTermId,
                        0,
                        positionBitsToShift,
                        descriptor.initialTermId());
                    final long termFinishPosition = LogBufferDescriptor.computePosition(
                        activeTermId + 1,
                        0,
                        positionBitsToShift,
                        descriptor.initialTermId());

                    final long replayPosition = Math.max(termStartPosition, descriptor.startPosition());
                    final long limitPosition = Math.min(termFinishPosition, descriptor.stopPosition());

                    if (replayPosition < descriptor.stopPosition())
                    {
                        final long replaySessionId = archive.startReplay(
                            descriptor.recordingId(),
                            replayPosition,
                            limitPosition - replayPosition,
                            replaySub.channel(),
                            replaySub.streamId());
                        final int imageSessionId = (int)(replaySessionId & 0xFFFFFFFFL);

                        Image replayImage;
                        while (null == (replayImage = replaySub.imageBySessionId(imageSessionId)))
                        {
                            idleStrategy.idle();
                        }

                        logPositionScanner.reset();
                        while (!replayImage.isEndOfStream())
                        {
                            replayImage.poll(logPositionScanner::onMessage, 10);
                        }
                    }

                    activeTermId--;
                }
                while (activeTermId >= descriptor.initialTermId() &&
                    Aeron.NULL_VALUE == logPositionScanner.getLastEndPosition());
            }

            if (logPositionScanner.getLastEndPosition() < descriptor.stopPosition())
            {
                recordingSignalByCorrelationId.clear();
                archive.truncateRecording(descriptor.recordingId(), logPositionScanner.getLastEndPosition());
                final long correlationId = archive.lastCorrelationId();

                idleStrategy.reset();
                while (RecordingSignal.DELETE != recordingSignalByCorrelationId.get(correlationId))
                {
                    idleStrategy.idle(archive.pollForRecordingSignals());
                }
            }
            else if (logPositionScanner.getLastEndPosition() > descriptor.stopPosition())
            {
                throw new RuntimeException("Should not be possible");
            }

            final String channelUri = new ChannelUriStringBuilder(notificationChannel)
                .initialPosition(logPositionScanner.getLastEndPosition(), descriptor.initialTermId(), descriptor.termBufferLength())
                .build();

            final ExclusivePublication exclusivePublication =
                archive.context().aeron().addExclusivePublication(channelUri, notificationStream);

            archive.extendRecording(
                descriptor.recordingId(), notificationChannel, notificationStream, SourceLocation.LOCAL);

            return new HWMarkedPublication(exclusivePublication, logPositionScanner.getLogPosition());
        }
    }

    public void onSignal(
        final long controlSessionId,
        final long correlationId,
        final long recordingId,
        final long subscriptionId,
        final long position,
        final RecordingSignal signal)
    {
        if (RecordingSignal.DELETE == signal)
        {
            recordingSignalByCorrelationId.put(correlationId, signal);
        }
    }

    void sync()
    {
        final long currentPosition = publication.getPublication().position();

        cluster.idleStrategy().reset();
        while (currentPosition != archive.context().aeron().countersReader().getCounterValue(positionCounterId))
        {
            cluster.idleStrategy().idle();
        }
    }
}
