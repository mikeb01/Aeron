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

public class RecordingDescriptor
{
    public long controlSessionId;
    public long correlationId;
    public long recordingId;
    public long startTimestamp;
    public long stopTimestamp;
    public long startPosition;
    public long stopPosition;
    public int initialTermId;
    public int segmentFileLength;
    public int termBufferLength;
    public int mtuLength;
    public int sessionId;
    public int streamId;
    public String strippedChannel;
    public String originalChannel;
    public String sourceIdentity;

    public RecordingDescriptor reset() {
        this.controlSessionId = 0;
        this.correlationId = 0;
        this.recordingId = 0;
        this.startTimestamp = 0;
        this.stopTimestamp = 0;
        this.startPosition = 0;
        this.stopPosition = 0;
        this.initialTermId = 0;
        this.segmentFileLength = 0;
        this.termBufferLength = 0;
        this.mtuLength = 0;
        this.sessionId = 0;
        this.streamId = 0;
        this.strippedChannel = null;
        this.originalChannel = null;
        this.sourceIdentity = null;

        return this;
    }

    RecordingDescriptor set(
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
        this.controlSessionId = controlSessionId;
        this.correlationId = correlationId;
        this.recordingId = recordingId;
        this.startTimestamp = startTimestamp;
        this.stopTimestamp = stopTimestamp;
        this.startPosition = startPosition;
        this.stopPosition = stopPosition;
        this.initialTermId = initialTermId;
        this.segmentFileLength = segmentFileLength;
        this.termBufferLength = termBufferLength;
        this.mtuLength = mtuLength;
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.strippedChannel = strippedChannel;
        this.originalChannel = originalChannel;
        this.sourceIdentity = sourceIdentity;

        return this;
    }

    public String toString()
    {
        return "RecordingDescriptor{" +
            "controlSessionId=" + controlSessionId +
            ", correlationId=" + correlationId +
            ", recordingId=" + recordingId +
            ", startTimestamp=" + startTimestamp +
            ", stopTimestamp=" + stopTimestamp +
            ", startPosition=" + startPosition +
            ", stopPosition=" + stopPosition +
            ", initialTermId=" + initialTermId +
            ", segmentFileLength=" + segmentFileLength +
            ", termBufferLength=" + termBufferLength +
            ", mtuLength=" + mtuLength +
            ", sessionId=" + sessionId +
            ", streamId=" + streamId +
            ", strippedChannel='" + strippedChannel + '\'' +
            ", originalChannel='" + originalChannel + '\'' +
            ", sourceIdentity='" + sourceIdentity + '\'' +
            '}';
    }
}
