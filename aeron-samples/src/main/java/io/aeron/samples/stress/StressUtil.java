/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.samples.stress;

import io.aeron.ChannelUriStringBuilder;
import io.aeron.Image;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static io.aeron.driver.Configuration.MAX_UDP_PAYLOAD_LENGTH;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Simple common methods and constants for the stress client/server.
 */
public class StressUtil
{
    public static final int BASE_PORT = 9000;
    public static final int BASE_STREAM_ID = 10000;
    public static final int UNICAST_REQ_OFFSET = 1;
    public static final int UNICAST_RSP_OFFSET = 2;
    public static final int UNICAST_STREAM_ID_OFFSET = 1;
    public static final int MDC_STREAM_ID_OFFSET = 2;
    public static final int UNICAST_STREAM_ID = BASE_STREAM_ID + UNICAST_STREAM_ID_OFFSET;
    public static final int MDC_STREAM_ID = BASE_STREAM_ID + MDC_STREAM_ID_OFFSET;
    public static final int MDC_REQ_OFFSET_1 = 3;
    public static final int MDC_REQ_OFFSET_2 = 4;
    public static final int MDC_REQ_CONTROL_OFFSET = 5;
    public static final int MDC_RSP_OFFSET_1 = 6;
    public static final int MDC_RSP_OFFSET_2 = 7;
    public static final int MDC_RSP_CONTROL_OFFSET = 8;
    public static final long RSP_GROUP_TAG = 1001;
    public static final long REQ_GROUP_TAG = 1002;
    public static final int SERVER_RECV_COUNT = 1000;
    public static final int SERVER_SEND_COUNT = 1001;
    public static final int CLIENT_RECV_COUNT = 1002;
    public static final int CLIENT_SEND_COUNT = 1003;
    public static final List<Integer> MTU_LENGTHS;

    static
    {
        List<Integer> parsedLengths = Arrays.asList(
            1408, 4000, 8192, 1 << 14, 1 << 15, MAX_UDP_PAYLOAD_LENGTH);
        final String mtuLengths;

        if (null != (mtuLengths = System.getProperty("aeron.stress.client.mtu.lengths")))
        {
            try
            {
                parsedLengths = Pattern.compile(",")
                    .splitAsStream(mtuLengths)
                    .map(Integer::parseInt)
                    .collect(Collectors.toList());
            }
            catch (final Exception e)
            {
                System.err.println(
                    "Unable to parse aeron.stress.client.mtu.lengths='" + mtuLengths + "', using default");
            }
        }

        MTU_LENGTHS = parsedLengths;
    }

    /**
     * Log a simple message.
     *
     * @param message to be logged
     */
    public static void info(final String message)
    {
        System.out.println(message);
    }

    /**
     * Start forming unicast request channel for the specific server.
     *
     * @param serverAddress as the endpoint receiver of the unicast traffic.
     * @return a partially constructed URI with the endpoint set.
     */
    public static ChannelUriStringBuilder unicastReqChannel(final String serverAddress)
    {
        return new ChannelUriStringBuilder().media("udp")
            .linger(0L)
            .endpoint(serverAddress + ":" + (BASE_PORT + UNICAST_REQ_OFFSET));
    }

    /**
     * Start forming unicast respopnse channel for the specific server.
     *
     * @param clientAddress as the endpoint receiver of the unicast traffic.
     * @return a partially constructed URI with the endpoint set.
     */
    public static ChannelUriStringBuilder unicastRspChannel(final String clientAddress)
    {
        return new ChannelUriStringBuilder().media("udp")
            .linger(0L)
            .endpoint(clientAddress + ":" + (BASE_PORT + UNICAST_RSP_OFFSET));
    }

    private static ChannelUriStringBuilder mdcChannel(
        final String endpointAddress,
        final int endpointOffset,
        final String controlAddress,
        final int controlOffset)
    {
        return new ChannelUriStringBuilder().media("udp")
            .endpoint(endpointAddress + ":" + (BASE_PORT + endpointOffset))
            .controlEndpoint(controlAddress + ":" + (BASE_PORT + controlOffset));
    }

    static ChannelUriStringBuilder mdcReqSubChannel1(final String serverAddress, final String clientAddress)
    {
        return mdcChannel(serverAddress, MDC_REQ_OFFSET_1, clientAddress, MDC_REQ_CONTROL_OFFSET)
            .groupTag(REQ_GROUP_TAG)
            .alias("req_sub1");
    }

    static ChannelUriStringBuilder mdcReqSubChannel2(final String serverAddress, final String clientAddress)
    {
        return mdcChannel(serverAddress, MDC_REQ_OFFSET_2, clientAddress, MDC_REQ_CONTROL_OFFSET)
            .groupTag(REQ_GROUP_TAG)
            .alias("req_sub2");
    }

    static ChannelUriStringBuilder mdcReqPubChannel(final String clientAddress)
    {
        return new ChannelUriStringBuilder().media("udp")
            .controlEndpoint(clientAddress + ":" + (BASE_PORT + MDC_REQ_CONTROL_OFFSET))
            .taggedFlowControl(REQ_GROUP_TAG, 2, "5s")
            .alias("req_pub");
    }

    static ChannelUriStringBuilder mdcRspSubChannel1(final String serverAddress, final String clientAddress)
    {
        return mdcChannel(clientAddress, MDC_RSP_OFFSET_1, serverAddress, MDC_RSP_CONTROL_OFFSET)
            .groupTag(RSP_GROUP_TAG)
            .alias("rsp_sub1");
    }

    static ChannelUriStringBuilder mdcRspSubChannel2(final String serverAddress, final String clientAddress)
    {
        return mdcChannel(clientAddress, MDC_RSP_OFFSET_2, serverAddress, MDC_RSP_CONTROL_OFFSET)
            .groupTag(RSP_GROUP_TAG)
            .alias("rsp_sub2");
    }

    static ChannelUriStringBuilder mdcRspPubChannel(final String serverAddress)
    {
        return new ChannelUriStringBuilder().media("udp")
            .controlEndpoint(serverAddress + ":" + (BASE_PORT + MDC_RSP_CONTROL_OFFSET))
            .taggedFlowControl(RSP_GROUP_TAG, 2, "5s")
            .alias("rsp_pub");
    }

    static void imageAvailable(final Image image)
    {
        info("Available image=" + image);
    }

    static void imageUnavailable(final Image image)
    {
        info("Unavailable image=" + image);
    }

    static void error(final String message)
    {
        System.err.println(message);
    }

    static String serverAddress()
    {
        return System.getProperty("aeron.stress.server.address", "localhost");
    }

    static String clientAddress()
    {
        return System.getProperty("aeron.stress.client.address", "localhost");
    }

    static boolean crcMatches(final DirectBuffer msg, final int offset, final int length, final CRC64 crc)
    {
        final long recvCrc = msg.getLong(offset, LITTLE_ENDIAN);
        final long calcCrc = crc.recalculate(msg, offset + BitUtil.SIZE_OF_LONG, length - BitUtil.SIZE_OF_LONG);
        final boolean dataMatches = calcCrc == recvCrc;
        return dataMatches;
    }

    static void validateMessage(
        final CRC64 crc,
        final DirectBuffer msg,
        final int offset,
        final int length,
        final long correlationId)
    {
        final long recvCrc = msg.getLong(offset, LITTLE_ENDIAN);
        final long calcCrc = crc.recalculate(msg, offset + BitUtil.SIZE_OF_LONG, length - BitUtil.SIZE_OF_LONG);

        if (calcCrc != recvCrc)
        {
            throw new RuntimeException(
                "CRC validation failed, correlationId=" + correlationId +
                ", length=" + length + ", calc=" + calcCrc + ", recv=" + recvCrc);
        }
    }
}
