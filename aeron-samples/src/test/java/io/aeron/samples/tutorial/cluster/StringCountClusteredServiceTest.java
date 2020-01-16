package io.aeron.samples.tutorial.cluster;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.service.Cluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.BitUtil;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.extension.TestWatcher;
import org.mockito.invocation.InvocationOnMock;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

class StringCountClusteredServiceTest
{
    public static final int STREAM_ID = 1001;

    private final long seed = System.nanoTime();
    private final Random rnd = new Random(seed);

    @RegisterExtension
    final TestWatcher testWatcher = new TestWatcher()
    {
        public void testFailed(final ExtensionContext context, final Throwable cause)
        {
            System.err.println(context.getDisplayName() + " failed with random seed: " + seed);
        }
    };

    private final Cluster cluster = mock(Cluster.class);
    //    private final List<String> messages = Arrays.asList("First Message", "Second Message");
    private final List<String> messages = generateRandomMessages(rnd, 1000, 50);

    @BeforeEach
    void setUp()
    {
        doAnswer(StringCountClusteredServiceTest::idle).when(cluster).idle();
    }

    @Test
    void shouldStoreAndLoadSnapshot() throws InterruptedException
    {
        final StringCountClusteredService serviceInitialState = new StringCountClusteredService();
        final StringCountClusteredService serviceRestoredState = new StringCountClusteredService();

        serviceInitialState.onStart(cluster, null);
        final MutableDirectBuffer buffer = new ExpandableArrayBuffer(1024);

        long correlationId = rnd.nextLong();
        for (final String message : messages)
        {
            buffer.putLong(0, correlationId++);
            final int keyLength = buffer.putStringUtf8(BitUtil.SIZE_OF_LONG, message);
            serviceInitialState.onSessionMessage(null, 0, buffer, 0, BitUtil.SIZE_OF_LONG + keyLength, null);
        }

        try (MediaDriver driver = MediaDriver.launchEmbedded(
            new MediaDriver.Context().threadingMode(ThreadingMode.SHARED));
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            ExclusivePublication snapshotPublication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID);
            Subscription subscription = aeron.addSubscription(IPC_CHANNEL, STREAM_ID))
        {
            while (!snapshotPublication.isConnected())
            {
                yieldAndCheckInterrupt();
            }

            while (subscription.hasNoImages())
            {
                yieldAndCheckInterrupt();
            }

            assertEquals(subscription.imageCount(), 1);

            final Thread t = new Thread(() ->
            {
                final Image image = subscription.imageAtIndex(0);
                serviceRestoredState.onStart(cluster, image);
            });

            t.setUncaughtExceptionHandler((t1, e) -> e.printStackTrace());
            t.start();

            serviceInitialState.onTakeSnapshot(snapshotPublication);
            snapshotPublication.close();

            t.join();

            assertEquals(serviceInitialState, serviceRestoredState);
        }
    }

    private static void yieldAndCheckInterrupt()
    {
        Thread.yield();
        if (Thread.currentThread().isInterrupted())
        {
            fail("unexpected interrupt - test likely to have timed out");
        }
    }

    private static List<String> generateRandomMessages(final Random r, final int count, final int length)
    {
        final List<String> strings = new ArrayList<>();
        final byte[] b = new byte[length];
        for (int i = 0; i < count; i++)
        {
            for (int j = 0; j < b.length; j++)
            {
                b[j] = (byte)(32 + r.nextInt(94));
            }
            strings.add(new String(b, StandardCharsets.US_ASCII));
        }

        return strings;
    }

    private static Object idle(final InvocationOnMock invocation)
    {
        yieldAndCheckInterrupt();
        return null;
    }
}