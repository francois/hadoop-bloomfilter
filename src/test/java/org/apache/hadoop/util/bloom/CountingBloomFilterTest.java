package org.apache.hadoop.util.bloom;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.*;

import static org.junit.Assert.*;

public class CountingBloomFilterTest {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    public void testBasics() {
        final CountingBloomFilter cbf = new CountingBloomFilter(100, 2, Hash.MURMUR_HASH);
        final Key key = new Key("abc".getBytes(UTF8));
        cbf.add(key);
        assertTrue("key present after insertion", cbf.membershipTest(key));

        cbf.delete(key);
        assertFalse("key gone after deletion", cbf.membershipTest(key));
    }


    private static final Random RANDOM = new Random();
    private static final int LARGE_VECTOR_SIZE = 2 << 29;
    private static final int NBHASH = 5;

    @Test
    public void testDoesNotUseMoreThanOneGigabyteOfMemory() {
        assertTrue(LARGE_VECTOR_SIZE > 0);

        final int numberOfQwords = CountingBloomFilter.buckets2words(LARGE_VECTOR_SIZE);
        final int bytesPerQword = 8;
        final int bytesForCBF = bytesPerQword * numberOfQwords;

        System.out.println("bytes = " + bytesForCBF);
        System.out.println("items = " + LARGE_VECTOR_SIZE);
        assertTrue(bytesForCBF < /* 1 GiB */ (1048576L * 1024));
    }

    @Test
    public void testManyItems() {
        final CountingBloomFilter cbf = new CountingBloomFilter(LARGE_VECTOR_SIZE, NBHASH, Hash.MURMUR_HASH);
        final List<String> services = new ArrayList<String>();
        services.add("Twitter");
        services.add("Twitter");
        services.add("Facebook");
        services.add("Facebook");
        services.add("Facebook");

        long count = LARGE_VECTOR_SIZE;
        while (count > 0) {
            count -= 1;
            if (((LARGE_VECTOR_SIZE - count) % 250000) == 0) System.out.println("count = " + count);

            long serviceId = RANDOM.nextLong();
            while (serviceId < 0) serviceId = RANDOM.nextLong();
            String serviceName = services.get(RANDOM.nextInt(services.size()));

            final Key key = new Key((serviceName + serviceId).getBytes(UTF8));
            cbf.add(key);
            assertTrue(cbf.membershipTest(key));
        }
    }
}
