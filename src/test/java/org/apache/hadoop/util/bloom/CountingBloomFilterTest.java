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


    private static final int LARGE_VECTOR_SIZE = 2 << 29;

    @Test
    public void testDoesNotUseMoreThanOneGigabyteOfMemory() {
        assertTrue(LARGE_VECTOR_SIZE > 0);

        final int expectedMemorySize = LARGE_VECTOR_SIZE / 8 /* bits per byte */ * 4 /* bits per counter */;
        final int bytesPerQword = 8;
        final int numberOfQwords = CountingBloomFilter.buckets2words(LARGE_VECTOR_SIZE);
        final int bytesForCBF = bytesPerQword * numberOfQwords;

        System.out.println("bytes = " + bytesForCBF);
        System.out.println("items = " + LARGE_VECTOR_SIZE);
        assertEquals(expectedMemorySize, bytesForCBF);
        assertTrue(bytesForCBF < /* 1 GiB */ (1048576L * 1024));
    }
}
