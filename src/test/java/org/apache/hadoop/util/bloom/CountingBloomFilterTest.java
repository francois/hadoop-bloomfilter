package org.apache.hadoop.util.bloom;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.junit.Assert.*;

public class CountingBloomFilterTest {
    private static final Charset utf8 = Charset.forName("UTF-8");

    @Test
    public void testBasics() {
        CountingBloomFilter cbf = new CountingBloomFilter(100, 2, Hash.MURMUR_HASH);
        final Key key = new Key("abc".getBytes(utf8));
        cbf.add(key);
        assertTrue("key present after insertion", cbf.membershipTest(key));

        cbf.delete(key);
        assertFalse("key gone after deletion", cbf.membershipTest(key));
    }
}
