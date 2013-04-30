package org.apache.hadoop.util.bloom;

import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

public class StableBloomFilterTest {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Test
    public void testBasics() {
        final Key abc = new Key("abc".getBytes(UTF8));
        final Key def = new Key("def".getBytes(UTF8));
        final Key ghi = new Key("ghi".getBytes(UTF8));
        final Key jkl = new Key("jkl".getBytes(UTF8));
        final Key mno = new Key("mno".getBytes(UTF8));
        final Key pqr = new Key("pqr".getBytes(UTF8));

        StableBloomFilter sbf = new StableBloomFilter(32, 2, Hash.MURMUR_HASH);
        sbf.add(abc);
        sbf.add(def);
        sbf.add(ghi);
        sbf.add(jkl);
        // sbf.add(mno);
        // sbf.add(pqr);
        assertTrue(sbf.membershipTest(abc));
        assertTrue(sbf.membershipTest(def));
        assertTrue(sbf.membershipTest(ghi));
        assertTrue(sbf.membershipTest(jkl));
        assertFalse(sbf.membershipTest(mno));
        assertFalse(sbf.membershipTest(pqr));
    }

    @Test
    public void testEmpiricalErrorRates() {

        final StableBloomFilter sbf = new StableBloomFilter(Integer.MAX_VALUE, 4, Hash.MURMUR_HASH);
        final int numValues = 2 << 23; // adding approximatively 16 million entries
        System.out.printf("number of values = %8d\n", numValues);
        final long startInsert = System.nanoTime();
        for (int i = 1; i <= numValues; i++) {
            sbf.add(new Key(String.valueOf(i).getBytes(UTF8)));
        }

        final long startMembership = System.nanoTime();
        int present = 0, absent = 0;
        for (int i = 1; i <= numValues; i++) {
            if (sbf.membershipTest(new Key(String.valueOf(i).getBytes(UTF8)))) {
                present += 1;
            } else {
                absent += 1;
            }
        }

        final long endMembership = System.nanoTime();

        System.out.println("--- should all be present");
        System.out.printf("present = %8d\n", present);
        System.out.printf("absent = %8d\n", absent);
        System.out.printf("error = %8.3f%%\n", 100.0 * absent / numValues);
        assertTrue("testing for present values does not return enough presents", present > numValues * (1.0 - 0.000025)); // 25 errors per 1,000,000 entries

        final long startNonMembership = System.nanoTime();
        present = absent = 0;
        for (int i = numValues + 1; i <= 2 * numValues; i++) {
            if (sbf.membershipTest(new Key(String.valueOf(i).getBytes(UTF8)))) {
                present += 1;
            } else {
                absent += 1;
            }
        }
        final long endNonMembership = System.nanoTime();

        System.out.println("--- should all be absent");
        System.out.printf("present = %8d\n", present);
        System.out.printf("absent = %8d\n", absent);
        System.out.printf("error = %8.3f%%\n", 100.0 * present / numValues);

        System.out.printf("insertion      = %.1fs\n", TimeUnit.MILLISECONDS.convert(startMembership - startInsert, TimeUnit.NANOSECONDS) / 1000.0);
        System.out.printf("membership     = %.1fs\n", TimeUnit.MILLISECONDS.convert(endMembership - startMembership, TimeUnit.NANOSECONDS) / 1000.0);
        System.out.printf("non-membership = %.1fs\n", TimeUnit.MILLISECONDS.convert(endNonMembership - startNonMembership, TimeUnit.NANOSECONDS) / 1000.0);

        assertTrue("testing for absent values does not return enough absents", absent >= numValues * (1.0 - 0.00025)); // 25 errors per 100,000 entries
    }
}
