/**
 *
 * Copyright (c) 2013, François Beausoleil francois@teksol.info
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *  - Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the distribution.
 *  - Neither the name of the University Catholique de Louvain - UCL
 *    nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util.bloom;

import java.util.Random;

/**
 * Implements a <i>stable Bloom filter</i>, as defined by Fan et al. in a ToN
 * 2000 paper.
 * <p/>
 * A stable Bloom filter is an improvement over a counting Bloom filter as it
 * allows old elements to expire.
 * <p/>
 *
 * @see Filter The general behavior of a filter
 * @see <a href="http://webdocs.cs.ualberta.ca/~drafiei/papers/DupDet06Sigmod.pdf">Approximately Detecting Duplicates for Streaming Data using Stable Bloom Filters</a>
 *
 * @author François Beausoleil <francois@teksol.info>
 */
public class StableBloomFilter extends CountingBloomFilter {
    /**
     * The random number generator to use for decrementing P buckets
     */
    private final Random random;

    /**
     * The number of buckets in our filter
     */
    private final int numBuckets;

    /**
     * The number of decrement operations we do on each add
     */
    private final int p;

    /**
     * Constructor
     *
     * @param vectorSize The vector size of <i>this</i> filter.
     * @param nbHash     The number of hash function to consider.
     * @param hashType   type of the hashing function (see
     *                   {@link org.apache.hadoop.util.hash.Hash}).
     */
    public StableBloomFilter(int vectorSize, int nbHash, int hashType) {
        this(vectorSize, nbHash, hashType, new Random());
    }

    /**
     * Constructor
     *
     * @param vectorSize The vector size of <i>this</i> filter.
     * @param nbHash     The number of hash function to consider.
     * @param hashType   type of the hashing function (see
     * @param random     a random number generator for decrementing P buckets.
     *                   {@link org.apache.hadoop.util.hash.Hash}).
     */
    public StableBloomFilter(int vectorSize, int nbHash, int hashType, Random random) {
        super(vectorSize, nbHash, hashType);
        this.random = random;
        this.p = 2 * nbHash;
        numBuckets = 8 * CountingBloomFilter.buckets2words(vectorSize); /* 8 nibbles per long */
    }

  @Override
  protected void inc(int[] h) {
    int n = random.nextInt(numBuckets);
    for (int i = 0; i < p; i++) {
      int wordNum = n >> 4;          // div 16
      int bucketShift = (n & 0x0f) << 2;  // (mod 16) * 4

      long bucketMask = 15L << bucketShift;

      if ((buckets[wordNum] & bucketMask) > 0) {
        // decrement by 1
        long newValue = (((buckets[wordNum] & bucketMask) >>> bucketShift) - 1) << bucketShift;
        long bitsToKeepMask = 0xffffffffffffffffl & ~bucketMask;
        long existingValue = buckets[wordNum] & bitsToKeepMask;
        buckets[wordNum] = existingValue | newValue;
        bucketMask += 1;
      }

      // as in the paper, only do one random operation per add
      n -= 1;
      if (n < 0) n = numBuckets;
    }

    for (int i = 0; i < nbHash; i++) {
        // find the bucket
        int wordNum = h[i] >> 4;          // div 16
        int bucketShift = (h[i] & 0x0f) << 2;  // (mod 16) * 4

        long bucketMask = 15L << bucketShift;

        // set to max
        buckets[wordNum] = (buckets[wordNum] | bucketMask);
    }
  }
}
