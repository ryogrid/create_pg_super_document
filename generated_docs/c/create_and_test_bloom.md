# create_and_test_bloom

## Location
[src/test/modules/test_bloomfilter/test_bloomfilter.c:72-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/test/modules/test_bloomfilter/test_bloomfilter.c#L72-L112)

## Overview
Creates and performs comprehensive testing of a Bloom filter with specified parameters, measuring its false positive rate and reporting performance metrics.

## Definition
```c
static void create_and_test_bloom(int power, int64 nelements, int callerseed)
```

## Detailed Description
This static function is the core testing routine for PostgreSQL's Bloom filter implementation. It orchestrates a complete lifecycle test by creating a Bloom filter, populating it with dummy data, measuring its false positive rate, and reporting detailed performance statistics.

The function calculates the working memory size based on the power parameter (bloom_work_mem = (1L << power) / 8L / 1024L KB), handles random seed generation for reproducible tests, and creates a Bloom filter optimized for the specified number of elements. After populating the filter with dummy strings via `populate_with_dummy_strings`, it measures false positives using `nfalsepos_for_missing_strings` and reports comprehensive statistics including the seed used, false positive count and percentage, and the proportion of bits set in the filter.

The function includes intelligent alerting - it issues a WARNING if the false positive rate exceeds the threshold (1%), otherwise it logs at DEBUG1 level. This helps identify potential issues with filter performance during testing.

## Parameters / Member Variables
- `power`: Determines the working memory size for the Bloom filter (memory = 2^power / 8 / 1024 KB)
- `nelements`: The number of elements to add to the filter and use for false positive testing
- `callerseed`: Seed for random number generation; if negative, a random seed is generated automatically

## Dependencies
- Functions called/Symbols referenced:
  - [bloom_create](../b/bloom_create.md) (creates the Bloom filter with specified parameters)
  - [populate_with_dummy_strings](../p/populate_with_dummy_strings.md) (adds test data to the filter)
  - [nfalsepos_for_missing_strings](../n/nfalsepos_for_missing_strings.md) (measures false positive rate)
  - [bloom_prop_bits_set](../b/bloom_prop_bits_set.md) (calculates proportion of set bits)
  - [bloom_free](../b/bloom_free.md) (releases filter memory)
  - [pg_prng_int32p](../p/pg_prng_int32p.md) (generates random seed when needed)
  - elog/ereport (logging and reporting functions)
- Called from (representative examples):
  - [test_bloomfilter](../t/test_bloomfilter.md)

## Notes and Other Information
- Uses `FPOSITIVE_THRESHOLD` (0.01 or 1%) as the warning threshold for false positive rates
- Generates comprehensive performance reports including seed, false positive statistics, and bit utilization
- Ensures reproducible testing by allowing fixed seeds via the callerseed parameter
- Memory calculation converts bit-based power parameter to KB for bloom_create
- Automatically cleans up allocated memory via bloom_free
- This is a static function, only accessible within the test_bloomfilter.c file
- The function provides both performance validation and debugging information for Bloom filter behavior