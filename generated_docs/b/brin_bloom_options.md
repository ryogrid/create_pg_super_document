# brin_bloom_options

## Location
[src/backend/access/brin/brin_bloom.c:747-776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L747-L776)

## Overview
Configures the reloptions (relation options) for BRIN Bloom indexes, allowing users to specify parameters that control the behavior and performance characteristics of bloom filters in BRIN indexes.

## Definition
```c
Datum brin_bloom_options(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL reloption handler that defines and initializes the configurable parameters for BRIN Bloom indexes. It sets up two key parameters that control the bloom filter behavior: the expected number of distinct values per page range and the desired false positive rate. The function uses PostgreSQL's local reloption framework to register these parameters with their default values, valid ranges, and storage offsets within the BloomOptions structure.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - [init_local_reloptions](../i/init_local_reloptions.md)
  - [add_local_real_reloption](../a/add_local_real_reloption.md)
  - PG_GETARG_POINTER
  - PG_RETURN_VOID
- Data types used:
  - [local_relopts](../l/local_relopts.md)
  - [BloomOptions](../B/BloomOptions.md)
- Constants referenced:
  - BLOOM_DEFAULT_NDISTINCT_PER_RANGE
  - BLOOM_DEFAULT_FALSE_POSITIVE_RATE
  - BLOOM_MIN_FALSE_POSITIVE_RATE
  - BLOOM_MAX_FALSE_POSITIVE_RATE
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's reloption framework)

## Notes and Other Information
- This function registers two reloptions:
  1. `n_distinct_per_range`: Expected number of distinct items in a BRIN page range (default from BLOOM_DEFAULT_NDISTINCT_PER_RANGE, range: -1.0 to INT_MAX)
  2. `false_positive_rate`: Desired false-positive rate for bloom filters (default from BLOOM_DEFAULT_FALSE_POSITIVE_RATE, constrained by MIN/MAX constants)
- The function is part of PostgreSQL's extensible index AM (Access Method) framework
- These options directly affect bloom filter sizing and accuracy in BRIN indexes

## Simplified Source

```c
Datum brin_bloom_options(PG_FUNCTION_ARGS) {
    local_relopts *relopts = (local_relopts *) PG_GETARG_POINTER(0);

    // Initialize reloptions structure for BloomOptions
    init_local_reloptions(relopts, sizeof(BloomOptions));

    // Add n_distinct_per_range option
    add_local_real_reloption(relopts, "n_distinct_per_range",
                            "number of distinct items expected in a BRIN page range",
                            BLOOM_DEFAULT_NDISTINCT_PER_RANGE,
                            -1.0, INT_MAX,
                            offsetof(BloomOptions, nDistinctPerRange));

    // Add false_positive_rate option
    add_local_real_reloption(relopts, "false_positive_rate",
                            "desired false-positive rate for the bloom filters",
                            BLOOM_DEFAULT_FALSE_POSITIVE_RATE,
                            BLOOM_MIN_FALSE_POSITIVE_RATE,
                            BLOOM_MAX_FALSE_POSITIVE_RATE,
                            offsetof(BloomOptions, falsePositiveRate));

    PG_RETURN_VOID();
}
```