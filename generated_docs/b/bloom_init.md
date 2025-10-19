# bloom_init

## Location
[src/backend/access/brin/brin_bloom.c:310-369](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/brin/brin_bloom.c#L310-L369)

## Overview
Initializes and allocates memory for a new Bloom filter with optimal parameters calculated for the expected number of distinct values and desired false positive rate.

## Definition

```c
static BloomFilter *
bloom_init(int ndistinct, double false_positive_rate)
```
## Detailed Description
This function creates a new Bloom filter by first calculating the optimal parameters using bloom_filter_size(), then allocating memory for the filter structure. The function includes safeguards to prevent creating filters that are too large to store on a database page. The filter is allocated with palloc0() so it starts as all zeros, making it highly compressible initially. As values are added, the filter becomes more random and less compressible.

The function validates input parameters and rejects filters that exceed BloomMaxFilterSize to prevent runtime failures when the filter becomes less compressible after adding values.

## Parameters / Member Variables
- `ndistinct`: Expected number of distinct values that will be stored in the filter (must be > 0)
- `false_positive_rate`: Desired false positive rate as a probability (must be between 0 and 1)
## Dependencies
- Functions called/Symbols referenced:
  - [bloom_filter_size](bloom_filter_size.md): Calculates optimal filter parameters
  - [BloomFilter](../B/BloomFilter.md): The bloom filter data structure
  - BloomMaxFilterSize: Maximum allowed filter size constant
  - [palloc0](../p/palloc0.md): PostgreSQL memory allocation function (initializes to zero)
  - SET_VARSIZE: Macro to set the size of a varlena structure
  - Assert: Assertion macro for parameter validation
  - elog: PostgreSQL logging/error function
- Called from (representative examples):
  - [brin_bloom_add_value](brin_bloom_add_value.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the brin_bloom.c file
- Returns a pointer to a newly allocated BloomFilter structure
- The filter is stored as a varlena (variable-length) structure for efficient storage
- Includes comprehensive size checking to prevent page overflow issues
- Initial zero-filled state makes the filter highly compressible until values are added
- Contains detailed comments explaining the rationale for size restrictions
- Located in src/backend/access/brin/brin_bloom.c:310-369

## Simplified Source

```c
static BloomFilter *
bloom_init(int ndistinct, double false_positive_rate)
{
    int nbits, nbytes, nhashes;

    // Validate inputs
    Assert(ndistinct > 0);
    Assert(false_positive_rate > 0 && false_positive_rate < 1);

    // Calculate optimal filter parameters
    bloom_filter_size(ndistinct, false_positive_rate, &nbytes, &nbits, &nhashes);

    // Check if filter size is acceptable for page storage
    if (nbytes > BloomMaxFilterSize)
        elog(ERROR, "bloom filter too large (%d > %zu)", nbytes, BloomMaxFilterSize);

    // Allocate filter structure with data space
    Size len = offsetof(BloomFilter, data) + nbytes;
    BloomFilter *filter = (BloomFilter *) palloc0(len);

    // Initialize filter metadata
    filter->flags = 0;
    filter->nhashes = nhashes;
    filter->nbits = nbits;
    SET_VARSIZE(filter, len);

    return filter;
}
```