# TuplesortInstrumentation

## Location
[src/include/utils/tuplesort.h:110-115](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/tuplesort.h#L110-L115)

## Overview
TuplesortInstrumentation is a structure used to collect and report statistics about tuple sorting operations, including the sorting method used and space consumption metrics.

## Definition
```c
typedef struct TuplesortInstrumentation
{
    TuplesortMethod sortMethod;   /* sort algorithm used */
    TuplesortSpaceType spaceType; /* type of space spaceUsed represents */
    int64       spaceUsed;        /* space consumption, in kB */
} TuplesortInstrumentation;
```

## Detailed Description
TuplesortInstrumentation serves as a data collection structure for gathering performance and resource usage statistics from tuplesort operations. This structure is designed to be safe for shared memory usage (contains no pointers) and is used extensively in PostgreSQL execution nodes that perform sorting operations.

The structure captures three key metrics: the sorting algorithm that was ultimately used, the type of space being measured (disk or memory), and the actual space consumption. This information is crucial for query performance analysis, EXPLAIN output, and understanding the behavior of sorting operations in complex queries.

The parallel sort infrastructure relies on having a zero TuplesortMethod to indicate that a worker never performed any sorting work, enabling proper aggregation of statistics across multiple worker processes.

## Parameters / Member Variables
- `sortMethod`: Enumerated value indicating which sorting algorithm was used (heapsort, quicksort, external sort, or external merge). Can be ORed together to represent multiple methods used by different workers