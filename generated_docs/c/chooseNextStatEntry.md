# chooseNextStatEntry

## Location
[src/backend/utils/adt/tsvector_op.c:2381-2412](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/tsvector_op.c#L2381-L2412)

## Overview
A recursive static function that implements a sampling strategy to process a subset of words from a TSVector for statistical analysis, using a divide-and-conquer approach to select representative entries.

## Definition

```c
static void
chooseNextStatEntry(MemoryContext persistentContext, TSVectorStat *stat, TSVector txt,
					uint32 low, uint32 high, uint32 offset)
```
## Detailed Description
The  function implements a strategic sampling algorithm for PostgreSQL's text search statistics system (). Instead of processing every word in a TSVector (which could be computationally expensive for large documents), this function selects a representative subset of words using a recursive divide-and-conquer approach.

The function operates on a range defined by  and  parameters, calculating a middle point and selecting two positions around this middle point for processing. It then recursively processes the left and right halves of the range, creating a sampling pattern that ensures good coverage across the entire TSVector while limiting the total number of words processed.

The sampling strategy helps balance statistical accuracy with performance, particularly important when dealing with large text documents or when building statistics across many documents. The function includes bounds checking to ensure that selected positions are valid within the TSVector.

## Parameters / Member Variables
- `persistentContext`: Memory context for allocating StatEntry nodes that persist across function calls
- `*stat`: Pointer to the TSVectorStat structure containing the statistics tree and configuration
- `txt`: The TSVector being sampled for statistical analysis
- `low`: Lower bound of the current range being processed
- `high`: Upper bound of the current range being processed
- `offset`: Base offset to adjust position calculations relative to the TSVector's word array
## Dependencies
- Functions called/Symbols referenced:
  - : Called to process selected word entries and update statistics
  - : Recursive calls to process left and right sub-ranges
  - : Text search vector type containing words to be sampled
  - : Structure containing the statistics tree and metadata
- Called from (representative examples):
  - : Recursive self-calls for divide-and-conquer processing
  - : Main aggregation function that initiates the sampling process

## Notes and Other Information
- This is a static function, accessible only within the same source file
- Implements a recursive divide-and-conquer sampling algorithm
- The sampling strategy helps manage performance when processing large TSVectors
- Uses bit shifting () for efficient division by 2 in middle point calculations
- Includes bounds checking to prevent accessing invalid TSVector positions
- The offset parameter allows for processing sub-sections of TSVectors
- Part of PostgreSQL's text search statistics optimization system
- The recursive nature creates a balanced sampling across the entire word range
- Located in 
- Critical for maintaining reasonable performance in ts_stat operations on large text corpora

## Simplified Source

```c
static void chooseNextStatEntry(MemoryContext persistentContext, TSVectorStat *stat,
                               TSVector txt, uint32 low, uint32 high, uint32 offset) {
    uint32 pos;
    uint32 middle = (low + high) >> 1;

    // Sample two positions around the middle point
    pos = (low + middle) >> 1;
    if (low != middle && pos >= offset && pos - offset < txt->size)
        insertStatEntry(persistentContext, stat, txt, pos - offset);

    pos = (high + middle + 1) >> 1;
    if (middle + 1 != high && pos >= offset && pos - offset < txt->size)
        insertStatEntry(persistentContext, stat, txt, pos - offset);

    // Recursively process left and right halves
    if (low != middle)
        chooseNextStatEntry(persistentContext, stat, txt, low, middle, offset);
    if (high != middle + 1)
        chooseNextStatEntry(persistentContext, stat, txt, middle + 1, high, offset);
}
```