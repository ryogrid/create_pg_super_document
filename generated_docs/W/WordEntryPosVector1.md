# WordEntryPosVector1

## Location
[src/include/tsearch/ts_type.h:76-78](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/tsearch/ts_type.h#L76-L78)

## Overview
WordEntryPosVector1 is a specialized variant of WordEntryPosVector with a fixed-size array containing exactly one position entry, optimized for the common case of single-position words.

## Definition
```c
typedef struct
{
    uint16      npos;
    WordEntryPos pos[1];
} WordEntryPosVector1;
```

## Detailed Description
WordEntryPosVector1 is a memory-optimized version of WordEntryPosVector designed for the common case where a word appears at exactly one position in a document. Instead of using a flexible array member, it declares a fixed-size array with exactly one element.

This structure serves as a performance optimization and type safety mechanism:
- It avoids the overhead of flexible array allocation for single-position words
- Provides compile-time size information for stack allocation scenarios
- Ensures type safety when code specifically expects single-position vectors
- Maintains the same binary layout as WordEntryPosVector when npos=1

The structure is particularly useful in ranking calculations where individual position entries are processed, allowing algorithms to work with a concrete type rather than variable-length structures.

## Parameters / Member Variables
- `npos`: Number of position entries (should always be 1 for this structure type)
- `pos`: Fixed-size array containing exactly one WordEntryPos value with encoded position and weight

## Dependencies
- Functions called/Symbols referenced:
  - WordEntryPos (position/weight data type)
- Used by (representative examples):
  - calc_rank_and (AND ranking calculations with single positions)
  - calc_rank_or (OR ranking calculations with single positions)

## Notes and Other Information
- This is a specialized optimization for the common single-position case
- Binary compatible with WordEntryPosVector when npos=1
- Provides better type safety and stack allocation efficiency than the flexible array version
- Used primarily in ranking algorithms where single-position processing is common
- The npos field should always contain the value 1 for consistency
- Enables compiler optimizations that aren't possible with flexible array members