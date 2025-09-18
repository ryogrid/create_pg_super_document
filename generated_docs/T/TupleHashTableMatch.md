# TupleHashTableMatch

## Location
src/backend/executor/execGrouping.c: 535 - 558

## Overview
Hash table comparison function that determines whether two tuples with the same hash value are actually equal by performing a detailed tuple comparison.

## Definition
```c
static int TupleHashTableMatch(struct tuplehash_hash *tb, const MinimalTuple tuple1, const MinimalTuple tuple2)
```

## Detailed Description
This function serves as the equality comparison callback for PostgreSQL's tuple hash table implementation. It is called by the underlying hash table infrastructure when hash collisions occur to determine if two tuples are actually equal. The function expects tuple1 to be an actual table entry and tuple2 to be NULL (representing the input slot being searched for). It sets up the comparison context by storing the tuples in appropriate slots and then uses the hash table's equality function to perform the comparison. The function returns 0 for equal tuples and non-zero for different tuples.

## Parameters / Member Variables
- `tb`: Hash table structure containing the private data and configuration
- `tuple1`: The first tuple to compare (actual table entry, must not be NULL)
- `tuple2`: The second tuple to compare (expected to be NULL, represents input slot)

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple
  - TupleHashTable
  - ExecStoreMinimalTuple
  - ExecQualAndReset
- Called from (representative examples):
  - SH_EQUAL (via macro definition)

## Notes and Other Information
- This is a static function used as a callback by the hash table implementation
- The function assumes a specific calling convention where tuple1 is the stored entry and tuple2 is NULL
- Uses the hash table's expression context and equality function for comparison
- The input slot must be placed as the inner tuple and table slot as outer tuple for cross-type comparisons
- Returns the negation of ExecQualAndReset result (0 for match, non-zero for no match)
- Part of PostgreSQL's simplehash.h-based hash table infrastructure
- Critical for correctness of hash-based grouping and joining operations