# FindTupleHashEntry

## Location
src/backend/executor/execGrouping.c: 391 - 423

## Overview
Searches for a hashtable entry matching the given tuple without creating new entries, supporting cross-type comparisons with custom hash and equality functions.

## Definition
```c
TupleHashEntry FindTupleHashEntry(TupleHashTable hashtable, TupleTableSlot *slot, ExprState *eqcomp, FmgrInfo *hashfunctions)
```

## Detailed Description
FindTupleHashEntry is a specialized lookup function that differs from LookupTupleHashEntry in its support for cross-type comparisons. While LookupTupleHashEntry requires the input tuple to be of the same type as the hash table entries, FindTupleHashEntry allows comparison between tuples of different types by accepting custom hash and equality functions.

This function is particularly useful in scenarios where you need to search for entries using a tuple with a different structure than what's stored in the hash table, such as when searching for a full tuple using only a subset of its columns, or when comparing tuples with different but compatible data types.

The function only performs lookups and never creates new entries, making it purely a search operation. It uses the provided hash functions and equality comparison expressions to perform the lookup operation.

## Parameters / Member Variables
- `hashtable`: The TupleHashTable to search in
- `slot`: TupleTableSlot containing the tuple to search for (may be different type than table entries)
- `eqcomp`: ExprState for the equality comparison function to use for matching
- `hashfunctions`: FmgrInfo array containing the hash functions to use for the input tuple

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - tuplehash_lookup
- Called from (representative examples):
  - ExecHashSubPlan (nodeSubplan.c:160)
  - Referenced in executor.h header for external usage

## Notes and Other Information
- Never creates new entries, always returns NULL if no match is found
- Supports cross-type comparisons by allowing custom hash and equality functions
- Uses NULL as a flag to indicate that the inputslot should be referenced during lookup
- Essential for subplan execution where different tuple formats may need to be compared
- Memory context switching ensures hash computations occur in appropriate temporary context
- The custom hash and equality functions must be compatible with the tuple types being compared
- Provides flexibility for complex query execution scenarios requiring heterogeneous tuple comparisons