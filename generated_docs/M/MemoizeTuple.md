# MemoizeTuple

## Location
[src/backend/executor/nodeMemoize.c:94-99](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeMemoize.c#L94-L99)

## Overview
MemoizeTuple is a data structure that stores an individually cached tuple in PostgreSQL's memoization system, forming a linked list for entries with the same parameter values.

## Definition

```c
typedef struct MemoizeTuple
{
	MinimalTuple mintuple;		/* Cached tuple */
	struct MemoizeTuple *next;	/* The next tuple with the same parameter
								 * values or NULL if it's the last one */
} MemoizeTuple;
```
## Detailed Description
MemoizeTuple is a fundamental component of PostgreSQL's memoization (caching) mechanism in the execution engine. It represents a single cached tuple result and is designed to form a linked list structure when multiple tuples share the same parameter values. This allows the memoization system to efficiently store and retrieve multiple result tuples for a given set of input parameters, which is essential for optimizing repeated queries with identical conditions.

The structure is lightweight and uses MinimalTuple format for space efficiency, which is a compact representation of tuples that excludes certain metadata present in full HeapTuples.

## Parameters / Member Variables
- `mintuple`: A MinimalTuple containing the actual cached tuple data in a space-efficient format
- `*next`: Pointer to the next MemoizeTuple in the linked list, or NULL if this is the last tuple for the current parameter set
## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple
- Called from (representative examples):
  - [entry_purge_tuples](../e/entry_purge_tuples.md)
  - [cache_store_tuple](../c/cache_store_tuple.md)
  - [ExecEndMemoize](../E/ExecEndMemoize.md)
  - [ExecEstimateCacheEntryOverheadBytes](../E/ExecEstimateCacheEntryOverheadBytes.md)

## Notes and Other Information
- Used in conjunction with MemoizeEntry and MemoizeKey to implement the complete memoization cache
- The linked list structure allows multiple result tuples to be associated with a single cache key
- Memory management is handled by the memoization subsystem, including cleanup during plan termination
- Size calculations include this structure in cache memory overhead estimates