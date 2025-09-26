# TupleHashEntry

## Location
[src/include/nodes/execnodes.h:799-799](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/nodes/execnodes.h#L799-L799)

## Overview
TupleHashEntry is a pointer typedef to TupleHashEntryData struct, representing entries in PostgreSQL's in-memory tuple hash tables used for grouping, aggregation, and other hash-based operations.

## Definition

```c
typedef struct TupleHashEntryData *TupleHashEntry;
```
## Detailed Description
TupleHashEntry represents individual entries in PostgreSQL's all-in-memory tuple hash tables, which are fundamental data structures used for operations like GROUP BY, aggregation, hash joins, and duplicate elimination. Each entry stores a representative tuple from a group along with cached hash values for efficient lookups and user-defined additional data. The hash table system supports both same-type and cross-data-type hashing operations, making it versatile for various executor node implementations.

## Parameters / Member Variables
- : MinimalTuple containing a copy of the first tuple encountered for this hash group
- : Generic pointer to user-defined data associated with this hash entry (e.g., aggregate state)
- : Hash status flags indicating the state of this entry in the hash table
- hash: hash table empty: Cached hash value computed from the tuple's key columns for efficient comparisons

## Dependencies
- Functions called/Symbols referenced:
  - [TupleHashEntryData](TupleHashEntryData.md) (the actual struct definition)
  - MinimalTuple (tuple representation type)
- Called from (representative examples):
  - [LookupTupleHashEntry](../L/LookupTupleHashEntry.md) (src/backend/executor/execGrouping.c:307)
  - [lookup_hash_entries](../l/lookup_hash_entries.md) (src/backend/executor/nodeAgg.c:2106)
  - [findPartialMatch](../f/findPartialMatch.md) (src/backend/executor/nodeSubplan.c:750)

## Notes and Other Information
TupleHashEntry is central to PostgreSQL's hash-based execution strategies. The hash table system it participates in supports sophisticated features like cross-data-type hashing for operations involving different but compatible data types (e.g., int4 = int8 comparisons). The firstTuple member uses MinimalTuple format for memory efficiency, while the additional pointer allows executor nodes to attach node-specific state (like aggregate accumulators). The cached hash value significantly improves performance by avoiding repeated hash computations during table operations.