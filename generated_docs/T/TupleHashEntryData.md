# TupleHashEntryData

## Location
src/include/nodes/execnodes.h: 802 - 808

## Overview
TupleHashEntryData represents an individual entry within a TupleHashTable, storing a tuple along with its associated metadata for hash-based operations.

## Definition
```c
typedef struct TupleHashEntryData
{
    MinimalTuple firstTuple;    /* copy of first tuple in this group */
    void       *additional;     /* user data */
    uint32      status;         /* hash status */
    uint32      hash;           /* hash value (cached) */
} TupleHashEntryData;
```

## Detailed Description
TupleHashEntryData serves as the fundamental storage unit within PostgreSQLs tuple hash tables. Each entry represents a single tuple or a group of identical tuples (in the case of grouping operations) along with essential metadata for efficient hash table operations.

The structure is designed to minimize memory overhead while providing all necessary information for hash table management. The cached hash value eliminates the need for recomputation during table operations, while the status field tracks the entrys state within the hash table lifecycle.

This structure is particularly important in aggregation operations where multiple tuples may be grouped under a single entry, with the firstTuple serving as the representative tuple for the group and additional pointing to aggregate state or other operation-specific data.

## Parameters / Member Variables
- `firstTuple`: MinimalTuple representing the first (or representative) tuple in this hash entry, used for key comparison and grouping operations
- `additional`: Generic pointer to user-defined data, typically used to store aggregation state, additional tuples, or other operation-specific information
- `status`: Hash table status flags indicating the entrys current state (active, deleted, etc.)
- `hash`: Cached hash value of the tuples key columns, stored to avoid recomputation during lookups and table operations

## Dependencies
- Functions called/Symbols referenced:
  - MinimalTuple (tuple representation)
- Called from (representative examples):
  - BuildTupleHashTableExt (src/backend/executor/execGrouping.c:166)
  - [LookupTupleHashEntry_internal](../L/LookupTupleHashEntry_internal.md) (src/backend/executor/execGrouping.c:497)
  - [hash_agg_entry_size](../h/hash_agg_entry_size.md) (src/backend/executor/nodeAgg.c:1716)
  - [setop_fill_hash_table](../s/setop_fill_hash_table.md) (src/backend/executor/nodeSetOp.c:367)
  - [setop_retrieve_hash_table](../s/setop_retrieve_hash_table.md) (src/backend/executor/nodeSetOp.c:427)

## Notes and Other Information
- Used as SH_ELEMENT_TYPE in the underlying hash table implementation, indicating it serves as the element type for the specialized hash table
- The firstTuple field uses MinimalTuple format for memory efficiency in hash storage scenarios
- The additional field provides flexibility for different use cases: in aggregation it points to aggregate state, in set operations it may be NULL or point to duplicate tracking information
- Hash value caching significantly improves performance during hash table resizing and repeated lookups
- Status field management is crucial for proper hash table entry lifecycle and memory management
- Memory layout is optimized for cache efficiency during hash table traversals