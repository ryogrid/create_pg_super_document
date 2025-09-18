# logicalrep_partition_open

## Location
[src/backend/replication/logical/relation.c:602-744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L602-L744)

## Overview
Opens and manages cached logical replication relation map entries for table partitions, handling attribute mapping differences between partitions and their root tables.

## Definition
```c
LogicalRepRelMapEntry *logicalrep_partition_open(LogicalRepRelMapEntry *root,
                                                 Relation partrel, AttrMap *map)
```

## Detailed Description
This function creates or retrieves a cached relation map entry for a table partition in logical replication. It handles the complex task of mapping partition attributes to remote relation attributes, which can differ from the root table's mapping due to partition-specific column arrangements. The function performs deep copying of relation metadata to ensure independence from the root table's entry, which may be freed or rebuilt.

The function first checks for an existing cached entry and updates the local relation pointer if found. For new entries, it copies the remote relation metadata from the root entry and creates a new attribute map specific to the partition. The attribute mapping logic handles the conversion from tuple routing's 1-based attribute numbers to logical replication's 0-based numbering system.

## Parameters / Member Variables
- `root`: Pointer to the root table's LogicalRepRelMapEntry containing base relation information
- `partrel`: Relation pointer for the specific partition being opened
- `map`: AttrMap containing attribute number mappings from partition to root table (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [logicalrep_partmap_init](logicalrep_partmap_init.md)
  - [hash_search](../h/hash_search.md)
  - RelationGetRelid
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - memset
  - [free_attrmap](../f/free_attrmap.md)
  - [pstrdup](../p/pstrdup.md)
  - [palloc](../p/palloc.md)
  - [bms_copy](../b/bms_copy.md)
  - [make_attrmap](../m/make_attrmap.md)
  - memcpy
  - logicalrep_rel_mark_updatable
  - [FindLogicalRepLocalIndex](../F/FindLogicalRepLocalIndex.md)
- Types referenced:
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md)
  - [LogicalRepPartMapEntry](../L/LogicalRepPartMapEntry.md)
  - [LogicalRepRelation](../L/LogicalRepRelation.md)
  - [AttrMap](../A/AttrMap.md)
  - [MemoryContext](../M/MemoryContext.md)
  - AttrNumber
- Hash operation flags:
  - HASH_ENTER
- Called from (representative examples):
  - [apply_handle_tuple_routing](../a/apply_handle_tuple_routing.md)

## Notes and Other Information
- No corresponding close function exists as the caller handles closing the component relation
- Always updates entry->localrel with the latest partition Relation pointer to handle potential relation cache clearing
- Performs deep copying of remote relation metadata to ensure entry independence
- Handles attribute number conversion between tuple routing (1-based) and logical replication (0-based) systems
- Memory operations are performed in LogicalRepPartMapContext for proper memory management
- Uses FindLogicalRepLocalIndex in the original memory context to avoid memory leaks
- Sets localrelvalid to true to indicate the entry is current and valid
- The function is designed to handle both new partition entries and updates to existing cached entries