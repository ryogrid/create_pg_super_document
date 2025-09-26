# logicalrep_relmap_free_entry

## Location
[src/backend/replication/logical/relation.c:132-163](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/relation.c#L132-L163)

## Overview
Properly deallocates all memory associated with a logical replication relation map cache entry, including nested structures and attribute mappings.

## Definition
static void logicalrep_relmap_free_entry(LogicalRepRelMapEntry *entry)

## Detailed Description
This function performs comprehensive cleanup of a LogicalRepRelMapEntry structure, ensuring that all dynamically allocated memory within the entry is properly freed to prevent memory leaks. The function handles the complex nested structure of relation metadata including relation names, attribute information, and attribute mappings.

The cleanup process involves several steps:
1. Freeing the namespace and relation name strings
2. Iterating through all attributes to free individual attribute name strings
3. Freeing the arrays containing attribute names and types
4. Releasing the bitmap set used for attribute keys
5. Deallocating the attribute map if one exists

This function is crucial for maintaining proper memory management in the logical replication subsystem, especially when cache entries need to be updated or removed.

## Parameters / Member Variables
- : Pointer to the LogicalRepRelMapEntry structure to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [bms_free](../b/bms_free.md) (bitmap set deallocation)
  - [free_attrmap](../f/free_attrmap.md) (attribute map cleanup utility)
  - [LogicalRepRelMapEntry](../L/LogicalRepRelMapEntry.md) (entry structure type)
  - [LogicalRepRelation](../L/LogicalRepRelation.md) (relation metadata structure)
- Called from (representative examples):
  - [logicalrep_relmap_update](logicalrep_relmap_update.md)
  - [logicalrep_partmap_reset_relmap](logicalrep_partmap_reset_relmap.md)

## Notes and Other Information
- This is a static function, only accessible within the relation.c file
- Follows PostgreSQL's memory management conventions using pfree()
- Handles variable-length arrays and optional components safely
- Uses proper null checking before freeing optional components like attrmap
- Integrates with the related processed symbol 'free_attrmap' for complete cleanup
- Essential for preventing memory leaks in long-running logical replication processes
- Part of the logical replication cache management infrastructure