# logicalrep_rel_open

## Location
src/backend/replication/logical/relation.c: 327 - 472

## Overview
Opens the local relation associated with a remote relation in logical replication, establishing and maintaining the mapping between local and remote relation attributes while handling cache invalidation and relation validation.

## Definition
```c
LogicalRepRelMapEntry *logicalrep_rel_open(LogicalRepRelId remoteid, LOCKMODE lockmode)
```

## Detailed Description
This function is the primary entry point for opening local relations that correspond to remote relations in logical replication. It manages a complex process of relation lookup, validation, attribute mapping, and cache maintenance. The function handles both initial opens and re-opens after cache invalidation due to DDL operations.

The function first searches for an existing entry in the logical replication relation map. If the entry is marked as valid, it attempts to open the relation by OID to check if invalidation has occurred. If the entry is invalid or becomes invalidated, it rebuilds all derived data by re-opening the relation by name, reconstructing attribute mappings, validating replica identity compatibility, and finding appropriate indexes for replication operations.

Key responsibilities include ensuring no relation reference leaks, handling relation renames/drops gracefully, building attribute mappings between local and remote relations, validating that all replicated columns are present locally, checking replica identity compatibility for UPDATE/DELETE operations, and maintaining subscription relation state information.

## Parameters / Member Variables
- `remoteid`: LogicalRepRelId identifying the remote relation to be opened
- `lockmode`: LOCKMODE specifying the type of lock to acquire on the local relation

## Dependencies
- Functions called/Symbols referenced:
  - logicalrep_relmap_init: Initializes the logical replication relation map if not already done
  - hash_search: Searches for existing entries in the relation map hash table
  - try_table_open: Attempts to open a table by OID, returning NULL if it fails
  - table_open: Opens a table by OID with the assumption it exists
  - table_close: Closes a previously opened table relation
  - free_attrmap: Deallocates attribute mapping structures
  - make_attrmap: Creates new attribute mapping structures
  - RangeVarGetRelid: Resolves relation name to OID
  - makeRangeVar: Creates a RangeVar structure from namespace and relation names
  - CheckSubscriptionRelkind: Validates that the relation kind is supported for subscriptions
  - logicalrep_rel_att_by_name: Finds remote attribute number by name
  - logicalrep_report_missing_attrs: Reports errors for missing local attributes
  - logicalrep_rel_mark_updatable: Determines if relation supports UPDATE/DELETE operations
  - FindLogicalRepLocalIndex: Finds appropriate local index for replication operations
  - GetSubscriptionRelState: Retrieves current subscription relation state
  - bms_add_range: Adds range of members to bitmap set
  - bms_del_member: Removes member from bitmap set
  - bms_free: Deallocates bitmap set
- Called from (representative examples):
  - copy_table: Initial table synchronization operations
  - apply_handle_insert: Processing INSERT operations from logical replication stream
  - apply_handle_update: Processing UPDATE operations from logical replication stream
  - apply_handle_delete: Processing DELETE operations from logical replication stream
  - apply_handle_truncate: Processing TRUNCATE operations from logical replication stream

## Notes and Other Information
- This function is critical for logical replication performance as it manages relation cache entries
- Handles graceful recovery from DDL operations that invalidate relation cache entries
- Implements a two-phase approach: first try by OID (fast path), then by name (rebuild path)
- The function prevents relation reference leaks by ensuring only one open relation per entry
- Memory management is handled carefully with proper context switching for persistent allocations
- Supports both regular tables and partitioned tables through the same interface
- The attribute mapping built by this function is essential for translating between local and remote tuple formats
- Relation state management integrates with the subscription system to track synchronization progress