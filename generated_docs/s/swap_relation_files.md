# swap_relation_files

## Location
src/backend/commands/cluster.c: 1061 - 1437

## Overview
Swaps the physical files of two relations while maintaining their logical identities, handling both regular and mapped relations along with associated TOAST tables and indexes.

## Definition


## Detailed Description
The `swap_relation_files` function is a critical component of PostgreSQL's table reorganization operations that swaps the physical storage identities of two relations while preserving their logical identities. This allows the system to atomically replace an old table with a reorganized version.

Key operations performed:
1. **Physical Identity Swap**: Exchanges relfilenumber, reltablespace, relam, and relpersistence between relations
2. **Mapped Relations**: For system catalogs, updates the relation mapping instead of pg_class entries
3. **TOAST Handling**: Supports both content-based and link-based TOAST table swapping
4. **Statistics Transfer**: Exchanges table statistics (relpages, reltuples, relallvisible)
5. **Dependency Management**: Updates access method and TOAST table dependencies as needed
6. **Freeze Information**: Sets new freeze transaction ID and MultiXact cutoff values

The function handles the complexity of PostgreSQL's dual approach to relation storage (regular vs mapped relations) and ensures atomicity of the swap operation.

## Parameters / Member Variables
- `r1`: OID of the first relation to swap (typically the target relation)
- `r2`: OID of the second relation to swap (typically the temporary relation)
- `target_is_pg_class`: Boolean indicating if we're swapping pg_class itself (special case)
- `swap_toast_by_content`: Boolean controlling TOAST table swapping method (content vs links)
- `is_internal`: Boolean indicating if this is an internal operation (affects hooks)
- `frozenXid`: Transaction ID to set as the new freeze cutoff for r1
- `cutoffMulti`: MultiXact ID to set as the new cutoff for r1
- `mapped_tables`: Output array to collect OIDs of mapped tables involved

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopy1: Retrieves pg_class tuples for both relations
  - RelationMapOidToFilenumber/RelationMapUpdateMap: Handles mapped relation file swapping
  - relation_open/relation_close: Opens relations to update subtransaction tracking
  - RelationAssumeNewRelfilelocator: Marks relation as having new storage in subtransaction
  - CatalogOpenIndexes/CatalogTupleUpdateWithInfo: Updates pg_class entries with index maintenance
  - changeDependencyFor: Updates access method dependencies when they differ
  - deleteDependencyRecordsFor/recordDependencyOn: Manages TOAST table dependencies
  - toast_get_valid_index: Retrieves TOAST table indexes for content swapping
  - InvokeObjectPostAlterHookArg: Fires post-alter hooks for both relations
- Called from (representative examples):
  - swap_relation_files: Recursive calls for TOAST tables and indexes
  - finish_heap_swap: Main heap swapping coordination

## Notes and Other Information
- Handles both regular relations (via pg_class updates) and mapped relations (via relation mapper)
- For mapped relations, enforces restrictions on tablespace, persistence, and access method changes
- Supports recursive swapping of TOAST tables and their indexes when using content-based swapping
- Updates subtransaction tracking to ensure proper cleanup on rollback
- Special handling for pg_class swaps to avoid updating data that will be discarded
- Maintains dependency information for TOAST tables when using link-based swapping
- Critical for the atomicity of table reorganization operations like CLUSTER and ALTER TABLE
- The function is recursive - it calls itself to handle TOAST table and index swapping