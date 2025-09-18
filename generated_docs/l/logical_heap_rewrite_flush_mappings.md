# logical_heap_rewrite_flush_mappings

## Location
[src/backend/access/heap/rewriteheap.c:807-904](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L807-L904)

## Overview
Flushes all accumulated logical rewrite mappings from memory to disk and logs them via WAL, ensuring that tuple location mappings needed for logical decoding are persistently stored during heap rewrite operations.

## Definition
```c
static void
logical_heap_rewrite_flush_mappings(RewriteState state)
```

## Detailed Description
This function processes all accumulated logical rewrite mappings stored in memory and flushes them to persistent storage. For each transaction that has mappings, it:

1. Collects all mapping entries for the transaction
2. Writes the mappings directly to a disk file using FileWrite
3. Creates a WAL record (XLOG_HEAP2_REWRITE) containing the mapping information
4. Cleans up the in-memory mapping structures

The function follows a non-standard WAL pattern where data is written to disk BEFORE the WAL record is inserted, rather than the typical approach of writing WAL first. This is necessary because the mapping files are not stored in shared_buffers, so the normal checkpoint interlocking mechanism doesn't apply.

The function iterates through each RewriteMappingFile in the hash table, processes all mappings for that transaction, writes them to the corresponding file, and logs the operation via WAL for replication support.

## Parameters / Member Variables
- `state`: RewriteState structure containing the logical rewrite context, including the hash table of mapping files and accumulated mapping count

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md), hash_seq_search (hash table iteration)
  - [dclist_count](../d/dclist_count.md), dclist_foreach_modify, dclist_container, dclist_delete_from (doubly-linked list operations)
  - [FileWrite](../F/FileWrite.md) (file I/O)
  - [XLogBeginInsert](../X/XLogBeginInsert.md), XLogRegisterData, XLogInsert (WAL logging)
  - RelationGetRelid (relation utilities)
- Called from (representative examples):
  - [logical_end_heap_rewrite](logical_end_heap_rewrite.md)
  - [logical_rewrite_log_mapping](logical_rewrite_log_mapping.md)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- Uses a non-standard WAL pattern: writes data to disk first, then logs to WAL (documented deviation from usual practices)
- Handles both shared and non-shared relations appropriately by setting the database OID
- Cleans up memory structures as it processes them to avoid memory leaks
- Critical for maintaining logical replication consistency during DDL operations that rewrite heap files
- The function ensures all mappings are flushed when called, resetting rs_num_rewrite_mappings to 0