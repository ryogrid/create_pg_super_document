# logical_rewrite_log_mapping

## Location
[src/backend/access/heap/rewriteheap.c:935-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L935-L998)

## Overview
Logs a single tuple location mapping from old to new position for a specific transaction during heap rewrite operations, maintaining the mapping data needed for logical decoding consistency.

## Definition
```c
static void
logical_rewrite_log_mapping(RewriteState state, TransactionId xid,
                            LogicalRewriteMappingData *map)
```

## Detailed Description
This function records a single tuple mapping entry that tracks how a tuple's location changes during a heap rewrite operation. The mapping is essential for logical decoding to maintain correct visibility information (cmin/cmax) for catalog tuples after the rewrite.

For each transaction ID, the function:

1. Looks up or creates a RewriteMappingFile entry in the hash table
2. If this is the first mapping for the transaction, creates a new mapping file on disk with a unique filename incorporating database ID, relation ID, LSN, and transaction IDs
3. Adds the mapping entry to the per-transaction mapping list
4. Implements an optimization that flushes all accumulated mappings to disk when the in-memory count reaches 1000 entries

The mapping files are created in the pg_logical/mappings directory with a specific naming convention that includes transaction information to ensure uniqueness and enable proper cleanup of aborted transaction data.

## Parameters / Member Variables
- `state`: RewriteState structure containing the rewrite context and mapping infrastructure
- `xid`: Transaction ID for which the mapping applies (the transaction that modified the original tuple)
- `map`: LogicalRewriteMappingData structure containing the old and new tuple location information

## Dependencies
- Functions called/Symbols referenced:
  - RelationGetRelid (gets relation identifier)
  - [hash_search](../h/hash_search.md) (hash table operations)
  - [GetCurrentTransactionId](../G/GetCurrentTransactionId.md) (transaction system)
  - [dclist_init](../d/dclist_init.md), dclist_push_tail (doubly-linked list operations)
  - [PathNameOpenFile](../P/PathNameOpenFile.md) (file I/O)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (memory management)
  - [logical_heap_rewrite_flush_mappings](logical_heap_rewrite_flush_mappings.md) (mapping flush operations)
- Called from (representative examples):
  - [logical_rewrite_heap_tuple](logical_rewrite_heap_tuple.md) (multiple calls)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- Creates mapping files with a specific naming format that includes database, relation, LSN, and transaction identifiers
- Uses lazy file creation - mapping files are only created when the first mapping for a transaction is recorded
- Implements batching optimization by flushing mappings when 1000 entries accumulate in memory
- The mapping files are created in exclusive mode (O_CREAT | O_EXCL) to prevent conflicts
- Critical component of PostgreSQL's logical replication system that ensures consistency during DDL operations
- Handles both shared and non-shared relations by appropriately setting the database OID in the filename

## Simplified Source

```c
static void
logical_rewrite_log_mapping(RewriteState state, TransactionId xid,
                            LogicalRewriteMappingData *map)
{
    RewriteMappingFile *src;
    RewriteMappingDataEntry *pmap;
    Oid relid;
    bool found;

    relid = RelationGetRelid(state->rs_old_rel);

    // Look for existing mapping file for this transaction
    src = hash_search(state->rs_logical_mappings, &xid, HASH_ENTER, &found);

    // Create new mapping file if this is first mapping for this xid
    if (!found) {
        char path[MAXPGPATH];
        Oid dboid;

        // Set database OID (InvalidOid for shared relations)
        if (state->rs_old_rel->rd_rel->relisshared)
            dboid = InvalidOid;
        else
            dboid = MyDatabaseId;

        // Generate unique filename with db, rel, LSN, and transaction IDs
        snprintf(path, MAXPGPATH,
                 "pg_logical/mappings/" LOGICAL_REWRITE_FORMAT,
                 dboid, relid,
                 LSN_FORMAT_ARGS(state->rs_begin_lsn),
                 xid, GetCurrentTransactionId());

        // Initialize new mapping file structure
        dclist_init(&src->mappings);
        src->off = 0;
        memcpy(src->path, path, sizeof(path));

        // Create the mapping file
        src->vfd = PathNameOpenFile(path, O_CREAT | O_EXCL | O_WRONLY | PG_BINARY);
        if (src->vfd < 0)
            ereport(ERROR, (errmsg("could not create file \"%s\": %m", path)));
    }

    // Add this mapping to the transaction's mapping list
    pmap = MemoryContextAlloc(state->rs_cxt, sizeof(RewriteMappingDataEntry));
    memcpy(&pmap->map, map, sizeof(LogicalRewriteMappingData));
    dclist_push_tail(&src->mappings, &pmap->node);
    state->rs_num_rewrite_mappings++;

    // Flush mappings to disk if we have too many in memory
    if (state->rs_num_rewrite_mappings >= 1000) {
        logical_heap_rewrite_flush_mappings(state);
    }
}
```