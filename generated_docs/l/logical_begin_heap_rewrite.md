# logical_begin_heap_rewrite

## Location
[src/backend/access/heap/rewriteheap.c:759-806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L759-L806)

## Overview
Initializes logical rewrite support during heap rewriting operations, setting up the necessary infrastructure to track tuple mappings for logical decoding when the table is accessible during logical replication.

## Definition

```c
static void
logical_begin_heap_rewrite(RewriteState state)
```
## Detailed Description
This function prepares for logging logical mappings during a heap rewrite operation when necessary for logical decoding support. It determines whether the rewritten table needs logical rewrite tracking by checking if the relation is accessible during logical decoding and if there are active logical replication slots. If logical rewrite support is needed, it initializes the necessary data structures including a hash table to track rewrite mappings organized by transaction ID.

The function implements an optimization where it only enables logical rewrite tracking if:
1. The relation is accessible in logical decoding
2. There are active logical replication slots (logical_xmin is valid)

When enabled, it sets up the RewriteState with logical rewrite parameters and creates a hash table to store rewrite mappings that will be used to maintain the correctness of (relfilelocator,ctid) => (cmin, cmax) mappings during logical decoding.

## Parameters / Member Variables
- `state`: RewriteState structure that maintains the state of the heap rewrite operation, including logical rewrite settings and mapping structures
## Dependencies
- Functions called/Symbols referenced:
  - RelationIsAccessibleInLogicalDecoding
  - [ProcArrayGetReplicationSlotXmin](../P/ProcArrayGetReplicationSlotXmin.md)
  - [GetXLogInsertRecPtr](../G/GetXLogInsertRecPtr.md)
  - [hash_create](../h/hash_create.md)
- Called from (representative examples):
  - [begin_heap_rewrite](../b/begin_heap_rewrite.md)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- The function uses lazy initialization - it only sets up logical rewrite infrastructure when actually needed
- Creates a hash table with transaction IDs as keys and RewriteMappingFile structures as values
- The logical rewrite system is crucial for maintaining consistency in logical replication during DDL operations that rewrite heap files
- Part of PostgreSQL's logical replication infrastructure that ensures catalog tuple visibility information (cmin/cmax) remains correct after heap rewrites

## Simplified Source

```c
static void
logical_begin_heap_rewrite(RewriteState state)
{
    HASHCTL hash_ctl;
    TransactionId logical_xmin;

    // Check if logical rewrite tracking is needed for this relation
    state->rs_logical_rewrite =
        RelationIsAccessibleInLogicalDecoding(state->rs_old_rel);

    if (!state->rs_logical_rewrite)
        return;

    // Get minimum XID from active logical replication slots
    ProcArrayGetReplicationSlotXmin(NULL, &logical_xmin);

    // If no logical slots are active, no rewrite tracking needed
    if (logical_xmin == InvalidTransactionId) {
        state->rs_logical_rewrite = false;
        return;
    }

    // Initialize logical rewrite state
    state->rs_logical_xmin = logical_xmin;
    state->rs_begin_lsn = GetXLogInsertRecPtr();
    state->rs_num_rewrite_mappings = 0;

    // Set up hash table for tracking mappings by transaction ID
    hash_ctl.keysize = sizeof(TransactionId);
    hash_ctl.entrysize = sizeof(RewriteMappingFile);
    hash_ctl.hcxt = state->rs_cxt;

    state->rs_logical_mappings =
        hash_create("Logical rewrite mapping",
                    128,  // initial size
                    &hash_ctl,
                    HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}
```