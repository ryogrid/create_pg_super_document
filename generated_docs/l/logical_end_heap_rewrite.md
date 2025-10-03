# logical_end_heap_rewrite

## Location
[src/backend/access/heap/rewriteheap.c:905-934](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/rewriteheap.c#L905-L934)

## Overview
Finalizes logical rewrite operations by flushing any remaining mappings to disk and synchronizing all mapping files to ensure durability of the logical rewrite mapping data.

## Definition
```c
static void
logical_end_heap_rewrite(RewriteState state)
```

## Detailed Description
This function performs the cleanup and finalization phase of logical heap rewriting. It ensures that all logical rewrite mapping data is properly persisted to disk before the rewrite operation completes. The function:

1. Checks if logical rewriting is enabled for this operation
2. Flushes any remaining in-memory mappings to disk by calling logical_heap_rewrite_flush_mappings()
3. Iterates through all mapping files that were created during the rewrite
4. Synchronizes each mapping file to disk using fsync() to ensure durability
5. Closes all mapping file descriptors

The function is critical for ensuring crash safety and consistency of logical replication. Without proper fsync() of the mapping files, a system crash could leave the logical decoding infrastructure in an inconsistent state where some mappings are lost.

## Parameters / Member Variables
- `state`: RewriteState structure containing the logical rewrite context, including the hash table of mapping files and the logical rewrite flag

## Dependencies
- Functions called/Symbols referenced:
  - [logical_heap_rewrite_flush_mappings](logical_heap_rewrite_flush_mappings.md) (flushes remaining mappings)
  - [hash_seq_init](../h/hash_seq_init.md), hash_seq_search (hash table iteration)
  - [FileSync](../F/FileSync.md) (synchronizes file to disk)
  - [FileClose](../F/FileClose.md) (closes file descriptors)
  - [data_sync_elevel](../d/data_sync_elevel.md) (error level for sync operations)
- Called from (representative examples):
  - [end_heap_rewrite](../e/end_heap_rewrite.md)

## Notes and Other Information
- This is a static function internal to the rewriteheap.c module
- The function is the counterpart to logical_begin_heap_rewrite, handling cleanup operations
- Memory context cleanup automatically handles deallocation of the hash table and related structures
- The fsync() operations are essential for crash safety and ensuring logical replication consistency
- Part of the larger heap rewrite infrastructure that maintains logical decoding correctness during DDL operations
- Uses appropriate error levels for synchronization failures via data_sync_elevel()

## Simplified Source

```c
static void
logical_end_heap_rewrite(RewriteState state)
{
    HASH_SEQ_STATUS seq_status;
    RewriteMappingFile *src;

    // Exit early if no logical rewrite was performed
    if (!state->rs_logical_rewrite)
        return;

    // Flush any remaining in-memory mappings to disk
    if (state->rs_num_rewrite_mappings > 0)
        logical_heap_rewrite_flush_mappings(state);

    // Iterate through all mapping files and sync them to disk
    hash_seq_init(&seq_status, state->rs_logical_mappings);
    while ((src = (RewriteMappingFile *) hash_seq_search(&seq_status)) != NULL) {
        // Synchronize file to disk for crash safety
        if (FileSync(src->vfd, WAIT_EVENT_LOGICAL_REWRITE_SYNC) != 0)
            ereport(data_sync_elevel(ERROR),
                    (errmsg("could not fsync file \"%s\": %m", src->path)));

        // Close the file descriptor
        FileClose(src->vfd);
    }

    // Memory context cleanup will handle the rest of the cleanup
}
```