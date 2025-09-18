# XLogPrefetcherFree

## Location
src/backend/access/transam/xlogprefetcher.c: 392 - 402

## Overview
Deallocates an XLogPrefetcher instance and releases all associated resources including streaming read queues and hash tables.

## Definition
```c
void XLogPrefetcherFree(XLogPrefetcher *prefetcher)
```

## Detailed Description
XLogPrefetcherFree performs complete cleanup of a WAL prefetcher instance, ensuring all dynamically allocated resources are properly released. The function follows a specific cleanup order: first releasing the LSN read queue used for streaming operations, then destroying the hash table used for relation filters, and finally freeing the main prefetcher structure itself.

This cleanup function is the counterpart to XLogPrefetcherAllocate and ensures no memory leaks occur when WAL recovery operations complete or are terminated.

## Parameters / Member Variables
- `prefetcher`: Pointer to the XLogPrefetcher instance to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [lrq_free](../l/lrq_free.md) (releases LSN read queue resources)
  - [hash_destroy](../h/hash_destroy.md) (destroys the filter hash table)
  - [pfree](../p/pfree.md) (releases the main prefetcher structure)
- Called from (representative examples):
  - [ShutdownWalRecovery](../S/ShutdownWalRecovery.md) (during WAL recovery shutdown)

## Notes and Other Information
- The cleanup order is important: streaming_read resources are freed first, followed by the filter_table, and finally the prefetcher structure itself
- The function safely handles cases where streaming_read might be NULL (lazy allocation means it may never have been created)
- Part of the resource management pair with XLogPrefetcherAllocate, ensuring proper lifecycle management of prefetcher instances
- Called during WAL recovery shutdown to prevent resource leaks in long-running recovery processes