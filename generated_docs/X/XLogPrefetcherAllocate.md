# XLogPrefetcherAllocate

## Location
src/backend/access/transam/xlogprefetcher.c: 362 - 391

## Overview
Creates and initializes a new XLogPrefetcher instance that manages WAL record prefetching during recovery operations.

## Definition
```c
XLogPrefetcher *XLogPrefetcherAllocate(XLogReaderState *reader)
```

## Detailed Description
XLogPrefetcherAllocate allocates and initializes a complete WAL prefetcher system. The function sets up the core data structures needed for tracking and filtering blocks during WAL replay, including a hash table for maintaining per-relation filters and a doubly-linked list queue for managing filter lifecycle.

The function initializes shared statistics counters to zero and sets up the prefetcher for lazy allocation of the streaming read infrastructure. The reconfigure_count is set to trigger immediate reconfiguration on first use, ensuring optimal performance parameters are established when prefetching begins.

## Parameters / Member Variables
- `reader`: XLogReaderState pointer that provides the WAL reading context and will be associated with this prefetcher

## Dependencies
- Functions called/Symbols referenced:
  - palloc0 (memory allocation)
  - hash_create (hash table creation)
  - dlist_init (doubly-linked list initialization)
- Called from (representative examples):
  - InitWalRecovery (during WAL recovery initialization)

## Notes and Other Information
- Uses a static HASHCTL structure to configure the filter hash table with RelFileLocator keys and XLogPrefetcherFilter entries
- The hash table is sized to 1024 initial buckets and uses HASH_ELEM | HASH_BLOBS flags for efficient binary key comparison
- Shared statistics (wal_distance, block_distance, io_depth) are reset to zero, providing a clean state for new recovery sessions
- The streaming_read component is allocated lazily on first actual use, optimizing memory usage when prefetching might not be needed
- The prefetcher maintains a filter_queue using PostgreSQL's doubly-linked list implementation for efficient filter management