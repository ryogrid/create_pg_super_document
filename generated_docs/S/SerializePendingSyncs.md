# SerializePendingSyncs

## Location
[src/backend/catalog/storage.c:584-634](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/storage.c#L584-L634)

## Overview
SerializePendingSyncs serializes pending sync operations for parallel workers, allowing them to inherit the sync requirements from the main process.

## Definition

```c
void
SerializePendingSyncs(Size maxSize, char *startAddress)
```
## Detailed Description
This function prepares pending synchronization operations for parallel workers by serializing the active RelFileLocator entries into a memory region. It creates a temporary hash table to collect all active relation file locators from the pending syncs, filters out any that are marked for deletion at commit, and writes the remaining locators to the provided memory address. The serialized data can then be used by parallel workers to inherit sync responsibilities from the main process.

The function handles the case where there are no pending syncs by simply terminating the list with a zero-filled RelFileLocator entry.

## Parameters / Member Variables
- `maxSize`: Maximum size available for serialization (unused in current implementation)
- `startAddress`: Memory address where serialized RelFileLocator entries will be written

## Dependencies
- Functions called/Symbols referenced:
  - [hash_create](../h/hash_create.md)
  - [hash_get_num_entries](../h/hash_get_num_entries.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [hash_search](../h/hash_search.md)
  - [hash_destroy](../h/hash_destroy.md)
  - MemSet
- Called from (representative examples):
  - [InitializeParallelDSM](../I/InitializeParallelDSM.md)

## Notes and Other Information
- The function uses a temporary hash table to avoid duplicates and efficiently manage the collection of active relation file locators
- Deleted relations (marked with atCommit flag) are filtered out during serialization
- The serialized list is null-terminated with a zero-filled RelFileLocator entry
- This is part of PostgreSQL's parallel query infrastructure for sharing pending I/O operations between processes