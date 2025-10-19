# sts_reinitialize

## Location
[src/backend/utils/sort/sharedtuplestore.c:234-252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L234-L252)

## Overview
Prepares a shared tuplestore for rescanning by resetting read positions for all participants to the beginning.

## Definition
void sts_reinitialize(SharedTuplestoreAccessor *accessor)

## Detailed Description
This function prepares a shared tuplestore for rescanning by resetting the shared read head for all participants files back to the beginning (page 0). It must be called by only one participant before any participant can begin a new parallel scan using sts_begin_parallel_scan(). The function ensures that all participants start reading from the first page of their respective tuple files, effectively allowing the tuplestore to be rescanned from the beginning.

The function is designed to be called between scan cycles and must not be called concurrently with an active scan. Synchronization to avoid concurrent access is the callers responsibility.

## Parameters / Member Variables
- `accessor`: A pointer to the SharedTuplestoreAccessor structure that provides access to the shared tuplestore to be reinitialized

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md) (structure type)
- Called from (representative examples):
  - Functions that need to rescan shared tuplestores (context depends on usage)

## Notes and Other Information
- Only one participant should call this function to avoid race conditions
- Must not be called concurrently with active scanning operations
- After calling this function, all participants may call sts_begin_parallel_scan() to start a new scan
- The function resets read_page to 0 for all participants, ensuring they start from the beginning
- Synchronization to prevent concurrent access during reinitialization is the callers responsibility
- This is part of the shared tuplestore parallel scanning infrastructure used in PostgreSQLs parallel query execution

## Simplified Source
```c
void
sts_reinitialize(SharedTuplestoreAccessor *accessor)
{
    // Reset read position for all participants to start from beginning
    for (int i = 0; i < accessor->sts->nparticipants; ++i)
    {
        accessor->sts->participants[i].read_page = 0;
    }
}
```