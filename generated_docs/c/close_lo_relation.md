# close_lo_relation

## Location
[src/backend/storage/large_object/inv_api.c:98-130](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/large_object/inv_api.c#L98-L130)

## Overview
Cleans up the PostgreSQL large object relation and index references at the end of a main transaction, optionally closing them if the transaction is being committed.

## Definition

```c
void
close_lo_relation(bool isCommit)
```
## Detailed Description
This function handles the cleanup of large object relation references at transaction end. It takes a commit/abort flag to determine the appropriate cleanup behavior. When committing, it explicitly closes both the large object heap relation and its index using the appropriate close functions, switching resource ownership temporarily to TopTransactionResourceOwner for proper cleanup. When aborting, it relies on the abort cleanup mechanism to handle the closing, only resetting the global pointers to NULL. This approach optimizes cleanup by avoiding unnecessary work during transaction aborts.

## Parameters / Member Variables
- `isCommit`: Boolean flag indicating whether the transaction is being committed (true) or aborted (false)
## Dependencies
- Functions called/Symbols referenced:
  - [index_close](../i/index_close.md) (to close the large object index)
  - [table_close](../t/table_close.md) (to close the large object heap relation)
  - [ResourceOwner](../R/ResourceOwner.md) (for resource management)
- Called from (representative examples):
  - [AtEOXact_LargeObject](../A/AtEOXact_LargeObject.md) (transaction end processing)

## Notes and Other Information
- Function has public visibility (not static)
- Only performs explicit closing operations during transaction commit
- During transaction abort, relies on automatic cleanup mechanisms
- Resets global variables lo_heap_r and lo_index_r to NULL in both cases
- Uses NoLock when closing relations since locks are released during transaction end
- Resource ownership is temporarily switched to TopTransactionResourceOwner during cleanup

## Simplified Source

```c
void
close_lo_relation(bool isCommit)
{
    if (lo_heap_r || lo_index_r)
    {
        // Only close explicitly if committing; abort cleanup handles it otherwise
        if (isCommit)
        {
            ResourceOwner currentOwner;

            currentOwner = CurrentResourceOwner;
            CurrentResourceOwner = TopTransactionResourceOwner;

            if (lo_index_r)
                index_close(lo_index_r, NoLock);
            if (lo_heap_r)
                table_close(lo_heap_r, NoLock);

            CurrentResourceOwner = currentOwner;
        }
        lo_heap_r = NULL;
        lo_index_r = NULL;
    }
}
```