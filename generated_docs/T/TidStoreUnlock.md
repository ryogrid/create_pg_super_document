# TidStoreUnlock

## Location
[src/backend/access/common/tidstore.c:312-327](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/common/tidstore.c#L312-L327)

## Overview
Releases a previously acquired lock on a TidStore, allowing other processes to acquire locks on the shared data structure.

## Definition
```c
void TidStoreUnlock(TidStore *ts)
```

## Detailed Description
TidStoreUnlock releases any lock previously acquired on a TidStore object. The function checks if the TidStore is configured for shared memory usage and only performs unlocking operations if necessary. For shared TidStores, it calls the internally generated shared_ts_unlock() function from the radix tree implementation. This function must be called after TidStoreLockShare() or TidStoreLockExclusive() to properly release locks and allow other processes to access the TidStore. For local (non-shared) TidStores, this function is effectively a no-op since no locking is required.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to unlock

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - shared_ts_unlock (radix tree generated function)
- Called from (representative examples):
  - [do_set_block_offsets](../d/do_set_block_offsets.md) (in test_tidstore.c)
  - [check_set_block_offsets](../c/check_set_block_offsets.md) (in test_tidstore.c)

## Notes and Other Information
- Only performs unlocking operations on shared TidStores (when ts->area != NULL)
- For local TidStores, this function is effectively a no-op
- Must be called to release locks acquired by TidStoreLockShare() or TidStoreLockExclusive()
- Failure to call this function after acquiring a lock can lead to deadlocks in multi-process scenarios
- Part of PostgreSQL's TidStore locking protocol for parallel processing