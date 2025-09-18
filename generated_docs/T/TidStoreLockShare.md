# TidStoreLockShare

## Location
src/backend/access/common/tidstore.c: 305 - 311

## Overview
Acquires a shared lock on a TidStore to enable safe concurrent read access in multi-process scenarios.

## Definition


## Detailed Description
TidStoreLockShare is a locking function that acquires a shared (read) lock on a TidStore object when it is configured for shared memory usage across multiple processes. The function checks if the TidStore is shared using the TidStoreIsShared() macro and only performs locking if necessary. For shared TidStores, it calls the internally generated shared_ts_lock_share() function from the radix tree implementation. This enables multiple processes to safely read from the TidStore concurrently while preventing conflicts with exclusive write operations.

## Parameters / Member Variables
- `ts`: Pointer to the TidStore object to lock for shared access

## Dependencies
- Functions called/Symbols referenced:
  - TidStoreIsShared (macro)
  - shared_ts_lock_share (radix tree generated function)
- Called from (representative examples):
  - check_set_block_offsets (in test_tidstore.c)

## Notes and Other Information
- Only performs locking operations on shared TidStores (when ts->area != NULL)
- For local TidStores, this function is effectively a no-op
- Must be paired with TidStoreUnlock() to release the lock
- Multiple shared locks can be held simultaneously, allowing concurrent readers
- Part of PostgreSQL's TidStore locking protocol for parallel processing scenarios