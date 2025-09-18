# BootStrapMultiXact

## Location
src/backend/access/transam/multixact.c: 2026 - 2065

## Overview
BootStrapMultiXact initializes the MultiXact subsystem during PostgreSQL installation by creating and zeroing the initial segments for both offset and member logs.

## Definition
```c
void BootStrapMultiXact(void)
```

## Detailed Description
This function must be called exactly ONCE during system installation to set up the MultiXact subsystem. It performs the critical initialization of MultiXact storage by creating the first pages of both the offsets log and members log, ensuring they are properly written to disk. The function assumes that the MultiXacts directories have already been created by initdb and that MultiXactShmemInit has been called to initialize shared memory structures.

The function operates in two phases:
1. Initializes the offset log by acquiring the appropriate lock, creating and zeroing the first page, and ensuring it's written to disk
2. Initializes the member log using the same process

This bootstrap process is essential for the MultiXact system to function correctly, as it establishes the foundational storage structures needed for tracking multiple transaction IDs that share locks on the same tuple.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruGetBankLock
  - LWLockAcquire
  - ZeroMultiXactOffsetPage
  - SimpleLruWritePage
  - LWLockRelease
  - ZeroMultiXactMemberPage
- Global variables accessed:
  - MultiXactOffsetCtl
  - MultiXactMemberCtl
- Called from:
  - BootStrapXLOG

## Notes and Other Information
- This function must be called only once during database cluster initialization
- The function assumes MultiXacts directories exist and shared memory is initialized
- Uses exclusive locks to ensure atomic initialization of both offset and member logs
- Critical for proper MultiXact subsystem functionality in PostgreSQL's MVCC implementation