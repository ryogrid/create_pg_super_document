# BootStrapCLOG

## Location
src/backend/access/transam/clog.c: 833 - 859

## Overview
Creates the initial CLOG (Commit Log) segment during PostgreSQL system installation, establishing the foundation for transaction status tracking.

## Definition


## Detailed Description
BootStrapCLOG is a critical initialization function that must be called exactly once during PostgreSQL system installation (via initdb). It creates the very first CLOG segment that will serve as the foundation for all transaction status tracking in the database.

The function performs the following essential operations:

1. **Lock acquisition**: Acquires an exclusive lock on the CLOG bank 0 to ensure exclusive access during initialization
2. **Page creation**: Creates and zeros the first CLOG page (page 0) using ZeroCLOGPage()
3. **Page persistence**: Writes the newly created page to disk using SimpleLruWritePage() to ensure durability
4. **State verification**: Asserts that the page is no longer marked as dirty after being written
5. **Lock release**: Releases the exclusive lock

This function assumes that the CLOG directory structure has already been created by initdb and that CLOGShmemInit() has been called to initialize the shared memory structures.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md) (obtains lock for SLRU bank)
  - LWLockAcquire/LWLockRelease (lock management)
  - [ZeroCLOGPage](../Z/ZeroCLOGPage.md) (creates and zeros a CLOG page)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md) (writes page to disk)
- Types referenced:
  - [LWLock](../L/LWLock.md) (lightweight lock type)
- Global variables:
  - XactCtl (CLOG SLRU control structure)
- Called from:
  - [BootStrapXLOG](BootStrapXLOG.md) (during WAL/transaction log bootstrap)

## Notes and Other Information
- This function is called exactly once during the lifetime of a PostgreSQL installation
- It is part of the bootstrap process that occurs during initdb
- The function creates CLOG page 0, which will contain transaction status information for the first batch of transaction IDs
- Proper locking ensures that this critical initialization is atomic and thread-safe
- The function assumes that the CLOG directory (pg_xact) already exists
- After this function completes, the CLOG subsystem is ready to track transaction commit/abort status
- This is a prerequisite for normal database operation as MVCC depends on transaction status information stored in CLOG