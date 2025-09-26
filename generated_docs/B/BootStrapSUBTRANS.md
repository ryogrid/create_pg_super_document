# BootStrapSUBTRANS

## Location
src/backend/access/transam/subtrans.c: 270 - 295

## Overview
BootStrapSUBTRANS initializes the SUBTRANS (subtransaction) system during PostgreSQL database cluster creation by creating and writing the initial subtransaction log page.

## Definition
```c
void BootStrapSUBTRANS(void)
```

## Detailed Description
This function must be called exactly once during system installation (initdb). It creates the initial SUBTRANS segment by zeroing the first page of the subtransaction log and ensuring it is written to disk. The function operates under exclusive lock protection to ensure atomicity of the bootstrap operation. While the SLRU system could create the initial segment on first write, this function proactively creates it to ensure the directory structure is properly set up and the system is in a known good state.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruGetBankLock
  - LWLockAcquire
  - ZeroSUBTRANSPage
  - SimpleLruWritePage
  - LWLockRelease
- Global variables accessed:
  - SubTransCtl
- Types used:
  - LWLock
- Constants used:
  - LW_EXCLUSIVE
- Called from (representative examples):
  - BootStrapXLOG

## Notes and Other Information
- Must be called during system installation (initdb) and only once
- Requires that SUBTRANSShmemInit has already been called
- Assumes the SUBTRANS directory has been created by initdb
- Uses exclusive locking on bank 0 to protect the bootstrap operation
- Creates page 0 of the subtransaction log and forces it to disk
- Part of the database cluster initialization sequence
- While not strictly necessary (SLRU would create on first write), it ensures proper setup
- Located in src/backend/access/transam/subtrans.c:270-295