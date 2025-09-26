# ZeroSUBTRANSPage

## Location
src/backend/access/transam/subtrans.c: 296 - 308

## Overview
ZeroSUBTRANSPage initializes or reinitializes a page of the SUBTRANS (subtransaction) log to contain all zeros.

## Definition
```c
static int ZeroSUBTRANSPage(int64 pageno)
```

## Detailed Description
This static function creates a zeroed page in the SUBTRANS system for the specified page number. It serves as a thin wrapper around SimpleLruZeroPage, delegating the actual work to the generic SLRU (Simple LRU) page management system. The function sets up the page in shared memory but does not write it to disk immediately - that responsibility lies with the caller. The page is marked as dirty and valid, ready for use by the subtransaction tracking system.

## Parameters / Member Variables
- `pageno`: The 64-bit page number to initialize in the SUBTRANS log

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruZeroPage
- Global variables accessed:
  - SubTransCtl
- Called from (representative examples):
  - BootStrapSUBTRANS
  - StartupSUBTRANS
  - ExtendSUBTRANS

## Notes and Other Information
- This is a static function, only accessible within subtrans.c
- The control lock must be held at entry and will be held at exit
- The page is set up in shared memory but not written to disk
- Returns the slot number of the newly initialized page
- Used during bootstrap, startup recovery, and log extension operations
- The actual zeroing and buffer management is handled by SimpleLruZeroPage
- Part of the SUBTRANS subsystem for tracking subtransaction commit status
- Located in src/backend/access/transam/subtrans.c:296-308