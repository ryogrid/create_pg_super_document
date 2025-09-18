# ZeroCommitTsPage

## Location
src/backend/access/transam/commit_ts.c: 615 - 631

## Overview
ZeroCommitTsPage initializes or reinitializes a page of commit timestamp data to zeroes in shared memory, optionally writing an XLOG record for crash recovery.

## Definition
```c
static int ZeroCommitTsPage(int64 pageno, bool writeXlog)
```

## Detailed Description
This function is a low-level utility for the commit timestamp subsystem that sets up a new page in the commit timestamp SLRU (Simple LRU) buffer. The function uses the SimpleLRU interface to zero out a page and optionally logs the operation for crash recovery purposes. The page is prepared in shared memory but not immediately written to disk - actual disk writes are handled by the SLRU management system.

The function assumes that the commit timestamp control lock is already held by the caller and maintains this lock throughout execution.

## Parameters / Member Variables
- `pageno`: The logical page number within the commit timestamp SLRU to initialize
- `writeXlog`: Boolean flag indicating whether to emit an XLOG record for this operation (required for crash recovery)

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruZeroPage](../S/SimpleLruZeroPage.md)
  - [WriteZeroPageXlogRec](../W/WriteZeroPageXlogRec.md)
  - CommitTsCtl
- Called from (representative examples):
  - [ActivateCommitTs](../A/ActivateCommitTs.md)
  - [ExtendCommitTs](../E/ExtendCommitTs.md)  
  - [commit_ts_redo](../c/commit_ts_redo.md)

## Notes and Other Information
- This is a static function, only accessible within the commit_ts.c module
- The caller must hold the commit timestamp control lock before calling this function
- The function returns the slot number where the page was allocated in the SLRU buffer
- The actual page write to disk is deferred and managed by the SLRU subsystem
- XLOG record generation is conditional based on the writeXlog parameter, allowing flexibility for different usage contexts (normal operation vs. recovery)