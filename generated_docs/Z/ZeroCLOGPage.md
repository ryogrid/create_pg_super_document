# ZeroCLOGPage

## Location
[src/backend/access/transam/clog.c:860-876](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/clog.c#L860-L876)

## Overview
Initializes or reinitializes a CLOG page to contain all zero values, optionally writing an XLOG record for recovery purposes.

## Definition

```c
static int
ZeroCLOGPage(int64 pageno, bool writeXlog)
```
## Detailed Description
ZeroCLOGPage is a static internal function that creates a new CLOG page filled with zeros in shared memory. This function is essential for CLOG page management, as it provides a clean, initialized page that can be used to track transaction status for a new range of transaction IDs.

The function performs two main operations:

1. **Page zeroing**: Uses SimpleLruZeroPage() to allocate a buffer slot and initialize the specified CLOG page with zero values
2. **WAL logging**: Conditionally writes an XLOG record using WriteZeroPageXlogRec() if writeXlog is true, ensuring that this operation can be replayed during recovery

The zeroed page represents transaction status entries where all transactions are initially in an uncommitted state (represented by zero values in CLOG). As transactions commit or abort, their status bits will be updated from the initial zero state.

The function requires that the appropriate control lock be held before entry and maintains this lock state upon exit, ensuring thread-safe access to the CLOG structures.

## Parameters / Member Variables
- : The 64-bit page number within the CLOG address space to be zeroed
- : Boolean flag indicating whether to write an XLOG record for WAL recovery purposes

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruZeroPage](../S/SimpleLruZeroPage.md) (SLRU function to zero a page)
  - [WriteZeroPageXlogRec](../W/WriteZeroPageXlogRec.md) (writes WAL record for page zeroing)
- Global variables:
  - XactCtl (CLOG SLRU control structure)
- Called from:
  - [BootStrapCLOG](../B/BootStrapCLOG.md) (during initial CLOG setup)
  - [ExtendCLOG](../E/ExtendCLOG.md) (when extending CLOG to cover new transaction IDs)
  - [clog_redo](../c/clog_redo.md) (during WAL replay)

## Notes and Other Information
- This is a static (internal) function, not exported from the clog.c module
- The function only sets up the page in shared memory; it does not write to disk
- The caller is responsible for acquiring and releasing the appropriate control lock
- WAL logging is optional and controlled by the writeXlog parameter
- Zero values in CLOG pages represent uncommitted transactions initially
- The function returns the slot number where the new page has been placed in the buffer pool
- This function is critical for extending CLOG coverage as new transactions are assigned XIDs beyond the current CLOG range
- Proper locking ensures that concurrent access to CLOG pages is coordinated correctly