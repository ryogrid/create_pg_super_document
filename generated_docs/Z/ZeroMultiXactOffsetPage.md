# ZeroMultiXactOffsetPage

## Location
src/backend/access/transam/multixact.c: 2066 - 2081

## Overview
ZeroMultiXactOffsetPage initializes or reinitializes a page of the MultiXact offset log to zeroes, optionally writing an XLOG record for crash recovery.

## Definition
```c
static int ZeroMultiXactOffsetPage(int64 pageno, bool writeXlog)
```

## Detailed Description
This static function creates a new zeroed page in the MultiXact offset log at the specified page number. The function serves both bootstrap initialization and runtime extension of the offset log. It operates entirely in shared memory without immediately writing to disk - the actual page writing is handled by the caller.

When the writeXlog parameter is true, the function also emits an XLOG record (XLOG_MULTIXACT_ZERO_OFF_PAGE) to ensure the operation can be replayed during crash recovery. This is crucial for maintaining consistency across primary and standby servers.

The function assumes that the appropriate control lock (for MultiXactOffsetCtl) is already held by the caller and maintains this lock state throughout execution.

## Parameters / Member Variables
- `pageno`: The page number in the MultiXact offset log to initialize
- `writeXlog`: Boolean flag indicating whether to write an XLOG record for this operation

## Dependencies
- Functions called/Symbols referenced:
  - SimpleLruZeroPage
  - WriteMZeroPageXlogRec
  - XLOG_MULTIXACT_ZERO_OFF_PAGE (constant)
- Global variables accessed:
  - MultiXactOffsetCtl
- Called from:
  - BootStrapMultiXact
  - MaybeExtendOffsetSlru
  - ExtendMultiXactOffset
  - multixact_redo

## Notes and Other Information
- Function is static and only accessible within the multixact.c module
- Control lock must be held by caller before entry and remains held after exit
- Page is created in shared memory but not immediately written to disk
- XLOG record generation is conditional based on the writeXlog parameter
- Returns the slot number of the newly created page for caller use
- Critical for both system initialization and runtime extension of MultiXact storage