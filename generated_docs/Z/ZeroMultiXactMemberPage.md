# ZeroMultiXactMemberPage

## Location
[src/backend/access/transam/multixact.c:2082-2109](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L2082-L2109)

## Overview
ZeroMultiXactMemberPage initializes or reinitializes a page of the MultiXact member log to zeroes, optionally writing an XLOG record for crash recovery.

## Definition
```c
static int ZeroMultiXactMemberPage(int64 pageno, bool writeXlog)
```

## Detailed Description
This static function creates a new zeroed page in the MultiXact member log at the specified page number. It is the counterpart to ZeroMultiXactOffsetPage, handling the member portion of the MultiXact storage system. The function serves both bootstrap initialization and runtime extension of the member log.

Like its offset counterpart, this function operates entirely in shared memory without immediately writing to disk. The actual page writing is handled by the caller. When the writeXlog parameter is true, the function emits an XLOG record (XLOG_MULTIXACT_ZERO_MEM_PAGE) to ensure the operation can be replayed during crash recovery, maintaining consistency across primary and standby servers.

The function assumes that the appropriate control lock (for MultiXactMemberCtl) is already held by the caller and maintains this lock state throughout execution.

## Parameters / Member Variables
- `pageno`: The page number in the MultiXact member log to initialize
- `writeXlog`: Boolean flag indicating whether to write an XLOG record for this operation

## Dependencies
- Functions called/Symbols referenced:
  - [SimpleLruZeroPage](../S/SimpleLruZeroPage.md)
  - [WriteMZeroPageXlogRec](../W/WriteMZeroPageXlogRec.md)
  - XLOG_MULTIXACT_ZERO_MEM_PAGE (constant)
- Global variables accessed:
  - MultiXactMemberCtl
- Called from:
  - [BootStrapMultiXact](../B/BootStrapMultiXact.md)
  - [ExtendMultiXactMember](../E/ExtendMultiXactMember.md)
  - [multixact_redo](../m/multixact_redo.md)

## Notes and Other Information
- Function is static and only accessible within the multixact.c module
- Control lock must be held by caller before entry and remains held after exit
- Page is created in shared memory but not immediately written to disk
- XLOG record generation is conditional based on the writeXlog parameter
- Returns the slot number of the newly created page for caller use
- Complements ZeroMultiXactOffsetPage in the dual-log structure of MultiXact storage
- Critical for both system initialization and runtime extension of MultiXact member storage

## Simplified Source

```c
static int
ZeroMultiXactMemberPage(int64 pageno, bool writeXlog)
{
    int slotno;

    // Create a zeroed page in the MultiXact member log
    slotno = SimpleLruZeroPage(MultiXactMemberCtl, pageno);

    // Write XLOG record if requested for crash recovery
    if (writeXlog)
        WriteMZeroPageXlogRec(pageno, XLOG_MULTIXACT_ZERO_MEM_PAGE);

    return slotno;
}
```