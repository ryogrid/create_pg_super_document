# multixact_redo

## Location
[src/backend/access/transam/multixact.c:3386-3501](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/multixact.c#L3386-L3501)

## Overview
multixact_redo is the resource manager's redo function that replays multixact-related xlog records during recovery, handling various multixact operations including page zeroing, multixact creation, and truncation.

## Definition

```c
typedef struct
	{
		MultiXactMember *members;
		int			nmembers;
		int			iter;
	} mxact;
```
## Detailed Description
This function serves as the main redo handler for the MULTIXACT resource manager during xlog replay. It processes different types of multixact operations based on the xlog record info field:
- XLOG_MULTIXACT_ZERO_OFF_PAGE: Zeros out a page in the multixact offsets SLRU
- XLOG_MULTIXACT_ZERO_MEM_PAGE: Zeros out a page in the multixact members SLRU  
- XLOG_MULTIXACT_CREATE_ID: Recreates a multixact during recovery
- XLOG_MULTIXACT_TRUNCATE_ID: Truncates multixact data during recovery

The function ensures proper locking, advances internal counters, and maintains consistency during the recovery process.

## Parameters / Member Variables
- : XLogReaderState pointer containing the xlog record to be replayed

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo
  - XLogRecHasAnyBlockRefs
  - XLogRecGetData
  - [SimpleLruGetBankLock](../S/SimpleLruGetBankLock.md)
  - [ZeroMultiXactOffsetPage](../Z/ZeroMultiXactOffsetPage.md)
  - [ZeroMultiXactMemberPage](../Z/ZeroMultiXactMemberPage.md)
  - [SimpleLruWritePage](../S/SimpleLruWritePage.md)
  - [RecordNewMultiXact](../R/RecordNewMultiXact.md)
  - [MultiXactAdvanceNextMXact](../M/MultiXactAdvanceNextMXact.md)
  - XLogRecGetXid
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md)
  - [AdvanceNextFullTransactionIdPastXid](../A/AdvanceNextFullTransactionIdPastXid.md)
  - [SetMultiXactIdLimit](../S/SetMultiXactIdLimit.md)
  - [PerformMembersTruncation](../P/PerformMembersTruncation.md)
  - [PerformOffsetsTruncation](../P/PerformOffsetsTruncation.md)
  - [MultiXactIdToOffsetSegment](../M/MultiXactIdToOffsetSegment.md)
  - [MXOffsetToMemberSegment](../M/MXOffsetToMemberSegment.md)
  - [MultiXactIdToOffsetPage](../M/MultiXactIdToOffsetPage.md)
- Called from:
  - Referenced by SizeOfMultiXactTruncate in src/include/access/multixact.h

## Notes and Other Information
- Asserts that backup blocks are not used in multixact records
- Handles exclusive locking of SLRU control structures during page operations
- Updates atomic counters for latest page numbers during truncation
- Advances transaction ID limits to ensure consistency
- Panics on unknown operation codes for safety
- Located in src/backend/access/transam/multixact.c:3386-3501