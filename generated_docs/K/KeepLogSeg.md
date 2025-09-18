# KeepLogSeg

## Location
[src/backend/access/transam/xlog.c:7967-8038](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L7967-L8038)

## Overview
KeepLogSeg calculates the oldest WAL segment that must be retained based on wal_keep_size, replication slot requirements, and WAL summarization needs, updating the provided segment number accordingly.

## Definition
```c
static void KeepLogSeg(XLogRecPtr recptr, XLogRecPtr slotsMinReqLSN, XLogSegNo *logSegNo)
```

## Detailed Description
This function determines the oldest WAL segment that should be retained by considering multiple retention policies and constraints. It works by calculating the minimum segment number needed based on:

1. Replication slot requirements (slotsMinReqLSN)
2. max_slot_wal_keep_size configuration limits  
3. WAL summarization requirements
4. wal_keep_size configuration

The function operates by "retreating" the segment number to ensure all retention requirements are satisfied. It applies each constraint in sequence, taking the most restrictive (earliest) segment number that satisfies all requirements. Special handling is provided during binary upgrades to avoid invalidating logical replication slots.

## Parameters / Member Variables
- `recptr`: Current WAL position used as reference point for calculations
- `slotsMinReqLSN`: Minimum LSN required by all replication slots
- `logSegNo`: Pointer to segment number that will be updated to reflect the oldest segment to keep

## Dependencies
- Functions called/Symbols referenced:
  - XLByteToSeg
  - ConvertToXSegs
  - [GetOldestUnsummarizedLSN](../G/GetOldestUnsummarizedLSN.md)
  - IsBinaryUpgrade
- Called from (representative examples):
  - [CreateCheckPoint](../C/CreateCheckPoint.md) (during checkpoint processing)
  - [CreateRestartPoint](../C/CreateRestartPoint.md) (during restart point creation)
  - [GetWALAvailability](../G/GetWALAvailability.md) (for availability analysis)
  - RefreshXLogWriteResult

## Notes and Other Information
- This is a static function used internally within xlog.c
- The function only retreats (decreases) the segment number - it never advances it
- During binary upgrades, max_slot_wal_keep_size limits are bypassed to preserve logical replication slots
- WAL summarization integration ensures summarized WAL is not prematurely removed
- If slots require more retention than max_slot_wal_keep_size allows, those slots should be invalidated
- The function includes underflow protection when calculating segment numbers
- Critical for coordinating WAL cleanup across different PostgreSQL subsystems