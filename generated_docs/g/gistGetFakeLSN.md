# gistGetFakeLSN

## Location
[src/backend/access/gist/gistutil.c:1015-1057](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/gist/gistutil.c#L1015-L1057)

## Overview
Provides fake LSN (Log Sequence Number) sequences for GiST indexes that are not WAL-logged, enabling detection of concurrent page splits even without actual WAL logging.

## Definition

```c
XLogRecPtr
gistGetFakeLSN(Relation rel)
```
## Detailed Description
This function generates fake LSN values for GiST indexes that don't participate in Write-Ahead Logging (WAL). LSNs are crucial for detecting concurrent page splits during index operations, even when the index itself isn't being logged to WAL. The function handles three different types of relations with distinct strategies:

1. **Temporary relations**: Uses a simple backend-local counter since these are only accessible within the current session
2. **Permanent relations** (not yet WAL-logged): Uses the current WAL insert position, ensuring LSNs are smaller than the next commit's LSN, and may emit dummy WAL records to ensure distinct LSNs
3. **Unlogged relations**: Delegates to the system-wide GetFakeLSNForUnloggedRel() function to handle cross-backend accessibility and restart survival

## Parameters / Member Variables
- : The relation (index) for which to generate a fake LSN

## Dependencies
- Functions called/Symbols referenced:
  - RelationIsPermanent
  - [GetXLogInsertRecPtr](../G/GetXLogInsertRecPtr.md)
  - RelationNeedsWAL
  - XLogRecPtrIsInvalid
  - [gistXLogAssignLSN](gistXLogAssignLSN.md)
  - [GetFakeLSNForUnloggedRel](../G/GetFakeLSNForUnloggedRel.md)
- Constants used:
  - RELPERSISTENCE_TEMP
  - RELPERSISTENCE_UNLOGGED
  - FirstNormalUnloggedLSN
  - InvalidXLogRecPtr
- Called from:
  - [gistplacetopage](gistplacetopage.md) (multiple calls for page placement operations)
  - [gistprunepage](gistprunepage.md) (for page pruning operations)
  - [gistvacuumscan](gistvacuumscan.md) (during vacuum scanning)
  - [gistvacuumpage](gistvacuumpage.md) (during vacuum page processing)
  - [gistdeletepage](gistdeletepage.md) (for page deletion operations)

## Notes and Other Information
- Uses static variables to maintain state between calls: counter for temporary relations and lastlsn for permanent relations
- For permanent relations, asserts that the relation doesn't need WAL logging (would be a bug if it did)
- The fake LSN mechanism is essential for GiST's concurrent split detection algorithm
- For permanent relations, if the insert LSN hasn't advanced since the last call, generates a dummy WAL record to ensure distinct LSNs
- Each relation type has different concurrency and persistence requirements, hence the three-way handling strategy