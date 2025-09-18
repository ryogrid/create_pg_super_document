# XLogPutNextOid

## Location
src/backend/access/transam/xlog.c: 8039 - 8075

## Overview
XLogPutNextOid writes a NEXTOID WAL record to log the allocation of a new range of OIDs, ensuring crash recovery can properly restore OID counter state.

## Definition
```c
void XLogPutNextOid(Oid nextOid)
```

## Detailed Description
This function creates a WAL record that logs the advancement of the global OID counter to a new value. The NEXTOID record serves as a checkpoint for OID allocation, allowing recovery to restore the correct OID counter state after a crash. This prevents OID reuse that could cause conflicts with objects created before the crash.

The function deliberately does not flush the WAL record immediately, relying on PostgreSQL's buffer LSN interlock mechanism. Any database objects using the newly allocated OIDs will have their own WAL records that must be written after this NEXTOID record, ensuring proper ordering through the standard WAL protocols.

A notable design consideration is that when OIDs are used as filesystem names (files/directories), there's a potential race condition where the filesystem change might reach disk before the NEXTOID WAL record. However, this is mitigated by PostgreSQL's practice of always checking for filename conflicts and retrying with different OIDs when necessary.

## Parameters / Member Variables
- `nextOid`: The next OID value to be recorded in the WAL, representing the new position of the global OID counter

## Dependencies
- Functions called/Symbols referenced:
  - [XLogBeginInsert](XLogBeginInsert.md)
  - [XLogRegisterData](XLogRegisterData.md)
  - [XLogInsert](XLogInsert.md)
  - XLOG_NEXTOID (WAL record type)
- Called from (representative examples):
  - [GetNewObjectId](../G/GetNewObjectId.md) (when allocating new OIDs and advancing the counter)

## Notes and Other Information
- The function does not immediately flush the WAL record, relying on buffer LSN interlock for proper ordering
- Part of PostgreSQL's OID management and crash recovery system
- Critical for preventing OID conflicts after database crashes and restarts
- The race condition with filesystem OID usage is considered acceptable due to conflict detection and retry mechanisms
- Uses the standard WAL insertion API (XLogBeginInsert/XLogRegisterData/XLogInsert pattern)
- The WAL record type XLOG_NEXTOID is handled during recovery to restore OID counter state