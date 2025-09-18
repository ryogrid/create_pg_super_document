# WriteTruncateXlogRec

## Location
src/backend/access/transam/commit_ts.c: 1007 - 1022

## Overview
Writes a TRUNCATE WAL record to log the truncation of CLOG pages, with mandatory flushing to disk to ensure durability before returning.

## Definition
```c
static void WriteTruncateXlogRec(int64 pageno, TransactionId oldestXact, Oid oldestXactDb)
```

## Detailed Description
This function creates and writes a WAL record to log the truncation of CLOG (commit log) pages. The function is critical for maintaining consistency during CLOG truncation operations, as it ensures that the truncation is properly logged before the actual truncation occurs. A key requirement is that the WAL record must be flushed to disk before the function returns, as noted in TruncateCLOG(). This ensures that if a crash occurs, the recovery process can properly handle the truncation state. The function packages the truncation information into an xl_clog_truncate structure containing the page number, oldest transaction ID, and oldest transaction's database OID.

## Parameters / Member Variables
- `pageno`: The CLOG page number where truncation begins
- `oldestXact`: The oldest transaction ID that should be preserved after truncation
- `oldestXactDb`: The database OID of the oldest transaction

## Dependencies
- Functions called/Symbols referenced:
  - xl_clog_truncate
  - XLogBeginInsert
  - XLogRegisterData
  - XLogInsert (with RM_CLOG_ID, CLOG_TRUNCATE)
  - XLogFlush
  - CLOG_TRUNCATE
- Called from (representative examples):
  - TruncateCLOG
  - TruncateCommitTs (via XactCtl function pointer)

## Notes and Other Information
- Static function, internal to clog.c
- Must flush the WAL record to disk before returning - this is a critical durability requirement
- Uses xl_clog_truncate structure to package all truncation information into a single WAL record
- Part of the CLOG truncation infrastructure that removes old transaction status information
- The WAL record type is CLOG_TRUNCATE under the RM_CLOG_ID resource manager
- Also used by commit timestamp functionality through the XactCtl function pointer mechanism
- The flush operation (XLogFlush) ensures that the truncation record is durably stored before any actual page truncation occurs
- Essential for crash recovery to maintain proper CLOG state across system restarts