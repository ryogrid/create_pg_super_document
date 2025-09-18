# xl_overwrite_contrecord

## Location
src/include/access/xlog_internal.h: 293 - 297

## Overview
A data structure that logs when a continuation record has been overwritten, typically during WAL recovery when handling incomplete records at the end of WAL segments.

## Definition


## Detailed Description
xl_overwrite_contrecord is a WAL record structure used to log instances where a continuation record (contrecord) has been overwritten during WAL processing. This typically occurs during recovery scenarios where an incomplete WAL record exists at the end of a WAL segment, and new WAL records overwrite the incomplete continuation data.

Continuation records are used when a single logical WAL record spans multiple WAL pages. When recovery encounters an incomplete continuation record, it may need to overwrite it with new data, and this structure logs that event with the XLOG_OVERWRITE_CONTRECORD record type (0xD0). This logging helps maintain WAL integrity and provides audit information about when and where overwrites occurred.

## Parameters / Member Variables
- : The LSN (Log Sequence Number) location where the continuation record was overwritten
- : Timestamp (with timezone) indicating when the overwrite operation occurred

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecPtr (WAL record pointer type)
  - TimestampTz (timestamp with timezone type)
- Called from (representative examples):
  - [CreateOverwriteContrecordRecord](../C/CreateOverwriteContrecordRecord.md) (creates and logs overwrite contrecord records)
  - [xlog_desc](xlog_desc.md) (describes overwrite contrecord records for debugging)
  - [xlogrecovery_redo](xlogrecovery_redo.md) (processes overwrite contrecord records during recovery)

## Notes and Other Information
- Associated with WAL record type XLOG_OVERWRITE_CONTRECORD (0xD0)
- Used during WAL recovery when handling incomplete continuation records
- Helps maintain WAL integrity by logging overwrite operations
- Part of PostgreSQL's mechanism for handling partial records at WAL segment boundaries
- The record type 0xD0 is specifically used for this purpose (0xC0 was used in PostgreSQL 9.5-11)
- Critical for ensuring proper recovery behavior when dealing with incomplete WAL records
- Provides audit trail for debugging recovery issues related to continuation records