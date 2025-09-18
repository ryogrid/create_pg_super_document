# XLogSetRecordFlags

## Location
src/backend/access/transam/xloginsert.c: 456 - 473

## Overview
XLogSetRecordFlags sets special flags for the WAL record currently being constructed to control record behavior during insertion and processing.

## Definition
void XLogSetRecordFlags(uint8 flags)

## Detailed Description
XLogSetRecordFlags is a utility function in PostgreSQL's WAL insertion system that allows callers to set special behavioral flags for the WAL record being constructed. The function uses a bitwise OR operation to combine the provided flags with any existing flags in the curinsert_flags global variable.

The function provides a way to modify the characteristics of a WAL record before insertion, affecting how the record is handled during WAL processing, replication, and archiving. The flags are accumulated across multiple calls, allowing different parts of the code to set different flags for the same record.

The primary use cases involve controlling replication origin inclusion and marking records as unimportant for durability purposes, which can optimize WAL processing by avoiding unnecessary background operations for certain types of records.

## Parameters / Member Variables
- : Bitfield containing the flags to set for the current WAL record. Valid flags include:
  - XLOG_INCLUDE_ORIGIN: Include replication origin information in the record
  - XLOG_MARK_UNIMPORTANT: Mark record as not critical for durability, allowing optimization of WAL archiving and background processes

## Dependencies
- Functions called/Symbols referenced:
  - Assert (for debug assertions)
  - curinsert_flags (global variable storing current insertion flags)
- Called from (representative examples):
  - [heap_insert](../h/heap_insert.md) (heap tuple insertions)
  - [XactLogCommitRecord](XactLogCommitRecord.md) (transaction commit logging)
  - [LogLogicalMessage](../L/LogLogicalMessage.md) (logical replication messages)
  - [ExecuteTruncateGuts](../E/ExecuteTruncateGuts.md) (table truncation operations)
  - [RequestXLogSwitch](../R/RequestXLogSwitch.md) (WAL switch requests)

## Notes and Other Information
- Must be called after XLogBeginInsert() and before XLogInsert()
- Flags are accumulated using bitwise OR, allowing multiple calls to set different flags
- XLOG_MARK_UNIMPORTANT is used for records that don't need immediate WAL archiving
- XLOG_INCLUDE_ORIGIN helps with replication origin tracking
- The function is lightweight and simply modifies a global flag variable
- Flags affect record processing and replication behavior but not the core record content