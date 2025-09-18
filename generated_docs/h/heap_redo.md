# heap_redo

## Location
src/backend/access/heap/heapam.c: 10338 - 10383

## Overview
WAL (Write-Ahead Logging) redo function for heap access method operations that processes heap-related log records during crash recovery and replication.

## Definition


## Detailed Description
The  function is the primary entry point for replaying heap table operations from WAL records during PostgreSQL recovery. It serves as a dispatcher that examines the operation type encoded in the WAL record and calls the appropriate specific redo function. This function handles basic heap operations that don't involve MVCC conflicts, distinguishing it from heap2_redo which handles more complex operations requiring conflict processing.

The function extracts the operation code from the WAL record and uses a switch statement to route to the correct handler function. It supports various heap operations including INSERT, DELETE, UPDATE, HOT_UPDATE, CONFIRM, LOCK, INPLACE updates, and TRUNCATE operations.

## Parameters / Member Variables
- : XLogReaderState pointer containing the WAL record to be replayed, including operation type and associated data

## Dependencies
- Functions called/Symbols referenced:
  - XLogRecGetInfo (extracts info from WAL record)
  - heap_xlog_insert (handles INSERT operations)
  - heap_xlog_delete (handles DELETE operations) 
  - heap_xlog_update (handles UPDATE and HOT_UPDATE operations)
  - heap_xlog_confirm (handles CONFIRM operations)
  - heap_xlog_lock (handles LOCK operations)
  - heap_xlog_inplace (handles in-place UPDATE operations)
- Called from:
  - WAL replay infrastructure (not directly referenced by other functions)

## Notes and Other Information
- This function processes only basic heap operations that don't require MVCC conflict processing
- TRUNCATE operations are handled as no-ops since the actual work is done by SMGR WAL records
- The function will panic with an error if it encounters an unknown operation code
- Part of PostgreSQL's crash recovery and replication system
- Distinguished from heap2_redo which handles operations requiring conflict processing