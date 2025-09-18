# XLogInsertRecord

## Location
src/backend/access/transam/xlog.c: 750 - 1109

## Overview
XLogInsertRecord is the core function responsible for inserting pre-constructed XLOG records into the Write-Ahead Log (WAL), implementing the fundamental WAL insertion mechanism with proper locking and space reservation.

## Definition


## Detailed Description
XLogInsertRecord is a low-level routine that inserts an XLOG record represented by a chain of pre-constructed data chunks into the WAL. This function implements a sophisticated two-step process: first reserving space in the WAL buffer, then copying the record data to that reserved space. It handles three different insertion classes: normal records, XLOG_SWITCH records (which require exclusive access), and checkpoint redo records. The function includes critical safety checks for full-page writes, manages WAL insertion locks to coordinate concurrent insertions, and updates various global state variables upon successful insertion.

## Parameters / Member Variables
- : Chain of XLogRecData structures containing the record data, with the first chunk containing the record header
- : Oldest LSN among pages affected by this record that were not included as full-page images; used for full-page write validation
- : Control flags for the record insertion (see XLogSetRecordFlags for details)
- : Number of full-page images included in this record
- : Whether the top-transaction ID is logged with the current subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - [WALInsertLockAcquire](../W/WALInsertLockAcquire.md)
  - [WALInsertLockAcquireExclusive](../W/WALInsertLockAcquireExclusive.md)
  - [ReserveXLogInsertLocation](../R/ReserveXLogInsertLocation.md)
  - [ReserveXLogSwitch](../R/ReserveXLogSwitch.md)
  - [CopyXLogRecordToWAL](../C/CopyXLogRecordToWAL.md)
  - [XLogInsertAllowed](XLogInsertAllowed.md)
  - [MarkCurrentTransactionIdLoggedIfAny](../M/MarkCurrentTransactionIdLoggedIfAny.md)
  - [XLogFlush](XLogFlush.md)
- Called from (representative examples):
  - [XLogInsert](XLogInsert.md) (from xloginsert.c)

## Notes and Other Information
- Implements the basic WAL rule "write the log before the data" by returning an LSN that must be flushed before affected data pages can be written
- Uses a critical section to ensure atomicity of the insertion process
- Handles three insertion classes with different locking requirements: normal (single lock), switch (exclusive), and checkpoint (exclusive with RedoRecPtr update)
- Includes sophisticated full-page write logic that may cause the function to return InvalidXLogRecPtr, requiring the caller to recalculate and retry
- Updates various global variables including ProcLastRecPtr, XactLastRecEnd, and WAL usage statistics
- Contains extensive debugging support when WAL_DEBUG is enabled