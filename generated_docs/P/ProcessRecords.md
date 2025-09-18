# ProcessRecords

## Location
src/backend/access/transam/twophase.c: 1680 - 1707

## Overview
ProcessRecords iterates through two-phase commit state data in memory and invokes appropriate callbacks for each stored 2PC record.

## Definition
static void ProcessRecords(char *bufptr, TransactionId xid, const TwoPhaseCallback callbacks[])

## Detailed Description
This static function serves as a generic processor for two-phase commit records stored in memory. It sequentially scans through a buffer containing serialized 2PC records, extracting each record's metadata (resource manager ID, info field, length), and dispatches the appropriate callback function based on the record's resource manager ID. The function continues processing until it encounters an end-of-records marker (TWOPHASE_RM_END_ID). Each record is processed by calling the corresponding callback function from the provided callback array, passing the transaction ID, record info, data pointer, and data length. This mechanism allows different subsystems to handle their specific 2PC cleanup or commit operations during transaction finalization.

## Parameters / Member Variables
- `bufptr`: Pointer to the memory buffer containing serialized two-phase commit records
- `xid`: The transaction ID of the transaction being processed 
- `callbacks[]`: Array of callback functions indexed by resource manager ID, where each callback handles records for its specific resource manager

## Dependencies
- Functions called/Symbols referenced:
  - TwoPhaseRecordOnDisk
  - TWOPHASE_RM_MAX_ID
  - TWOPHASE_RM_END_ID
- Called from (representative examples):
  - FinishPreparedTransaction
  - RecoverPreparedTransactions

## Notes and Other Information
- This is a static function, only accessible within the twophase.c module
- Records are processed in sequential order until TWOPHASE_RM_END_ID is encountered
- Callback functions may be NULL for resource managers that don't require post-commit/abort processing
- Uses MAXALIGN for proper memory alignment when advancing through the buffer
- The callback mechanism allows modular handling of different resource manager cleanup tasks