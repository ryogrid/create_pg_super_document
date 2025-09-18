# XLogPageReadPrivate

## Location
src/bin/pg_rewind/parsexlog.c: 47 - 51

## Overview
XLogPageReadPrivate is a private data structure used to pass parameters from higher-level WAL record reading functions down to the XLogPageRead callback function during PostgreSQL's write-ahead logging (WAL) recovery operations.

## Definition


## Detailed Description
This structure serves as a communication mechanism between the high-level WAL recovery logic and the low-level page reading operations. It encapsulates context information that the XLogPageRead callback function needs to make appropriate decisions about error handling, timeline selection, and access patterns during WAL recovery.

The structure is typically allocated and initialized in InitWalRecovery and passed to the XLogReader through its private_data field. This allows the XLogPageRead callback to access recovery-specific parameters without requiring them as direct function parameters.

## Parameters / Member Variables
- : Error mode specifying how errors should be handled during page reading (e.g., ERROR, WARNING, LOG levels)
- : Boolean flag indicating whether the current operation is fetching a checkpoint record, which may require special handling
- : Boolean flag indicating whether random access to WAL pages is being performed, as opposed to sequential reading
- : Timeline ID specifying which timeline should be used during replay operations

## Dependencies
- Functions called/Symbols referenced:
  - TimeLineID (type)
- Called from (representative examples):
  - InitWalRecovery (allocation and initialization at src/backend/access/transam/xlogrecovery.c:515,554)
  - ReadRecord (usage at src/backend/access/transam/xlogrecovery.c:3136)
  - XLogPageRead (dereferenced at src/backend/access/transam/xlogrecovery.c:3301,3302)
  - extractPageMap (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:72)
  - readOneRecord (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:130)
  - findLastCheckpoint (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:177)
  - SimpleXLogPageRead (pg_rewind usage at src/bin/pg_rewind/parsexlog.c:278)

## Notes and Other Information
- This structure is defined in src/backend/access/transam/xlogrecovery.c at lines 194-200
- The structure is used both in the main PostgreSQL backend for WAL recovery and in utility programs like pg_rewind for analyzing WAL records
- The private data pattern allows the XLogReader framework to remain generic while enabling specific recovery contexts to pass custom parameters to their page reading callbacks
- Memory for this structure is typically allocated using palloc0() to ensure all fields are properly initialized to zero/false
- The structure enables different error handling strategies depending on the recovery context (e.g., more permissive during checkpoint fetching)