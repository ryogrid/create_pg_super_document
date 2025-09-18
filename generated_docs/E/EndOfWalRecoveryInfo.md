# EndOfWalRecoveryInfo

## Location
src/include/access/xlogrecovery.h: 132 - 158

## Overview
A structure that contains comprehensive information about the end point of WAL (Write-Ahead Log) recovery, including the last valid record position, recovery termination reason, and signal file status.

## Definition


## Detailed Description
The EndOfWalRecoveryInfo structure serves as a comprehensive container for information about the conclusion of WAL recovery processing. It is returned by FinishWalRecovery() and contains critical details needed for transitioning from recovery mode to normal database operation.

The structure captures the exact position where recovery ended, including both the start and end positions of the last successfully applied WAL record. It also maintains timeline information, handles broken records at the end of WAL, and tracks the status of signal files that influence recovery behavior.

This information is essential for properly initializing the database system after recovery completion, ensuring data consistency, and setting up the correct starting point for new WAL generation.

## Parameters / Member Variables
- : XLogRecPtr pointing to the start of the last valid or applied WAL record
- : Timeline ID associated with the last record
- : XLogRecPtr pointing to the end of the last valid or applied WAL record
- : Timeline ID for the XLOG segment containing the last applied record
- : LSN (Log Sequence Number) of the page that contains endOfLog
- : Copy of the last partial page containing endOfLog (NULL if endOfLog is at page boundary)
- : Start pointer of a broken record found at end of WAL during recovery completion
- : Location of the first continuation record that went missing
- : Human-readable string describing why recovery terminated
- : Flag indicating whether standby.signal file was found
- : Flag indicating whether recovery.signal file was found

## Dependencies
- Functions called/Symbols referenced:
  - Used as return type for FinishWalRecovery()
- Called from (representative examples):
  - StartupXLOG (src/backend/access/transam/xlog.c:5396)
  - FinishWalRecovery (src/backend/access/transam/xlogrecovery.c:1460)
  - read_tablespace_map (indirectly via FinishWalRecovery)

## Notes and Other Information
- The structure is allocated using palloc() in FinishWalRecovery()
- Critical for transitioning from recovery to normal database operation
- Used extensively in StartupXLOG() to set up post-recovery state including EndOfLog, timeline information, and WAL buffer initialization
- Timeline switches during recovery are handled through the separation of lastRecTLI and endOfLogTLI
- Signal file flags determine whether the database enters standby mode or completes recovery to become a primary
- The recoveryStopReason provides diagnostic information useful for logging and troubleshooting recovery completion