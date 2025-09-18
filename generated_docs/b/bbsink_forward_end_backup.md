# bbsink_forward_end_backup

## Location
src/backend/backup/basebackup_sink.c: 111 - 120

## Overview
A forwarding function that passes the end backup signal along with WAL position information to the next backup sink in a chain, used to finalize the entire base backup process in PostgreSQL.

## Definition
void bbsink_forward_end_backup(bbsink *sink, XLogRecPtr endptr, TimeLineID endtli)

## Detailed Description
This function is part of PostgreSQL's base backup sink forwarding mechanism. It forwards the end_backup callback to the next sink in a chained configuration of backup sinks. This function is called when the entire backup process is complete and needs to be finalized, passing along critical WAL (Write-Ahead Logging) information including the end LSN position and timeline ID.

The function performs an assertion to ensure there is a valid next sink in the chain before forwarding the end_backup operation with the provided WAL position parameters. This is the final step in the backup process that ensures proper completion and cleanup across the entire sink chain.

The forwarding of endptr and endtli is crucial as these parameters contain the exact point in the WAL where the backup ends, which is essential for backup consistency and recovery operations.

## Parameters / Member Variables
- : Pointer to the current bbsink structure in the chain
- : XLogRecPtr indicating the WAL position where the backup ends
- : TimeLineID representing the timeline of the backup end point

## Dependencies
- Functions called/Symbols referenced:
  - bbsink_end_backup
  - bbsink (structure type)
  - XLogRecPtr (type)
  - TimeLineID (type)
- Called from (representative examples):
  - bbsink_zstd_end_backup

## Notes and Other Information
- This is the final callback in the backup process, indicating complete backup termination
- The endptr parameter is critical for backup consistency and contains the exact WAL position
- The endtli parameter ensures timeline consistency for the backup
- Part of the callback-based architecture for chaining backup sink operations
- Used to ensure all sinks in the chain properly finalize their backup operations with consistent WAL position information