# logfile_rotate

## Location
src/backend/postmaster/syslogger.c: 1362 - 1410

## Overview
Performs log file rotation for PostgreSQL's system logger, handling time-based and size-based rotation across multiple log destinations (stderr, CSV, and JSON logs).

## Definition


## Detailed Description
The  function is responsible for coordinating log file rotation across all active log destinations in PostgreSQL's system logger. It supports both time-based rotation (triggered by scheduled intervals) and size-based rotation (triggered when log files exceed size limits). The function ensures consistent naming across all log file types by using a unified timestamp, and handles the rotation process atomically by updating metadata and scheduling the next rotation time only after all individual destination rotations succeed.

## Parameters / Member Variables
- : Boolean flag indicating whether this rotation is triggered by time (true) or size (false)
- : When size-based rotation is performed, indicates which log destination triggered the rotation

## Dependencies
- Functions called/Symbols referenced:
  -  - Performs rotation for individual log destinations
  -  - Updates metadata file with current log file information
  -  - Schedules the next time-based rotation
  - , ,  - Log destination constants
  -  - PostgreSQL time type
- Called from (representative examples):
  -  - Main system logger process loop

## Notes and Other Information
- The function resets the  flag at the start to prevent duplicate rotations
- For time-based rotations, uses  rather than current time to prevent filename "slippage"
- Rotation is atomic - if any destination rotation fails, the entire process is aborted without updating metadata or scheduling next rotation
- Handles three log destinations: stderr (standard logs), CSV logs, and JSON logs
- The function maintains separate filename tracking variables for each log type (, , )