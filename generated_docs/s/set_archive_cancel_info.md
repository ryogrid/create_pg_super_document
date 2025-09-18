# set_archive_cancel_info

## Location
src/bin/pg_dump/parallel.c: 730 - 788

## Overview
Manages cancellation information for PostgreSQL database connections in the pg_dump parallel backup system, allowing graceful interruption of backup operations.

## Definition


## Detailed Description
This function sets up or clears cancellation information for a database connection within the archive handle structure. It manages the transition between different database connections during parallel backup operations, ensuring that signal handlers can properly cancel ongoing database operations when needed.

The function handles platform-specific synchronization requirements - using atomic pointer operations on Unix systems and critical sections on Windows to prevent race conditions between the main thread and signal handlers. It also ensures that the interrupt handler is properly initialized before setting up cancellation information.

## Parameters / Member Variables
- : Pointer to the ArchiveHandle structure that will store the cancellation information
- : PostgreSQL connection object for which to set up cancellation, or NULL to clear existing cancellation info

## Dependencies
- Functions called/Symbols referenced:
  - set_cancel_handler
  - PQfreeCancel
  - PQgetCancel
  - PGcancel (type)
- Called from (representative examples):
  - ParallelBackupStart
  - ConnectDatabase
  - DisconnectDatabase

## Notes and Other Information
- Thread-safe implementation with platform-specific synchronization (critical sections on Windows)
- Automatically frees previous cancellation objects to prevent memory leaks
- On Windows, only the main thread sets signal_info.myAH; worker threads handle this differently in RunWorker()
- Essential for graceful handling of Ctrl+C and other interruption signals during backup operations