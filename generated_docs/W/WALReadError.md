# WALReadError

## Location
src/include/access/xlogreader.h: 382 - 389

## Overview
WALReadError is a structure that encapsulates error information from WAL reading operations that can be processed by both backend and frontend callers. It specifically handles errors from pg_pread operations during WAL segment reading.

## Definition


## Detailed Description
WALReadError serves as a comprehensive error reporting structure for WAL reading operations. It captures all relevant context when a WAL read operation fails, including the system error code, the attempted read parameters (offset and requested bytes), the actual bytes read, and information about the WAL segment being accessed. This structure enables both PostgreSQL backend processes and frontend utilities to diagnose and handle WAL reading failures in a consistent manner.

The structure is primarily populated by low-level WAL reading functions like pg_pread() and is used by higher-level error handling routines to provide detailed diagnostic information when WAL operations fail.

## Parameters / Member Variables
- : The errno value set by the last pg_pread() call, indicating the specific system-level error that occurred
- : The file offset position where the read operation was attempted
- : The number of bytes that were requested to be read from the WAL segment
- : The actual number of bytes successfully read by the last read() operation (may be less than requested)
- : Complete information about the WAL segment that was being read when the error occurred

## Dependencies
- Functions called/Symbols referenced:
  - WALOpenSegment (for segment information storage)

- Called from (representative examples):
  - WALRead (src/backend/access/transam/xlogreader.c:1515)
  - read_local_xlog_page_guts (src/backend/access/transam/xlogutils.c:893)
  - WALReadRaiseError (src/backend/access/transam/xlogutils.c:1020)
  - summarizer_read_local_xlog_page (src/backend/postmaster/walsummarizer.c:1502)
  - logical_read_xlog_page (src/backend/replication/walsender.c:1060)
  - XLogSendPhysical (src/backend/replication/walsender.c:3107)
  - WALDumpReadPage (src/bin/pg_waldump/pg_waldump.c:394)

## Notes and Other Information
- This structure is designed to work across both backend and frontend contexts, making it suitable for use in standalone utilities like pg_waldump as well as server processes
- The error information captured allows for detailed diagnostics of WAL reading problems, which is crucial for debugging replication issues, recovery problems, and archiving failures
- The structure specifically focuses on pg_pread() errors, which are the most common type of low-level WAL reading failures
- Used extensively in WAL streaming, logical replication, and WAL archiving components
- Essential for proper error reporting in high-availability and disaster recovery scenarios