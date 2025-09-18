# HandleCopyResult

## Location
src/bin/psql/common.c: 902 - 956

## Overview
Handles PostgreSQL COPY command results by marshaling COPY data flow and managing the connection state transition out of COPY mode for both COPY IN and COPY OUT operations.

## Definition
static bool HandleCopyResult(PGresult **resultp, FILE *copystream)

## Detailed Description
This function serves as the central dispatcher for handling COPY operations in psql. It determines whether the operation is COPY IN or COPY OUT based on the result status, then delegates to the appropriate handler function (handleCopyIn or handleCopyOut). The function manages several critical aspects:

- Sets up cancellation handling during the COPY operation
- For COPY OUT: directs output to the specified copystream or discards if NULL
- For COPY IN: uses pset.copyStream if available, otherwise falls back to cur_cmd_source
- Handles result replacement to prevent duplicate status printing
- Manages memory by clearing the original result and replacing it with the final result

The function also implements logic to suppress status printing when the COPY output goes to the same destination as query results (pset.queryFout) to avoid mixing data with status messages.

## Parameters / Member Variables
- resultp: Double pointer to PGresult that will be updated with the final result status after COPY completion
- copystream: FILE pointer specifying the destination stream for COPY OUT operations, or ignored for COPY IN

## Dependencies
- Functions called/Symbols referenced:
  - PQresultStatus (determines if result is COPY IN/OUT)
  - Assert (validates result status)
  - SetCancelConn (enables cancellation during COPY)
  - handleCopyOut (handles COPY TO operations)
  - handleCopyIn (handles COPY FROM operations) 
  - PQbinaryTuples (checks if result uses binary format)
  - ResetCancelConn (disables cancellation after COPY)
  - PQclear (frees PGresult memory)
- Constants referenced:
  - PGRES_COPY_OUT (result status for COPY TO)
  - PGRES_COPY_IN (result status for COPY FROM)
  - ExecStatusType (enum for result status types)
- Global variables accessed:
  - pset.db (database connection)
  - pset.queryFout (default query output stream)
  - pset.copyStream (COPY data source stream)
  - pset.cur_cmd_source (current command input stream)
- Called from:
  - ExecQueryAndProcessResults (in src/bin/psql/common.c:1643)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function modifies the result pointer in-place, replacing the COPY status result with the final command result
- Proper cancellation handling is critical for COPY operations which can transfer large amounts of data
- The function handles both text and binary COPY formats automatically
- Memory management ensures the original result is properly freed and replaced
- Status suppression logic prevents cluttering COPY output with status messages when output goes to the same stream