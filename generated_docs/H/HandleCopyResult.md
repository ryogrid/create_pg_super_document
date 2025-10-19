# HandleCopyResult

## Location
[src/bin/psql/common.c:902-956](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/common.c#L902-L956)

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
  - [PQresultStatus](../P/PQresultStatus.md) (determines if result is COPY IN/OUT)
  - Assert (validates result status)
  - [SetCancelConn](../S/SetCancelConn.md) (enables cancellation during COPY)
  - [handleCopyOut](../h/handleCopyOut.md) (handles COPY TO operations)
  - [handleCopyIn](../h/handleCopyIn.md) (handles COPY FROM operations) 
  - [PQbinaryTuples](../P/PQbinaryTuples.md) (checks if result uses binary format)
  - [ResetCancelConn](../R/ResetCancelConn.md) (disables cancellation after COPY)
  - [PQclear](../P/PQclear.md) (frees PGresult memory)
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
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (in src/bin/psql/common.c:1643)

## Notes and Other Information
- This is a static function internal to psql's common.c module
- The function modifies the result pointer in-place, replacing the COPY status result with the final command result
- Proper cancellation handling is critical for COPY operations which can transfer large amounts of data
- The function handles both text and binary COPY formats automatically
- Memory management ensures the original result is properly freed and replaced
- Status suppression logic prevents cluttering COPY output with status messages when output goes to the same stream

## Simplified Source

```c
static bool HandleCopyResult(PGresult **resultp, FILE *copystream) {
    bool success;
    PGresult *copy_result;
    ExecStatusType result_status = PQresultStatus(*resultp);

    // Enable cancellation during COPY operation
    SetCancelConn(pset.db);

    if (result_status == PGRES_COPY_OUT) {
        // Handle COPY TO operation
        success = handleCopyOut(pset.db, copystream, &copy_result)
                  && (copystream != NULL);

        // Suppress status if output goes to same stream as query results
        if (copystream == pset.queryFout) {
            PQclear(copy_result);
            copy_result = NULL;
        }
    } else {
        // Handle COPY FROM operation - use configured or default input stream
        copystream = pset.copyStream ? pset.copyStream : pset.cur_cmd_source;
        success = handleCopyIn(pset.db, copystream,
                              PQbinaryTuples(*resultp), &copy_result);
    }

    // Clean up cancellation handling
    ResetCancelConn();

    // Replace original result with final COPY result
    PQclear(*resultp);
    *resultp = copy_result;

    return success;
}
```