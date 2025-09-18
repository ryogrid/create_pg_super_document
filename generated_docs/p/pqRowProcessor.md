# pqRowProcessor

## Location
[src/interfaces/libpq/fe-exec.c:1206-1305](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-exec.c#L1206-L1305)

## Overview
pqRowProcessor processes incoming row data from the PostgreSQL server and adds it to the current async result, handling both normal and partial result modes with appropriate memory management and data conversion.

## Definition


## Detailed Description
This function is responsible for processing incoming row data during asynchronous query execution. It takes raw column data from conn->rowBuf and converts it into a properly formatted tuple that gets added to the current PGresult. The function handles both regular result processing and partial result modes (single-row mode and chunked results).

In partial result mode, the function manages result cloning and switching to create incremental results that can be returned to the client before the full query completes. For each column in the row, it allocates appropriate storage, handles NULL values using the special null_field pointer, and properly copies and null-terminates field data (including binary data for safety).

The function integrates with the tuple array management through pqAddTuple() and can trigger immediate result availability when enough rows have been accumulated in partial result mode.

## Parameters / Member Variables
- : Pointer to the PGconn structure containing the connection state and current result
- : Double pointer for returning error messages; set to NULL to use default "out of memory" message

## Dependencies
- Functions called/Symbols referenced:
  - [PQcopyResult](../P/PQcopyResult.md) (for cloning results in partial mode)
  - [pqResultAlloc](pqResultAlloc.md) (for allocating memory within the result context)
  - memcpy (for copying field data)
  - [pqAddTuple](pqAddTuple.md) (for adding the completed tuple to the result)
- Types used:
  - PGdataValue (raw column data structure)
  - PGresAttValue (result attribute value structure)
- Constants used:
  - PG_COPYRES_ATTRS, PG_COPYRES_EVENTS, PG_COPYRES_NOTICEHOOKS (copy flags)
  - PGRES_SINGLE_TUPLE, PGRES_TUPLES_CHUNK (partial result status values)
  - NULL_LEN (marker for NULL field length)
  - PGASYNC_READY_MORE (async status indicating partial results ready)
- Called from:
  - [getAnotherTuple](../g/getAnotherTuple.md) (in fe-protocol3.c during result processing)

## Notes and Other Information
- Returns 1 on success, 0 on error (typically memory allocation failure)
- Handles partial result mode by cloning the current result and managing saved_result state
- All field data is null-terminated, even binary data, for safety and consistency
- Uses pqResultAlloc() with appropriate binary flags based on field format (text vs binary)
- NULL fields are represented using the shared null_field pointer to save memory
- In partial result mode, triggers PGASYNC_READY_MORE when enough rows (maxChunkSize) are accumulated
- Memory allocation failures are handled gracefully without attempting to format error messages (since memory is scarce)
- The function preserves result metadata while creating new result objects for partial results
- Integrates with both single-row mode and chunked partial result delivery mechanisms
- Field format detection (binary vs text) is based on the attDescs format field
- Manages the transition between full result mode and partial result modes seamlessly