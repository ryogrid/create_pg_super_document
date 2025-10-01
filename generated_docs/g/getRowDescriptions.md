# getRowDescriptions

## Location
[src/interfaces/libpq/fe-protocol3.c:503-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L503-L673)

## Overview
getRowDescriptions processes PostgreSQL protocol 'T' (RowDescription) messages to build result set metadata, including column names, types, and format information for subsequent data rows.

## Definition

```c
structed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);
```
## Detailed Description
This function parses RowDescription messages from the PostgreSQL server, which contain metadata about the columns that will be returned in subsequent DataRow messages. It creates or updates a PGresult structure with attribute descriptors for each column, including column names, table/column IDs, data types, type lengths, type modifiers, and format codes. The function handles both regular query results and DESCRIBE command results differently.

For DESCRIBE commands on prepared statements, it reuses an existing PGresult that may have been created by getParamDescriptions. For regular queries, it creates a new PGresult with PGRES_TUPLES_OK status. The function also determines whether the result set will be in binary or text format based on the format codes of all columns.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection
- : Length of the RowDescription message being processed

## Dependencies
- Functions called/Symbols referenced:
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md) (creates empty result structures)
  - [pqGetInt](../p/pqGetInt.md) (reads integer values from input buffer)
  - [libpq_gettext](../l/libpq_gettext.md) (translates error messages)
  - [pqResultAlloc](../p/pqResultAlloc.md) (allocates memory within result structure)
  - MemSet (zeroes memory)
  - [pqGets](../p/pqGets.md) (reads string values from input buffer)
  - [pqResultStrdup](../p/pqResultStrdup.md) (duplicates strings within result structure)
  - [PQclear](../P/PQclear.md) (cleans up result structures)
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md) (clears connection's async result)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends to error message buffer)
  - [pqSaveErrorResult](../p/pqSaveErrorResult.md) (saves error result)
  - PGQUERY_DESCRIBE (query class constant)
  - PGRES_COMMAND_OK (result status constant)
  - PGRES_TUPLES_OK (result status constant)
  - PGASYNC_READY (async status constant)
  - [PGresAttDesc](../P/PGresAttDesc.md) (attribute descriptor structure)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (from src/interfaces/libpq/fe-protocol3.c:332)
  - VALID_LONG_MESSAGE_TYPE (from src/interfaces/libpq/fe-protocol3.c:47)

## Notes and Other Information
- This is a static function, only accessible within fe-protocol3.c
- Returns 0 on success, EOF to suspend parsing (though EOF is not currently used)
- Handles both text and binary format result sets, setting the binary flag appropriately
- Implements comprehensive error handling with memory cleanup and meaningful error messages
- For DESCRIBE operations, the function sets the connection to READY status immediately
- Properly handles signed/unsigned integer conversions for 2-byte values
- Critical for establishing the structure of result sets before data rows arrive
- The function advances the input cursor on error to prevent protocol parsing issues

## Simplified Source

```c
static int
getRowDescriptions(PGconn *conn, int msgLength)
{
    PGresult *result;
    int numFields;

    // Determine if this is for a DESCRIBE command or regular query
    if (!conn->cmd_queue_head ||
        (conn->cmd_queue_head && conn->cmd_queue_head->queryclass == PGQUERY_DESCRIBE))
    {
        // Use existing result for DESCRIBE, or create new one
        if (conn->result)
            result = conn->result;
        else
            result = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);
    }
    else
    {
        // Create new result for regular query
        result = PQmakeEmptyPGresult(conn, PGRES_TUPLES_OK);
    }

    if (!result)
        goto error_out_of_memory;

    // Read number of fields
    if (pqGetInt(&(result->numAttributes), 2, conn))
        goto error_insufficient_data;
    numFields = result->numAttributes;

    // Allocate space for attribute descriptors
    if (numFields > 0)
    {
        result->attDescs = (PGresAttDesc *)
            pqResultAlloc(result, numFields * sizeof(PGresAttDesc), true);
        if (!result->attDescs)
            goto error_out_of_memory;
        MemSet(result->attDescs, 0, numFields * sizeof(PGresAttDesc));
    }

    // All columns binary only if ALL are binary format
    result->binary = (numFields > 0) ? 1 : 0;

    // Read metadata for each field
    for (int i = 0; i < numFields; i++)
    {
        int table_id, column_id, type_id, type_len, type_mod, format;

        // Read field metadata from protocol stream
        if (pqGets(&conn->workBuffer, conn) ||
            pqGetInt(&table_id, 4, conn) ||
            pqGetInt(&column_id, 2, conn) ||
            pqGetInt(&type_id, 4, conn) ||
            pqGetInt(&type_len, 2, conn) ||
            pqGetInt(&type_mod, 4, conn) ||
            pqGetInt(&format, 2, conn))
            goto error_insufficient_data;

        // Convert unsigned 2-byte values to signed
        column_id = (int) ((int16) column_id);
        type_len = (int) ((int16) type_len);
        format = (int) ((int16) format);

        // Store field metadata
        result->attDescs[i].name = pqResultStrdup(result, conn->workBuffer.data);
        if (!result->attDescs[i].name)
            goto error_out_of_memory;

        result->attDescs[i].tableid = table_id;
        result->attDescs[i].columnid = column_id;
        result->attDescs[i].format = format;
        result->attDescs[i].typid = type_id;
        result->attDescs[i].typlen = type_len;
        result->attDescs[i].atttypmod = type_mod;

        // If any field is text format, entire result is text
        if (format != 1)
            result->binary = 0;
    }

    // Success: store result
    conn->result = result;

    // For DESCRIBE operations, mark connection ready immediately
    if ((!conn->cmd_queue_head) ||
        (conn->cmd_queue_head && conn->cmd_queue_head->queryclass == PGQUERY_DESCRIBE))
    {
        conn->asyncStatus = PGASYNC_READY;
    }

    return 0;

error_insufficient_data:
    if (result && result != conn->result)
        PQclear(result);
    pqClearAsyncResult(conn);
    appendPQExpBuffer(&conn->errorMessage, "%s\n",
                     libpq_gettext("insufficient data in \"T\" message"));
    pqSaveErrorResult(conn);
    conn->inCursor = conn->inStart + 5 + msgLength;
    return 0;

error_out_of_memory:
    if (result && result != conn->result)
        PQclear(result);
    pqClearAsyncResult(conn);
    appendPQExpBuffer(&conn->errorMessage, "%s\n",
                     libpq_gettext("out of memory for query result"));
    pqSaveErrorResult(conn);
    conn->inCursor = conn->inStart + 5 + msgLength;
    return 0;
}
```