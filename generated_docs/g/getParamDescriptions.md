# getParamDescriptions

## Location
[src/interfaces/libpq/fe-protocol3.c:674-761](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L674-L761)

## Overview
getParamDescriptions processes PostgreSQL protocol 't' (ParameterDescription) messages to build parameter metadata for prepared statements, storing parameter type information in a PGresult structure.

## Definition

```c
structed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);
```
## Detailed Description
This function parses ParameterDescription messages from the PostgreSQL server, which contain metadata about the parameters expected by a prepared statement. It creates a new PGresult structure with PGRES_COMMAND_OK status and populates it with parameter descriptors that include the data type OID for each parameter. This information is essential for properly formatting and binding parameters when executing prepared statements.

The function reads the number of parameters from the message, allocates space for parameter descriptors, and then iterates through each parameter to read its type OID. Unlike getRowDescriptions, this function only needs to store type information since parameters don't have names, table associations, or format specifications.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection
- : Length of the ParameterDescription message being processed

## Dependencies
- Functions called/Symbols referenced:
  - [PQmakeEmptyPGresult](../P/PQmakeEmptyPGresult.md) (creates empty result structure with COMMAND_OK status)
  - [pqGetInt](../p/pqGetInt.md) (reads integer values from input buffer)
  - [pqResultAlloc](../p/pqResultAlloc.md) (allocates memory within result structure)
  - MemSet (zeroes allocated memory)
  - [PQclear](../P/PQclear.md) (cleans up result structures on error)
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md) (clears connection's async result)
  - [libpq_gettext](../l/libpq_gettext.md) (translates error messages)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends to error message buffer)
  - [pqSaveErrorResult](../p/pqSaveErrorResult.md) (saves error result)
  - PGRES_COMMAND_OK (result status constant)
  - PGresParamDesc (parameter descriptor structure)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (from src/interfaces/libpq/fe-protocol3.c:377)
  - VALID_LONG_MESSAGE_TYPE (from src/interfaces/libpq/fe-protocol3.c:48)

## Notes and Other Information
- This is a static function, only accessible within fe-protocol3.c
- Returns 0 on success, EOF to suspend parsing (though EOF is not currently used)
- Creates a result with COMMAND_OK status rather than TUPLES_OK since no data rows follow
- Simpler than getRowDescriptions as it only needs to store type OIDs, not full column metadata
- Essential for proper parameter binding in prepared statement execution
- Implements robust error handling with memory cleanup and localized error messages
- The result may be reused by subsequent getRowDescriptions calls for DESCRIBE operations
- Properly advances input cursor on error to maintain protocol synchronization
- Memory allocation uses pqResultAlloc to ensure proper cleanup when the result is freed

## Simplified Source

```c
static int
getParamDescriptions(PGconn *conn, int msgLength)
{
    PGresult *result;
    int numParams;

    // Create result structure for parameter descriptions
    result = PQmakeEmptyPGresult(conn, PGRES_COMMAND_OK);
    if (!result)
        goto error_out_of_memory;

    // Read number of parameters
    if (pqGetInt(&(result->numParameters), 2, conn))
        goto error_insufficient_data;
    numParams = result->numParameters;

    // Allocate space for parameter descriptors
    if (numParams > 0)
    {
        result->paramDescs = (PGresParamDesc *)
            pqResultAlloc(result, numParams * sizeof(PGresParamDesc), true);
        if (!result->paramDescs)
            goto error_out_of_memory;
        MemSet(result->paramDescs, 0, numParams * sizeof(PGresParamDesc));
    }

    // Read type OID for each parameter
    for (int i = 0; i < numParams; i++)
    {
        int type_oid;
        if (pqGetInt(&type_oid, 4, conn))
            goto error_insufficient_data;
        result->paramDescs[i].typid = type_oid;
    }

    // Success: store result and return
    conn->result = result;
    return 0;

error_insufficient_data:
    if (result && result != conn->result)
        PQclear(result);
    pqClearAsyncResult(conn);
    appendPQExpBuffer(&conn->errorMessage, "%s\n",
                     libpq_gettext("insufficient data in \"t\" message"));
    pqSaveErrorResult(conn);
    conn->inCursor = conn->inStart + 5 + msgLength;
    return 0;

error_out_of_memory:
    if (result && result != conn->result)
        PQclear(result);
    pqClearAsyncResult(conn);
    appendPQExpBuffer(&conn->errorMessage, "%s\n",
                     libpq_gettext("out of memory"));
    pqSaveErrorResult(conn);
    conn->inCursor = conn->inStart + 5 + msgLength;
    return 0;
}
```