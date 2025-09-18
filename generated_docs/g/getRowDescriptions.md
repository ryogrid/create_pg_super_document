# getRowDescriptions

## Location
src/interfaces/libpq/fe-protocol3.c: 503 - 673

## Overview
getRowDescriptions processes PostgreSQL protocol 'T' (RowDescription) messages to build result set metadata, including column names, types, and format information for subsequent data rows.

## Definition


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