# getAnotherTuple

## Location
[src/interfaces/libpq/fe-protocol3.c:762-881](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-protocol3.c#L762-L881)

## Overview
getAnotherTuple processes PostgreSQL protocol 'D' (DataRow) messages to extract row data from the server and add it to the current result set through the row processor mechanism.

## Definition

```c
structed result with an error result. First
	 * discard the old result to try to win back some memory.
	 */
	pqClearAsyncResult(conn);
```
## Detailed Description
This function parses DataRow messages from the PostgreSQL server, which contain the actual data values for a single row in a query result. It validates the field count against the expected number of columns, dynamically manages a row buffer to hold field values, and processes each field by reading its length and storing a pointer to its data in the input buffer. After collecting all field data for the row, it delegates to pqRowProcessor to actually add the row to the result set.

The function implements an efficient approach by avoiding data copying - instead of copying field values, it stores pointers directly into the input buffer. This requires careful coordination with the input buffer management to ensure data remains valid when accessed by the row processor.

## Parameters / Member Variables
- : Pointer to the PGconn structure representing the database connection
- : Length of the DataRow message being processed

## Dependencies
- Functions called/Symbols referenced:
  - [pqGetInt](../p/pqGetInt.md) (reads integer values from input buffer)
  - [libpq_gettext](../l/libpq_gettext.md) (translates error messages)
  - realloc (dynamically resizes row buffer)
  - [pqSkipnchar](../p/pqSkipnchar.md) (advances input cursor past field data)
  - [pqRowProcessor](../p/pqRowProcessor.md) (processes the complete row data)
  - [pqClearAsyncResult](../p/pqClearAsyncResult.md) (clears connection's async result)
  - [appendPQExpBuffer](../a/appendPQExpBuffer.md) (appends to error message buffer)
  - [pqSaveErrorResult](../p/pqSaveErrorResult.md) (saves error result)
  - PGdataValue (structure holding field value and length)
- Called from (representative examples):
  - [pqParseInput3](../p/pqParseInput3.md) (from src/interfaces/libpq/fe-protocol3.c:386)
  - VALID_LONG_MESSAGE_TYPE (from src/interfaces/libpq/fe-protocol3.c:49)

## Notes and Other Information
- This is a static function, only accessible within fe-protocol3.c
- Returns 0 on success, EOF to suspend parsing (though EOF is not currently used)
- Implements zero-copy field access by storing pointers directly into the input buffer
- Dynamically resizes the row buffer (conn->rowBuf) as needed to accommodate varying column counts
- Validates that the field count in the message matches the expected number of columns from RowDescription
- Handles both NULL and non-NULL field values appropriately (NULL values have length -1)
- Field value pointers always point to valid buffer addresses even for NULL values to aid row processors
- Critical component in the query result processing pipeline, bridging protocol parsing and result construction
- Error handling includes memory cleanup and maintains protocol synchronization
- Works in conjunction with getRowDescriptions to provide complete result set processing