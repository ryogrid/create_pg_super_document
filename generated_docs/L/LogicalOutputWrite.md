# LogicalOutputWrite

## Location
[src/backend/replication/logical/logicalfuncs.c:62-98](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L62-L98)

## Overview
Writes logical decoding output data into a tuplestore for SQL interface consumption, converting the decoded changes into a structured format with LSN, transaction ID, and data columns.

## Definition
```c
static void LogicalOutputWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid, bool last_write)
```

## Detailed Description
This static function performs the actual writing of logical decoding output into a tuplestore structure. It takes the decoded logical replication data from the output buffer and formats it into a three-column tuple containing the log sequence number (LSN), transaction ID, and the actual change data. The function includes safety checks for output size limits and encoding verification for textual output. The resulting tuples are stored in a tuplestore that can be consumed by PostgreSQL SQL functions.

## Parameters / Member Variables
- `ctx`: LogicalDecodingContext pointer containing the decoding context and output buffer with decoded changes
- `lsn`: XLogRecPtr specifying the log sequence number where this change occurred
- `xid`: TransactionId of the transaction that generated this change
- `last_write`: Boolean flag indicating if this is the last write operation (currently unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [LSNGetDatum](LSNGetDatum.md) (converts LSN to PostgreSQL Datum)
  - [TransactionIdGetDatum](../T/TransactionIdGetDatum.md) (converts transaction ID to PostgreSQL Datum)  
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md) (retrieves current database encoding)
  - [pg_verify_mbstr](../p/pg_verify_mbstr.md) (verifies multibyte string encoding)
  - [cstring_to_text_with_len](../c/cstring_to_text_with_len.md) (converts C string to PostgreSQL text type)
  - [tuplestore_putvalues](../t/tuplestore_putvalues.md) (stores the tuple in the tuplestore)
- Data types used:
  - [LogicalDecodingContext](LogicalDecodingContext.md) (logical decoding context)
  - [DecodingOutputState](../D/DecodingOutputState.md) (output state tracking structure)
  - MaxAllocSize (maximum allocation size constant)
- Called from:
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md) (main function for retrieving logical changes)

## Notes and Other Information
- This is a static function, only accessible within logicalfuncs.c
- Implements size checking to prevent oversized output that would exceed PostgreSQL limitations
- Handles both binary and textual output modes with appropriate encoding verification
- The function creates a 3-column tuple: (LSN, TransactionID, Data)
- Maintains a count of returned rows in the DecodingOutputState structure
- Uses PostgreSQL Datum system for type-safe data handling
- Located in src/backend/replication/logical/logicalfuncs.c:62-98