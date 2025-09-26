# DecodingOutputState

## Location
[src/backend/replication/logical/logicalfuncs.c:40-46](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/replication/logical/logicalfuncs.c#L40-L46)

## Overview
DecodingOutputState is a private data structure that manages output state for logical decoding operations, storing decoded changes into a tuplestore for SQL-callable logical replication functions.

## Definition

```c
typedef struct DecodingOutputState
{
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
	bool		binary_output;
	int64		returned_rows;
} DecodingOutputState;
```
## Detailed Description
DecodingOutputState serves as a container for managing the output state during logical decoding operations in PostgreSQL's logical replication system. This structure is specifically designed for the SQL-callable logical decoding functions like `pg_logical_slot_get_changes()` and `pg_logical_slot_peek_changes()`. It encapsulates all necessary information for storing decoded logical replication changes into a tuplestore, which can then be returned as a result set to SQL clients.

The structure acts as a bridge between the logical decoding framework and the SQL interface, handling the conversion of decoded changes into a format suitable for SQL consumption. It manages both textual and binary output formats, tracks the number of rows returned, and maintains the tuple structure definition for consistent output formatting.

## Parameters / Member Variables
- `tupstore`: Pointer to a Tuplestorestate that stores the decoded changes as tuples for return to the SQL client
- `tupdesc`: TupleDesc defining the structure and types of tuples being stored (typically lsn, xid, data columns)
- `binary_output`: Boolean flag indicating whether output should be in binary format (true) or textual format (false)
- `returned_rows`: Counter tracking the total number of rows/changes that have been returned to the client

## Dependencies
- Functions called/Symbols referenced:
  - [Tuplestorestate](../T/Tuplestorestate.md) (tuple storage management)
- Called from (representative examples):
  - [LogicalOutputWrite](../L/LogicalOutputWrite.md) (at src/backend/replication/logical/logicalfuncs.c:67,73)
  - [pg_logical_slot_get_changes_guts](../p/pg_logical_slot_get_changes_guts.md) (at src/backend/replication/logical/logicalfuncs.c:114,143)

## Notes and Other Information
This structure is allocated and initialized in `pg_logical_slot_get_changes_guts()` using `palloc0()` and is used as the `output_writer_private` data for the logical decoding context. The structure is specifically designed for the SQL interface to logical decoding and is not used in other logical replication contexts like streaming replication or logical replication workers. The `binary_output` flag affects how the decoded data is encoded before being stored in the tuplestore, with textual output being verified for proper database encoding.