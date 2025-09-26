# exec_command_encoding

## Location
[src/bin/psql/command.c:1338-1376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/psql/command.c#L1338-L1376)

## Overview
Implements the \encoding command in psql, which allows users to set or display the client-side character encoding.

## Definition

```c
static backslashResult
exec_command_encoding(PsqlScanState scan_state, bool active_branch)
```
## Detailed Description
This function handles the \encoding command which can operate in two modes:
- Without arguments: displays the current client encoding using pg_encoding_to_char()
- With an encoding name argument: attempts to set the client encoding to the specified value

When setting a new encoding, the function updates the database connection's encoding, validates the change, and synchronizes various internal psql state variables including the encoding setting in the formatting options and the ENCODING psql variable.

## Parameters / Member Variables
- : Scanner state for parsing the command line arguments
- : Boolean indicating whether this command should be executed (used for conditional processing)

## Dependencies
- Functions called/Symbols referenced:
  - psql_scan_slash_option
  - [pg_encoding_to_char](../p/pg_encoding_to_char.md)
  - [PQsetClientEncoding](../P/PQsetClientEncoding.md)
  - [PQclientEncoding](../P/PQclientEncoding.md)
  - [setFmtEncoding](../s/setFmtEncoding.md)
  - [SetVariable](../S/SetVariable.md)
  - [ignore_slash_options](../i/ignore_slash_options.md)
  - [PsqlScanState](../P/PsqlScanState.md) (type)
  - OT_NORMAL (constant)
  - PSQL_CMD_SKIP_LINE (return value)
  - [backslashResult](../b/backslashResult.md) (return type)
- Called from (representative examples):
  - [exec_command](exec_command.md)
  - EditableObjectType (indirectly through command dispatch)

## Notes and Other Information
- If encoding setting fails, an error message is logged but execution continues
- The function maintains consistency across multiple encoding-related settings in psql's internal state
- Memory allocated for the encoding string argument is properly freed
- When active_branch is false, arguments are consumed but not processed
- The ENCODING psql variable is updated to reflect the new encoding for use in other contexts
- Encoding validation is handled by the underlying PostgreSQL client library functions