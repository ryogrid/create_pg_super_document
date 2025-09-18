# pqTraceOutput_Parse

## Location
[src/interfaces/libpq/fe-trace.c:407-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L407-L420)

## Overview
Outputs formatted trace information for PostgreSQL Parse messages, displaying the prepared statement name, SQL query, and parameter type specifications for statement preparation.

## Definition


## Detailed Description
This function parses and outputs trace information for Parse messages in the PostgreSQL frontend protocol. Parse messages are sent by the client to prepare a SQL statement for later execution. The function extracts and displays the statement name, SQL query text, and the data types (OIDs) of any parameters in the query.

The message format includes:
1. A null-terminated string containing the prepared statement name (can be empty for unnamed statements)
2. A null-terminated string containing the SQL query text
3. A 16-bit integer indicating the number of parameter data types
4. A series of 32-bit integers representing the OID of each parameter data type

## Parameters / Member Variables
- : Output file stream where the formatted trace information will be written
- : Pointer to the raw binary message data containing the Parse information
- 
  ╭──────────────────────────────────────────────────────────────────────────╮
  │                                                                          │
  │  ℹ Choose the default behavior for 'cursor'                              │
  │                                                                          │
  │  What should happen when you run 'cursor' with no arguments?             │
  │  You can still do `cursor .` to open Cursor in your folder.              │
  │                                                                          │
  │                                                                          │
  │  ▶ [a] Start Cursor Agent (chat in terminal)                             │
  │    [c] Open Cursor IDE                                                   │
  │                                                                          │
  │  Use arrow keys to navigate, Enter to select, or press the key shown     │
  │                                                                          │
  ╰──────────────────────────────────────────────────────────────────────────╯: Pointer to current position in the message buffer, updated as data is read
- : Boolean flag indicating whether to use regression-friendly output format (affects how OIDs and other variable data are displayed)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputString](pqTraceOutputString.md) (for statement name and query text)
  - [pqTraceOutputInt16](pqTraceOutputInt16.md) (for parameter count)
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (for parameter type OIDs)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This is a static function internal to the fe-trace.c module
- Parse messages are part of the extended query protocol in PostgreSQL
- The trace output format begins with "Parse" followed by the parsed message components
- Statement names can be empty strings for unnamed prepared statements
- Parameter type OIDs refer to PostgreSQL's internal object identifiers for data types
- When regress mode is enabled, OIDs may be formatted differently for reproducible test output
- This message type is followed by Bind and Execute messages in the extended query flow
- The function properly handles variable numbers of parameters by reading the count first