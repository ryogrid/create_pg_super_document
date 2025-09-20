# pqTraceOutput_FunctionCall

## Location
[src/interfaces/libpq/fe-trace.c:347-372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L347-L372)

## Overview
Outputs formatted trace information for PostgreSQL FunctionCall messages, parsing and displaying the function parameters and their values in a human-readable format.

## Definition

```c
static void
pqTraceOutput_FunctionCall(FILE *f, const char *message, int *cursor, bool regress)
```
## Detailed Description
This function parses and outputs trace information for FunctionCall messages in the PostgreSQL frontend protocol. It processes the binary message data to extract function parameters, parameter values, and result format specifications. The function handles variable-length parameter data and formats the output for debugging and protocol analysis purposes.

The function follows the PostgreSQL FunctionCall message format:
1. Reads the function OID (object identifier)
2. Reads parameter format codes (number of codes and the codes themselves)
3. Reads parameter values (number of parameters and their binary data)
4. Reads the result format code

## Parameters / Member Variables
- : Output file stream where the formatted trace information will be written
- : Pointer to the raw binary message data containing the FunctionCall information
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
- : Boolean flag indicating whether to use regression-friendly output format (affects timestamp and other variable output)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (for function OID and parameter lengths)
  - [pqTraceOutputInt16](pqTraceOutputInt16.md) (for parameter format codes and counts)
  - [pqTraceOutputNchar](pqTraceOutputNchar.md) (for parameter value data)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This is a static function internal to the fe-trace.c module
- The function handles variable-length parameter data safely by checking for -1 length values (NULL parameters)
- The trace output format begins with "FunctionCall" followed by the parsed message components
- Parameter values are output as raw binary data using pqTraceOutputNchar
- The function properly advances the cursor through the message buffer to maintain synchronization with the protocol format