# pqTraceOutput_BackendKeyData

## Location
[src/interfaces/libpq/fe-trace.c:399-406](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L399-L406)

## Overview
Outputs formatted trace information for PostgreSQL BackendKeyData messages, displaying the process ID and secret key used for connection cancellation requests.

## Definition

```c
static void
pqTraceOutput_BackendKeyData(FILE *f, const char *message, int *cursor, bool regress)
```
## Detailed Description
This function parses and outputs trace information for BackendKeyData messages in the PostgreSQL frontend protocol. BackendKeyData messages are sent by the server during connection establishment to provide the client with a process ID and secret key. These values are used later if the client needs to send a cancellation request to interrupt a running query or operation on this connection.

The message format includes:
1. A 32-bit process ID of the backend process handling this connection
2. A 32-bit secret key for authentication of cancellation requests

## Parameters / Member Variables
- : Output file stream where the formatted trace information will be written
- : Pointer to the raw binary message data containing the BackendKeyData information
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
- : Boolean flag indicating whether to use regression-friendly output format (affects how sensitive data like process IDs are displayed)

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputInt32](pqTraceOutputInt32.md) (for both process ID and secret key)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md)

## Notes and Other Information
- This is a static function internal to the fe-trace.c module
- BackendKeyData is sent once per connection during the startup sequence
- The trace output format begins with "BackendKeyData" followed by the two 32-bit values
- The process ID and secret key are used together for connection cancellation via CancelRequest
- When regress mode is enabled, sensitive values may be masked or formatted differently for reproducible test output
- This message type is critical for the cancellation protocol but is only sent once per connection
- The secret key should be treated as sensitive information in production environments

## Simplified Source

```c
static void pqTraceOutput_BackendKeyData(FILE *f, const char *message, int *cursor, bool regress) {
    // Output backend key data message with process ID and secret key
    fprintf(f, "BackendKeyData\t");
    pqTraceOutputInt32(f, message, cursor, regress);  // Process ID
    pqTraceOutputInt32(f, message, cursor, regress);  // Secret key
}
```