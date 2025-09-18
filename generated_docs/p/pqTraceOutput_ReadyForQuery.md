# pqTraceOutput_ReadyForQuery

## Location
[src/interfaces/libpq/fe-trace.c:504-513](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L504-L513)

## Overview
Outputs a formatted trace message for PostgreSQL's ReadyForQuery backend message, displaying the transaction status indicator that signals the backend is ready to accept new commands.

## Definition
```c
static void pqTraceOutput_ReadyForQuery(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq client library tracing system that handles the parsing and output formatting of ReadyForQuery messages received from the PostgreSQL backend. ReadyForQuery messages are sent by the server to indicate that it has finished processing the current command and is ready to accept a new query from the client.

The message contains a single byte that indicates the current transaction status of the backend session. This status byte is crucial for understanding the state of the database session and determining what operations are valid to perform next.

## Parameters / Member Variables
- `f`: FILE pointer to the trace output destination (typically stderr or a log file)
- `message`: Pointer to the message buffer containing the raw protocol message data
- `cursor`: Pointer to an integer tracking the current read position within the message buffer; updated as data is consumed

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library)
  - [pqTraceOutputByte1](pqTraceOutputByte1.md) (reads and formats the transaction status byte)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message dispatcher for trace output)

## Notes and Other Information
- This is a static function within fe-trace.c, part of the internal tracing infrastructure
- The function outputs "ReadyForQuery" as a tab-separated label followed by the status character
- The status byte can have the following values:
  - 'I' = Idle (not in a transaction block)
  - 'T' = In a transaction block
  - 'E' = In a failed transaction block (transaction will be rolled back)
- This message is sent after successful completion of commands like SELECT, INSERT, UPDATE, DELETE
- Critical for client libraries to understand when it's safe to send the next command
- Used for debugging connection state and transaction management issues
- The ReadyForQuery message marks the end of a query cycle in the PostgreSQL protocol