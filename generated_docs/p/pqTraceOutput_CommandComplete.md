# pqTraceOutput_CommandComplete

## Location
[src/interfaces/libpq/fe-trace.c:266-272](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L266-L272)

## Overview
Outputs a formatted trace of a PostgreSQL CommandComplete message to a file stream, displaying the command completion status string.

## Definition
```c
static void pqTraceOutput_CommandComplete(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing functionality and handles the parsing and output formatting of CommandComplete protocol messages. The CommandComplete message is sent by the PostgreSQL server to indicate that a SQL command has finished executing successfully. The message contains a single component:

1. Outputs the "CommandComplete" message type identifier
2. Extracts and displays the command completion tag string

The command completion tag typically contains information about the command that was executed and may include details such as the command type (INSERT, UPDATE, DELETE, SELECT, etc.) and the number of rows affected. For example, "INSERT 0 1" indicates an INSERT command that inserted 1 row.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: Pointer to the raw protocol message buffer containing the CommandComplete message data
- `cursor`: Pointer to an integer tracking the current parsing position within the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputString](pqTraceOutputString.md) (for the command completion tag)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message tracing dispatcher)

## Notes and Other Information
- This is a static function within fe-trace.c, making it internal to the libpq tracing implementation
- The CommandComplete message is one of the simpler protocol messages, containing only a single string field
- Common command tags include "SELECT n", "INSERT oid n", "UPDATE n", "DELETE n", "CREATE TABLE", etc.
- This message indicates successful completion of a command; errors are reported through different message types
- Part of PostgreSQL's debugging and development tools for analyzing client-server protocol communication
- The function assumes the message buffer contains a valid CommandComplete message and does not perform extensive error checking

## Simplified Source

```c
static void pqTraceOutput_CommandComplete(FILE *f, const char *message, int *cursor) {
    // Output message type identifier
    fprintf(f, "CommandComplete\t");

    // Extract and display command completion tag (e.g., "INSERT 0 1", "UPDATE 5")
    pqTraceOutputString(f, message, cursor, false);
}
```