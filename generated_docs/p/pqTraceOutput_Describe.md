# pqTraceOutput_Describe

## Location
src/interfaces/libpq/fe-trace.c: 291 - 299

## Overview
Outputs a formatted trace of a PostgreSQL Describe message to a file stream, parsing and displaying the object type and name to be described.

## Definition
```c
static void pqTraceOutput_Describe(FILE *f, const char *message, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing functionality and handles the parsing and output formatting of Describe protocol messages. The Describe message is used in the PostgreSQL extended query protocol to request information about a prepared statement or portal. The function parses the message components:

1. Outputs the "Describe" message type identifier
2. Extracts and displays the object type (single byte: 'S' for statement, 'P' for portal)
3. Extracts and displays the name of the object to be described

When a client sends a Describe message, the server responds with metadata about the specified object, such as parameter types for prepared statements or result column information for portals.

## Parameters / Member Variables
- `f`: FILE pointer to the output stream where trace information will be written
- `message`: Pointer to the raw protocol message buffer containing the Describe message data
- `cursor`: Pointer to an integer tracking the current parsing position within the message buffer

## Dependencies
- Functions called/Symbols referenced:
  - [pqTraceOutputByte1](pqTraceOutputByte1.md) (for the object type indicator)
  - [pqTraceOutputString](pqTraceOutputString.md) (for the object name)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message tracing dispatcher)

## Notes and Other Information
- This is a static function within fe-trace.c, making it internal to the libpq tracing implementation
- The object type byte contains 'S' to describe a prepared statement or 'P' to describe a portal
- Describing a statement returns parameter information; describing a portal returns result column information
- The function assumes the message buffer contains a valid Describe message and does not perform extensive error checking
- Part of PostgreSQL's debugging and development tools for analyzing client-server protocol communication
- The Describe message is part of the extended query protocol and enables clients to obtain metadata about prepared statements and portals before execution
- This function has the same structure as pqTraceOutput_Close, as both messages share the same format (type byte + name string)