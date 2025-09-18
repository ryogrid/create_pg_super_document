# pqTraceOutput_CopyBothResponse

## Location
src/interfaces/libpq/fe-trace.c: 494 - 503

## Overview
Outputs a formatted trace message for PostgreSQL's CopyBothResponse backend message, displaying the copy format type and column format codes for bidirectional copy operations.

## Definition
```c
static void pqTraceOutput_CopyBothResponse(FILE *f, const char *message, int *cursor, int length)
```

## Detailed Description
This function is part of PostgreSQL's libpq client library tracing system that handles the parsing and output formatting of CopyBothResponse messages received from the PostgreSQL backend. CopyBothResponse messages are sent by the server to initiate a bidirectional copy operation, which is typically used for streaming replication and logical replication protocols.

The function first reads a single byte indicating the overall copy format (0 for text, 1 for binary), then iterates through the remaining message data to read 16-bit integers representing the format codes for each column involved in the copy operation. The format codes specify whether each column will be transferred in text or binary format.

## Parameters / Member Variables
- `f`: FILE pointer to the trace output destination (typically stderr or a log file)
- `message`: Pointer to the message buffer containing the raw protocol message data
- `cursor`: Pointer to an integer tracking the current read position within the message buffer; updated as data is consumed
- `length`: Total length of the message, used to determine when all column format codes have been read

## Dependencies
- Functions called/Symbols referenced:
  - fprintf (standard C library)
  - [pqTraceOutputByte1](pqTraceOutputByte1.md) (reads and formats the overall copy format byte)
  - [pqTraceOutputInt16](pqTraceOutputInt16.md) (reads and formats each column's format code)
- Called from (representative examples):
  - [pqTraceOutputMessage](pqTraceOutputMessage.md) (main message dispatcher for trace output)

## Notes and Other Information
- This is a static function within fe-trace.c, part of the internal tracing infrastructure
- The function outputs "CopyBothResponse" as a tab-separated label followed by format information
- The first byte (overall format) is typically 0 for text format or 1 for binary format
- Each subsequent 16-bit integer represents a column format code (0 for text, 1 for binary)
- Used primarily for debugging streaming replication and logical replication setup
- CopyBothResponse differs from CopyInResponse and CopyOutResponse by supporting bidirectional data flow
- The number of column format codes depends on the number of columns in the relation being replicated