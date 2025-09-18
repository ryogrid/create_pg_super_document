# pqTraceOutputInt16

## Location
[src/interfaces/libpq/fe-trace.c:125-143](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L125-L143)

## Overview
A static utility function that reads a 2-byte integer from a PostgreSQL protocol message buffer and outputs it to the trace log.

## Definition
```c
static int pqTraceOutputInt16(FILE *pfdebug, const char *data, int *cursor)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure. It extracts a 2-byte integer from the protocol message data at the current cursor position, converts it from network byte order to host byte order, and writes it to the trace output file. The function advances the cursor by 2 bytes and returns the converted integer value.

The function handles the byte order conversion using pg_ntoh16() to ensure correct interpretation of network-ordered data regardless of the host system's endianness.

## Parameters / Member Variables
- `pfdebug`: FILE pointer to the trace output file where the integer value will be written
- `data`: Pointer to the message buffer containing the binary protocol data
- `cursor`: Pointer to the current position in the data buffer; updated by 2 bytes after reading

## Dependencies
- Functions called/Symbols referenced:
  - pg_ntoh16 (network to host byte order conversion for 16-bit integers)
- Called from (representative examples):
  - [pqTraceOutput_Bind](pqTraceOutput_Bind.md)
  - [pqTraceOutput_DataRow](pqTraceOutput_DataRow.md)
  - [pqTraceOutput_FunctionCall](pqTraceOutput_FunctionCall.md)
  - [pqTraceOutput_CopyInResponse](pqTraceOutput_CopyInResponse.md)
  - [pqTraceOutput_CopyOutResponse](pqTraceOutput_CopyOutResponse.md)
  - [pqTraceOutput_Parse](pqTraceOutput_Parse.md)
  - [pqTraceOutput_ParameterDescription](pqTraceOutput_ParameterDescription.md)
  - [pqTraceOutput_RowDescription](pqTraceOutput_RowDescription.md)
  - [pqTraceOutput_CopyBothResponse](pqTraceOutput_CopyBothResponse.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Used extensively throughout the protocol tracing system for parsing 16-bit integer fields
- Always outputs the integer value prefixed with a space for consistent formatting
- The function modifies the cursor position as a side effect, enabling sequential parsing of protocol messages