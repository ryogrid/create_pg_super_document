# pqTraceOutput_ParameterDescription

## Location
src/interfaces/libpq/fe-trace.c: 443 - 454

## Overview
Outputs a formatted trace message for ParameterDescription protocol messages in PostgreSQL client tracing.

## Definition
```c
static void pqTraceOutput_ParameterDescription(FILE *f, const char *message, int *cursor, bool regress)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure, specifically designed to format and output ParameterDescription protocol messages. It writes a "ParameterDescription" tab-delimited header followed by the number of parameters and their corresponding type OIDs. ParameterDescription messages are sent by the server to describe the data types of parameters in prepared statements, allowing clients to understand what types of values are expected for parameter binding.

## Parameters / Member Variables
- `f`: File pointer to the trace output destination
- `message`: Raw protocol message buffer containing the parameter description data
- `cursor`: Pointer to current position in the message buffer (updated as data is read)
- `regress`: Boolean flag indicating whether to use regression-friendly output format (affects OID display)

## Dependencies
- Functions called/Symbols referenced:
  - pqTraceOutputInt16
  - pqTraceOutputInt32
- Called from (representative examples):
  - pqTraceOutputMessage

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- Part of the PostgreSQL frontend tracing system for debugging client-server protocol communication
- First reads a 16-bit integer indicating the number of parameters
- Then iterates through each parameter, reading its 32-bit type OID
- The regress parameter controls whether actual OIDs or placeholder values are shown for reproducible test output
- Used in conjunction with prepared statements and extended query protocol
- Output format follows tab-delimited structure for easy parsing by analysis tools
- Type OIDs correspond to PostgreSQL internal data type identifiers