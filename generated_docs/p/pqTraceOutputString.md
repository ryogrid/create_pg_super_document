# pqTraceOutputString

## Location
src/interfaces/libpq/fe-trace.c: 163 - 187

## Overview
A static utility function that reads a null-terminated string from a PostgreSQL protocol message buffer and outputs it to the trace log, with optional suppression of the actual content for security purposes.

## Definition
```c
static void pqTraceOutputString(FILE *pfdebug, const char *data, int *cursor, bool suppress)
```

## Detailed Description
This function is part of PostgreSQL's libpq tracing infrastructure. It extracts a null-terminated string from the protocol message data starting at the current cursor position and writes it to the trace output file enclosed in double quotes. When the suppress parameter is true, it outputs 'SSSS' instead of the actual string content, which is useful for hiding sensitive information like passwords or user data in trace logs.

The function calculates the string length and advances the cursor accordingly. It handles the cursor advancement differently depending on whether suppression is enabled - using strlen() when suppressed, or calculating from the fprintf() return value when not suppressed.

## Parameters / Member Variables
- `pfdebug`: FILE pointer to the trace output file where the string will be written
- `data`: Pointer to the message buffer containing the binary protocol data
- `cursor`: Pointer to the current position in the data buffer; updated by the string length + 1 (for null terminator) after reading
- `suppress`: Boolean flag indicating whether to output 'SSSS' instead of the actual string content

## Dependencies
- Functions called/Symbols referenced:
  - (Uses standard C library functions: fprintf, strlen)
- Called from (representative examples):
  - [pqTraceOutput_NotificationResponse](pqTraceOutput_NotificationResponse.md)
  - [pqTraceOutput_Bind](pqTraceOutput_Bind.md)
  - [pqTraceOutput_Close](pqTraceOutput_Close.md)
  - [pqTraceOutput_CommandComplete](pqTraceOutput_CommandComplete.md)
  - [pqTraceOutput_Describe](pqTraceOutput_Describe.md)
  - [pqTraceOutputNR](pqTraceOutputNR.md)
  - [pqTraceOutput_Execute](pqTraceOutput_Execute.md)
  - [pqTraceOutput_CopyFail](pqTraceOutput_CopyFail.md)
  - [pqTraceOutput_Parse](pqTraceOutput_Parse.md)
  - [pqTraceOutput_Query](pqTraceOutput_Query.md)
  - [pqTraceOutput_ParameterStatus](pqTraceOutput_ParameterStatus.md)
  - [pqTraceOutput_RowDescription](pqTraceOutput_RowDescription.md)

## Notes and Other Information
- This is a static function, only accessible within fe-trace.c
- The function assumes the string is properly null-terminated in the message buffer
- String output is always enclosed in double quotes for clear delimitation in trace logs
- The suppress feature is important for security when tracing messages containing sensitive data
- Cursor advancement calculation accounts for the formatting characters (space and quotes) when not suppressed
- The function has void return type, unlike the integer output functions which return the parsed value