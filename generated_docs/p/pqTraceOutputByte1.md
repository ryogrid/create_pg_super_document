# pqTraceOutputByte1

## Location
[src/interfaces/libpq/fe-trace.c:106-124](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/fe-trace.c#L106-L124)

## Overview
Outputs a single character from protocol message data to the trace log, formatting non-printable characters as hexadecimal values.

## Definition
```c
static void pqTraceOutputByte1(FILE *pfdebug, const char *data, int *cursor)
```

## Detailed Description
pqTraceOutputByte1 is a low-level utility function used internally by libpq's protocol tracing system to output individual bytes from message data. It intelligently formats the output by displaying printable ASCII characters as-is and non-printable characters (including control characters and the null terminator) in hexadecimal format. This dual formatting approach makes trace output human-readable while preserving the exact binary content of protocol messages. The function advances the cursor position after processing each byte, enabling sequential parsing of message data.

## Parameters / Member Variables
- `pfdebug`: File stream where the trace output should be written
- `data`: Pointer to the message data buffer being traced
- `cursor`: Pointer to the current position in the data buffer; incremented after processing

## Dependencies
- Functions called/Symbols referenced:
  - isprint (checks if character is printable)
  - fprintf (outputs formatted data to stream)
- Called from (representative examples):
  - [pqTraceOutput_Close](pqTraceOutput_Close.md) (line 261 in fe-trace.c)
  - [pqTraceOutput_Describe](pqTraceOutput_Describe.md) (line 294 in fe-trace.c)
  - [pqTraceOutputNR](pqTraceOutputNR.md) (line 309 in fe-trace.c)
  - [pqTraceOutput_CopyInResponse](pqTraceOutput_CopyInResponse.md) (line 378 in fe-trace.c)
  - [pqTraceOutput_CopyOutResponse](pqTraceOutput_CopyOutResponse.md) (line 391 in fe-trace.c)
  - [pqTraceOutput_CopyBothResponse](pqTraceOutput_CopyBothResponse.md) (line 497 in fe-trace.c)
  - [pqTraceOutput_ReadyForQuery](pqTraceOutput_ReadyForQuery.md) (line 507 in fe-trace.c)

## Notes and Other Information
- Static function - internal to fe-trace.c module
- Uses isprint() with unsigned char cast to handle extended ASCII properly
- Non-printable characters are displayed as \\x%02x format for clarity
- Automatically advances cursor for sequential byte processing
- Essential for tracing message terminators (null bytes) in ErrorResponse and NoticeResponse messages
- Part of the comprehensive protocol message tracing infrastructure
- Enables precise analysis of binary protocol data in human-readable format

## Simplified Source

```c
static void pqTraceOutputByte1(FILE *pfdebug, const char *data, int *cursor)
{
    const char *v = data + *cursor;

    // Format non-printable chars as hex, printable chars as-is
    if (!isprint((unsigned char) *v))
        fprintf(pfdebug, " \\x%02x", *v);
    else
        fprintf(pfdebug, " %c", *v);

    *cursor += 1;
}
```