# appendStringInfoVA

## Location
[src/common/stringinfo.c:139-181](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/stringinfo.c#L139-L181)

## Overview
Low-level function that attempts to format text using a va_list and append it to a StringInfo, returning space needed if the buffer is insufficient.

## Definition

```c
int
appendStringInfoVA(StringInfo str, const char *fmt, va_list args)
```
## Detailed Description
The  function is the core implementation for printf-style formatting in StringInfo operations. It takes a va_list argument (variable argument list) and attempts to format the text into the available buffer space. If successful, it returns 0 and updates the string length. If the buffer is too small, it returns an estimate of the additional space needed without modifying the string content. The function includes an optimization that skips formatting attempts when very little space is available (less than 16 bytes) and returns a conservative estimate. It uses  for the actual formatting work and carefully preserves the string's null termination.

## Parameters / Member Variables
- : Pointer to the StringInfo structure to append to
- : printf-style format string
- : va_list containing the variable arguments for formatting

## Dependencies
- Functions called/Symbols referenced:
  - [pvsnprintf](../p/pvsnprintf.md) (platform-specific vsnprintf implementation)
  - Assert (debugging assertion)
- Called from (representative examples):
  - [appendStringInfo](appendStringInfo.md)
  - [manifest_report_error](../m/manifest_report_error.md)
  - [ReportWalSummaryError](../R/ReportWalSummaryError.md)
  - EVALUATE_MESSAGE
  - [PLy_elog_impl](../P/PLy_elog_impl.md)

## Notes and Other Information
- This function is located at src/common/stringinfo.c:139-181
- Returns 0 on success, or estimated space needed on failure
- Includes optimization to avoid formatting when available space is very small (< 16 bytes)
- Preserves string integrity by restoring null termination if formatting fails
- The API is noted as "ugly" due to C va_list limitations requiring va_start to be called by the caller
- Critical for PostgreSQL's error reporting and message formatting infrastructure
- Used extensively in error handling and logging throughout the codebase