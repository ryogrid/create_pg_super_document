# executor_errposition

## Location
[src/backend/executor/execUtils.c:870-896](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execUtils.c#L870-L896)

## Overview
Reports cursor position information during execution-time errors by converting byte offsets to character positions for user-friendly error messages.

## Definition
```c
int executor_errposition(EState *estate, int location)
```

## Detailed Description
This function is used within ereport() calls to provide cursor position information when reporting execution-time errors. It converts byte offsets (which are stored in parse trees for efficiency) into 1-based character indexes that are more meaningful to users. The function handles multibyte character encoding by using pg_mbstrlen_with_len to properly count characters rather than bytes. If no location is provided (negative value) or if the source text is unavailable, the function returns 0 (no-op). This design avoids performance overhead during normal execution while providing helpful error context when needed.

## Parameters / Member Variables
- `estate`: Execution state containing the source query text
- `location`: Byte offset in the source text where the error occurred (negative means no location available)

## Dependencies
- Functions called/Symbols referenced:
  - [pg_mbstrlen_with_len](../p/pg_mbstrlen_with_len.md)
  - [errposition](errposition.md)
- Called from (representative examples):
  - [ExecInitFunc](../E/ExecInitFunc.md)
  - [ExecInitSubscriptingRef](../E/ExecInitSubscriptingRef.md)
  - [init_sexpr](../i/init_sexpr.md)

## Notes and Other Information
- Designed for use within ereport() error reporting calls
- Always returns 0 as a dummy value (actual position reporting is handled by errposition())
- Handles multibyte character encodings correctly for accurate cursor positioning
- Gracefully handles cases where source text or location information is unavailable
- Optimized for performance - avoids character counting during normal execution, only does it during error conditions
- Part of PostgreSQL's comprehensive error reporting infrastructure for user-friendly diagnostics