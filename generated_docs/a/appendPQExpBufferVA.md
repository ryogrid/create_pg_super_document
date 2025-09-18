# appendPQExpBufferVA

## Location
[src/interfaces/libpq/pqexpbuffer.c:294-366](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L294-L366)

## Overview
The core implementation function for printf-style formatted text appending to PQExpBuffer, used by both printfPQExpBuffer and appendPQExpBuffer.

## Definition
```c
bool appendPQExpBufferVA(PQExpBuffer str, const char *fmt, va_list args)
```

## Detailed Description
The `appendPQExpBufferVA` function is the shared implementation core for all printf-style formatting operations in the PQExpBuffer system. It handles the complex logic of formatting text with variable arguments and managing buffer space efficiently.

Key operational aspects:
1. **Smart formatting attempt**: If sufficient space exists (more than 16 bytes), attempts formatting directly using `vsnprintf`
2. **Efficient space management**: For small available space, skips formatting and enlarges buffer first to avoid unnecessary work
3. **C99 vsnprintf compliance**: Relies on C99-compliant `vsnprintf` behavior for accurate space requirement reporting
4. **Overflow protection**: Guards against format results that would exceed INT_MAX
5. **Retry mechanism**: Returns false when buffer enlargement is needed, allowing callers to retry
6. **Error handling**: Marks buffer as broken on formatting errors or memory allocation failures

The function uses an optimization strategy where it only attempts formatting if there's reasonable space available, otherwise it preemptively enlarges the buffer.

## Parameters / Member Variables
- `str`: Pointer to the PQExpBuffer structure to append formatted text to
- `fmt`: Printf-style format string
- `args`: Variable argument list containing values for the format string

## Dependencies
- Functions called/Symbols referenced:
  - vsnprintf (C standard library)
  - markPQExpBufferBroken
  - [enlargePQExpBuffer](../e/enlargePQExpBuffer.md)
- Called from (representative examples):
  - [printfPQExpBuffer](../p/printfPQExpBuffer.md) (buffer replacement formatting)
  - [appendPQExpBuffer](appendPQExpBuffer.md) (buffer append formatting)
  - [libpq_append_error](../l/libpq_append_error.md) (error message formatting)
  - [libpq_append_conn_error](../l/libpq_append_conn_error.md) (connection error formatting)

## Notes and Other Information
- Returns true when operation is complete (success or failure), false when retry is needed
- Callers must preserve errno across retry loops since format strings may contain "%m"
- Uses a 16-byte threshold to decide whether to attempt formatting or enlarge first
- Assumes C99-compliant vsnprintf that returns required space on overflow
- Critical performance optimization: avoids repeated small buffer enlargements
- Part of the libpq expandable string buffer core implementation
- Handles both successful formatting and definitive failures (memory exhaustion, format errors)