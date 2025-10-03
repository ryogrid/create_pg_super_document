# defaultGetLocalPQExpBuffer

## Location
[src/fe_utils/string_utils.c:42-68](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/string_utils.c#L42-L68)

## Overview
A static function that provides a temporary PQExpBuffer for internal use by identifier formatting functions, implementing a simple buffer reuse mechanism to reduce memory allocation overhead.

## Definition

```c
static PQExpBuffer
defaultGetLocalPQExpBuffer(void)
```
## Detailed Description
This function serves as the default implementation for the getLocalPQExpBuffer function pointer. It maintains a single static PQExpBuffer that is reused across calls, providing an efficient way to obtain temporary string buffers for identifier formatting operations like fmtId() and fmtQualifiedId().

The function implements a lazy initialization pattern where the buffer is created only on the first call, and subsequent calls simply reset the existing buffer contents rather than allocating new memory. This approach reduces memory leakage while maintaining simplicity.

The function is explicitly marked as non-reentrant and non-thread-safe due to its use of static storage. For multi-threaded applications, PostgreSQL provides alternative implementations that can be assigned to the getLocalPQExpBuffer function pointer.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - [resetPQExpBuffer](../r/resetPQExpBuffer.md) (for clearing existing buffer contents)
  - [createPQExpBuffer](../c/createPQExpBuffer.md) (for initial buffer allocation)
- Called from (representative examples):
  - Assigned to getLocalPQExpBuffer function pointer as default implementation
  - Used indirectly through getLocalPQExpBuffer() calls in fmtId and related functions

## Notes and Other Information
- The function uses a static variable id_return to store the buffer between calls
- First call creates a new buffer using createPQExpBuffer()
- Subsequent calls reuse the same buffer after calling resetPQExpBuffer() to clear contents
- This implementation is replaced in multi-threaded contexts (like pg_dump parallel mode) with thread-safe alternatives
- The buffer remains valid until the next call to this function, making it suitable only for temporary string operations
- Part of PostgreSQL's frontend utility library for string processing operations