# markPQExpBufferBroken

## Location
[src/interfaces/libpq/pqexpbuffer.c:50-71](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L50-L71)

## Overview
A static function that puts a PQExpBuffer into a "broken" (out-of-memory) state by cleaning up any existing buffer and setting it to point to a static empty buffer.

## Definition

```c
static void
markPQExpBufferBroken(PQExpBuffer str)
```
## Detailed Description
This function is responsible for transitioning a PQExpBuffer to a broken state when memory allocation failures occur. It safely deallocates any existing dynamically allocated buffer and redirects the buffer to point to a static, read-only empty string. This design ensures that:

1. Memory is properly cleaned up to prevent leaks
2. The buffer becomes safe for read operations (pointing to a valid empty string)
3. Any write attempts will fail harmlessly since the buffer points to read-only memory
4. The broken state is clearly identifiable via len=0 and maxlen=0

The function uses  to cast away the const qualifier when assigning the static buffer, which is a controlled violation of const-correctness for this specific use case.

## Parameters / Member Variables
- : Pointer to the PQExpBuffer structure to be marked as broken

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function)
  -  (PostgreSQL utility macro for const casting)
  -  (static pointer to the out-of-memory buffer)

- Called from:
  -  (when memory reallocation fails)
  -  (when buffer enlargement fails during append operations)

## Notes and Other Information
- This is a static function, only accessible within the pqexpbuffer.c module
- The function is designed to be safe to call multiple times on the same buffer
- After calling this function,  will return true
- The oom_buffer is intentionally placed in read-only memory to catch programming errors
- This function is part of PostgreSQL's robust error handling for memory allocation failures in the libpq client library