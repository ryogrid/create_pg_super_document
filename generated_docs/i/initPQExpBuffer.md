# initPQExpBuffer

## Location
[src/interfaces/libpq/pqexpbuffer.c:90-113](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/libpq/pqexpbuffer.c#L90-L113)

## Overview
Initializes a PQExpBufferData structure to represent an empty string by allocating the initial data buffer and setting up the structure fields.

## Definition

```c
void
initPQExpBuffer(PQExpBuffer str)
```
## Detailed Description
This function initializes a PQExpBufferData structure that has previously undefined contents. It attempts to allocate an initial buffer of  bytes and properly sets up all structure fields. The function handles two scenarios:

1. **Successful allocation**: Sets up a valid buffer with the initial size, length 0, and null-terminates the string
2. **Failed allocation**: Puts the buffer into a "broken" state by pointing to the static  and setting both length and maxlen to 0

This function is one of two standard ways to create a PQExpBuffer. Use this when you have a pre-allocated PQExpBufferData structure (e.g., as a local variable or struct member) that needs initialization.

## Parameters / Member Variables
- : Pointer to the PQExpBufferData structure to be initialized (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function for memory allocation)
  -  (PostgreSQL utility macro for const casting)
  -  (constant defining initial buffer size, typically 256 bytes)
  -  (static pointer to the out-of-memory buffer)

- Called from (representative examples):
  -  (after allocating the PQExpBufferData structure)
  -  (when resetting a broken buffer)
  - Numerous libpq client applications and PostgreSQL utilities
  - Various PostgreSQL backend utilities like , , , etc.

## Notes and Other Information
- The function assumes the PQExpBufferData structure is already allocated
- If memory allocation fails, the buffer enters a broken state but the function still succeeds
- Always null-terminates the initial buffer when allocation succeeds
- The initial buffer size is defined by  (256 bytes by default)
- This function is part of the public libpq API
- Use  to clean up the data buffer when done (but not the structure itself)
- Widely used throughout PostgreSQL tooling for string buffer management