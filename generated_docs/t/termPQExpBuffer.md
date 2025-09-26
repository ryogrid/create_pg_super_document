# termPQExpBuffer

## Location
src/interfaces/libpq/pqexpbuffer.c: 129 - 145

## Overview
Frees the data buffer of a PQExpBuffer and resets it to an empty but valid state, serving as the inverse operation to initPQExpBuffer.

## Definition

```c
void
termPQExpBuffer(PQExpBuffer str)
```
## Detailed Description
This function cleans up the data buffer associated with a PQExpBuffer while leaving the PQExpBufferData structure itself intact. It performs the following operations:

1. Checks if the current data buffer is not the static  (to avoid freeing static memory)
2. Frees the dynamically allocated data buffer if it exists
3. Points the buffer to the static  to maintain a valid empty state
4. Resets both  and  to 0

After calling this function, the PQExpBuffer is in a "broken" but safe state, similar to what  achieves. The structure remains valid and can be reinitialized later with .

## Parameters / Member Variables
- : Pointer to the PQExpBuffer whose data buffer should be terminated (must not be NULL)

## Dependencies
- Functions called/Symbols referenced:
  -  (standard C library function to deallocate the data buffer)
  -  (PostgreSQL utility macro for const casting)
  -  (static buffer for broken/empty state comparison)
  -  (static pointer to the out-of-memory buffer)

- Called from (representative examples):
  -  (before freeing the structure itself)
  - Numerous PostgreSQL utilities and libpq applications for cleanup
  - Error handling paths in various PostgreSQL tools like , , , etc.

## Notes and Other Information
- This function is the inverse of  - use it for PQExpBuffers initialized with that function
- Do NOT use this function to clean up PQExpBuffers created with  - use  instead
- The function safely handles buffers that are already in a broken state
- After calling this function, the PQExpBuffer structure can be reused by calling  again
- The function ensures the buffer remains in a valid empty state rather than leaving dangling pointers
- Widely used throughout PostgreSQL's frontend utilities for proper resource cleanup
- Part of the public libpq API for client applications