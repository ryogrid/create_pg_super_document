# destroyPQExpBuffer

## Location
src/interfaces/libpq/pqexpbuffer.c: 114 - 128

## Overview
Completely deallocates a PQExpBuffer by freeing both the data buffer and the PQExpBufferData structure itself, serving as the inverse operation to createPQExpBuffer.

## Definition

```c
void
destroyPQExpBuffer(PQExpBuffer str)
```
## Detailed Description
This function provides complete cleanup for a PQExpBuffer that was created with . It performs a two-step deallocation process:

1. First calls  to free the data buffer and reset the structure fields
2. Then frees the PQExpBufferData structure itself

The function safely handles NULL pointers by checking if the buffer exists before attempting to destroy it. This is the proper way to clean up dynamically allocated PQExpBuffers and prevents memory leaks.

## Parameters / Member Variables
- : Pointer to the PQExpBuffer to be destroyed (can be NULL, in which case the function does nothing)

## Dependencies
- Functions called/Symbols referenced:
  -  (cleans up the data buffer and resets structure fields)
  -  (standard C library function to deallocate the structure)

- Called from:
  - External libpq client applications (no internal references found in current analysis)

## Notes and Other Information
- This function is the inverse of  - use it to clean up buffers created with that function
- It's safe to call this function with a NULL pointer
- Do NOT use this function for PQExpBuffers initialized with  on stack-allocated or embedded structures - use  instead
- After calling this function, the PQExpBuffer pointer becomes invalid and should not be used
- This function is part of the public libpq API for client applications
- The function ensures complete cleanup by calling  first, then freeing the structure