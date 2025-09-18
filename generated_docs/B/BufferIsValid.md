# BufferIsValid

## Location
src/include/storage/bufmgr.h: 355 - 370

## Overview
BufferIsValid is a static inline function that validates whether a given buffer number is valid as either a shared or local buffer in PostgreSQLs buffer management system.

## Definition
static inline bool BufferIsValid(Buffer bufnum)

## Detailed Description
BufferIsValid checks if a buffer number is valid by comparing it against the InvalidBuffer constant. The function includes assertion checks to ensure the buffer number is within valid ranges for both shared and local buffers. The function was historically designed to also check if a buffer was pinned, but this behavior was changed to avoid masking logic errors.

The function performs range validation through assertions:
- Ensures buffer number does not exceed NBuffers (for shared buffers)
- Ensures buffer number is not less than -NLocBuffer (for local buffers)

The actual validity check simply compares the buffer number against InvalidBuffer, returning true if they are not equal.

## Parameters / Member Variables
- bufnum: Buffer identifier to validate (type: Buffer)

## Dependencies
- Functions called/Symbols referenced:
  - InvalidBuffer (constant)
  - NBuffers (global variable for shared buffer count)
  - NLocBuffer (global variable for local buffer count)
  - Assert (macro for debugging assertions)
- Called from (representative examples):
  - Various buffer management functions throughout the codebase

## Notes and Other Information
- This function was historically equivalent to BufferIsPinned but was changed to avoid masking logic errors
- Range checks were moved to assertions to reduce overhead in production builds
- The function supports both positive buffer numbers (shared buffers) and negative buffer numbers (local buffers)
- InvalidBuffer is used as the sentinel value to indicate an invalid buffer