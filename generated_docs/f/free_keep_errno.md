# free_keep_errno

## Location
src/bin/pg_dump/compress_io.c: 179 - 194

## Overview
This static utility function frees memory without modifying the global errno variable, preserving error state for the caller.

## Definition


## Detailed Description
The `free_keep_errno` function provides a wrapper around the standard C library's free() function that preserves the current value of errno. This is important in error handling scenarios where memory needs to be cleaned up but the original error condition (represented by errno) must be maintained for proper error reporting to the caller.

The function saves the current errno value, calls the standard free() function to deallocate the memory, and then restores the original errno value. This prevents the free() call from potentially modifying errno and masking the original error condition.

## Parameters / Member Variables
- `p`: Pointer to the memory block to be freed (void pointer, can accept any pointer type)

## Dependencies
- Functions called/Symbols referenced:
  - free (standard C library function)
  - errno (global error variable)
- Called from (representative examples):
  - check_compressed_file (src/bin/pg_dump/compress_io.c:222)
  - InitDiscoverCompressFileHandle (src/bin/pg_dump/compress_io.c:276)
  - InitDiscoverCompressFileHandle (src/bin/pg_dump/compress_io.c:279)
  - EndCompressFileHandle (src/bin/pg_dump/compress_io.c:298)

## Notes and Other Information
- This is a static function, meaning it's only visible within the compress_io.c file
- Essential for maintaining proper error handling semantics in cleanup paths
- The function handles NULL pointers safely (since standard free() handles NULL)
- Used in error recovery scenarios where memory must be freed but errno must be preserved
- Common pattern in systems programming where cleanup operations should not interfere with error reporting
- Located in src/bin/pg_dump/compress_io.c at lines 179-194