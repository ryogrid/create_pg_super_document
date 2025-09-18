# bbsink_lz4_cleanup

## Location
src/backend/backup/basebackup_lz4.c: 285 - 296

## Overview
Cleanup function that frees the LZ4 compression context to prevent memory leaks when a basebackup operation fails or completes.

## Definition


## Detailed Description
The `bbsink_lz4_cleanup` function is a cleanup handler specifically designed for LZ4-compressed basebackup sinks. It serves as a safeguard to ensure that memory allocated for the LZ4 compression context is properly released in case the backup operation fails or needs to be terminated prematurely. The function casts the generic `bbsink` pointer to the specific `bbsink_lz4` type and checks if a compression context exists before freeing it using the LZ4F library function `LZ4F_freeCompressionContext()`. After freeing the context, it sets the pointer to NULL to prevent double-free errors.

## Parameters / Member Variables
- `sink`: Generic basebackup sink pointer that gets cast to `bbsink_lz4` type

## Dependencies
- Functions called/Symbols referenced:
  - `LZ4F_freeCompressionContext()` (from LZ4F library)
- Called from (representative examples):
  - No direct references found in the codebase (likely used as a cleanup callback)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the basebackup_lz4.c file
- The function is designed to be idempotent - it can be called multiple times safely due to the NULL check
- The function specifically targets the `ctx` member of the `bbsink_lz4` structure, which is of type `LZ4F_compressionContext_t`
- This cleanup function is essential for preventing memory leaks in LZ4-compressed basebackup operations
- The function follows PostgreSQL's pattern of having type-specific cleanup handlers for different sink types