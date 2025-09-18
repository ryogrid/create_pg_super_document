# bbsink_zstd_cleanup

## Location
src/backend/backup/basebackup_zstd.c: 313 - 325

## Overview
Performs cleanup of Zstandard compression resources when a base backup operation fails, preventing memory leaks by freeing any allocated compression context.

## Definition
```c
static void bbsink_zstd_cleanup(bbsink *sink)
```

## Detailed Description
This function serves as an error handling cleanup routine for the Zstandard backup sink. It is called when a base backup operation fails or is aborted, ensuring that any allocated compression context is properly freed to prevent memory leaks. The function checks if a compression context exists and frees it if necessary, then sets the pointer to NULL to prevent double-free scenarios. This is crucial for maintaining system stability when backup operations encounter errors.

## Parameters / Member Variables
- `sink`: Pointer to the base backup sink structure that needs cleanup

## Dependencies
- Functions called/Symbols referenced:
  - ZSTD_freeCCtx (from libzstd)
- Called from (representative examples):
  - (No direct callers found - likely called through function pointer in bbsink vtable during error handling)

## Notes and Other Information
- This is a static function, part of the internal implementation of the Zstandard backup sink
- Designed specifically for error scenarios where normal cleanup via bbsink_zstd_end_backup might not occur
- The function is idempotent - it can be called multiple times safely due to the NULL check
- Essential for proper resource management in PostgreSQL's backup infrastructure
- Works in conjunction with bbsink_zstd_end_backup but handles abnormal termination cases