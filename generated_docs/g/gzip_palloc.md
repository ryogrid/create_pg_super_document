# gzip_palloc

## Location
src/bin/pg_basebackup/bbstreamer_gzip.c: 350 - 359

## Overview
A wrapper function that adapts PostgreSQL's palloc() memory allocator to match the function signature expected by the zlib compression library.

## Definition
```c
static void *gzip_palloc(void *opaque, unsigned items, unsigned size)
```

## Detailed Description
This function serves as an adapter between PostgreSQL's memory management system and zlib's allocation callback requirements. The zlib library expects memory allocation functions to follow a specific signature (taking opaque pointer, item count, and item size parameters), while PostgreSQL's palloc() function only takes a size parameter. This wrapper function multiplies the items and size parameters to calculate the total memory needed and passes it to palloc(), effectively bridging the interface difference. The opaque parameter is ignored as PostgreSQL's memory context system doesn't require it for this use case.

## Parameters / Member Variables
- `opaque`: User data pointer (unused in this implementation, as PostgreSQL's memory context handles allocation tracking)
- `items`: Number of items to allocate
- `size`: Size of each item in bytes

## Dependencies
- Functions called/Symbols referenced:
  - palloc() (PostgreSQL's memory allocation function)
- Called from (representative examples):
  - bbsink_gzip_begin_archive (in basebackup_gzip.c:122)
  - bbstreamer_gzip_decompressor_new (in bbstreamer_gzip.c:229)

## Notes and Other Information
- This is a static function, only accessible within the basebackup_gzip.c compilation unit
- Designed to be used as a callback function for zlib's zalloc field in z_stream structures
- Part of PostgreSQL's integration with zlib for backup compression/decompression
- The function signature matches zlib's alloc_func typedef requirements
- Memory allocated through this function should be freed using the corresponding gzip_pfree() wrapper
- Essential for enabling PostgreSQL to use its own memory context system with zlib operations
- Ensures proper memory tracking and cleanup within PostgreSQL's memory management framework