# gzip_pfree

## Location
[src/backend/backup/basebackup_gzip.c:299-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_gzip.c#L299-L304)

## Overview
This function serves as a wrapper around PostgreSQL's pfree() to provide memory deallocation functionality compatible with the zlib library's expected deallocation function signature.

## Definition
```c
static void gzip_pfree(void *opaque, void *address)
```

## Detailed Description
The `gzip_pfree` function is a static wrapper function that adapts PostgreSQL's standard `pfree` memory deallocation function to match the function signature expected by the zlib compression library. The zlib library requires deallocation functions that take an opaque pointer and the address to be freed as parameters. This wrapper performs the necessary signature translation by ignoring the opaque parameter and calling PostgreSQL's native pfree function with the address.

This function works in conjunction with `gzip_palloc` to provide complete memory management functionality for zlib operations. It is used when initializing zlib compression streams (z_stream structures) where the `zfree` member needs to point to a compatible deallocation function. Like its allocation counterpart, this function appears in two locations in the PostgreSQL codebase: in the backend's backup sink functionality and in the pg_basebackup client utility.

## Parameters / Member Variables
- `opaque`: Pointer to user data (unused in this implementation, required for zlib compatibility)
- `address`: Pointer to the memory block to be freed

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL's memory deallocation function)
- Called from (representative examples):
  - [bbsink_gzip_begin_archive](../b/bbsink_gzip_begin_archive.md) (assigned to zs->zfree)
  - [bbstreamer_gzip_decompressor_new](../b/bbstreamer_gzip_decompressor_new.md) (assigned to zs->zfree)

## Notes and Other Information
- This is a static function with identical implementations in two separate files
- The `opaque` parameter is ignored as PostgreSQL's pfree doesn't require context information
- Used exclusively for zlib compression/decompression stream initialization
- Forms a memory management pair with `gzip_palloc` for complete zlib integration
- The function simply delegates the deallocation operation to PostgreSQL's pfree
- Part of the memory management interface between PostgreSQL and the zlib library