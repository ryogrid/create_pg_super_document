# EndCompressorGzip

## Location
src/bin/pg_dump/compress_gzip.c: 144 - 151

## Overview
Public interface function that safely finalizes gzip compression by checking if compression was initialized before calling the cleanup routine.

## Definition
```c
static void EndCompressorGzip(ArchiveHandle *AH, CompressorState *cs)
```

## Detailed Description
This function serves as a safe wrapper around DeflateCompressorEnd, providing a layer of protection against calling compression cleanup routines when compression was never initialized. It checks if the private_data field in the CompressorState is non-null before proceeding with the actual deflate compression cleanup. This design pattern ensures that the function can be called safely even in scenarios where compression initialization may have failed or never occurred.

The function acts as the public interface for ending gzip compression operations, delegating the actual cleanup work to DeflateCompressorEnd when appropriate.

## Parameters / Member Variables
- `AH`: ArchiveHandle pointer for the pg_dump archive being processed
- `cs`: CompressorState pointer containing the compression state to be finalized

## Dependencies
- Functions called/Symbols referenced:
  - DeflateCompressorEnd (for actual compression cleanup)
- Types referenced:
  - ArchiveHandle
  - CompressorState
- Called from (representative examples):
  - No direct references found (likely used via function pointer in compression interface)

## Notes and Other Information
- Provides safe cleanup by checking cs->private_data before calling DeflateCompressorEnd
- Acts as a defensive programming measure to prevent cleanup of uninitialized compression state
- Serves as the public interface function for gzip compression cleanup in the pg_dump compression framework
- Very simple wrapper function that primarily provides safety checks
- The function is static and located in src/bin/pg_dump/compress_gzip.c:144-151