# _EndData

## Location
[src/bin/pg_dump/pg_backup_custom.c:329-349](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/pg_backup_custom.c#L329-L349)

## Overview
Finalizes the data writing process by terminating compression and writing an end marker to indicate the completion of table data dumping.

## Definition


## Detailed Description
The  function is a mandatory component of the custom archive format that handles the cleanup and finalization tasks after a dumper's  routine has completed writing table data. This function serves as the counterpart to , ensuring proper termination of the data writing process.

The function performs two critical tasks:
1. **Compression Termination**: Calls  to properly finalize the compression stream, flush any remaining compressed data, and clean up compression resources
2. **End Marker Writing**: Writes a zero integer value as an end marker to the archive stream, providing a clear delimiter that indicates the completion of the current data section

After compression cleanup, the function sets the compression state pointer to NULL, ensuring that any subsequent attempts to use the compressor will fail safely rather than accessing deallocated memory.

## Parameters / Member Variables
- : Archive handle containing the overall archive state and format configuration
- : Table of Contents entry representing the table data that has just finished being written

## Dependencies
- Functions called/Symbols referenced:
  -  - Finalizes compression stream and cleans up compression resources
  -  - Writes the end marker (zero value) to the archive stream
- Data structures used:
  -  - Local context for custom format containing compression state
  -  - General table of contents entry for the completed data section
- Called from:
  -  - Custom format initialization (function pointer assignment)
  -  - Directory format initialization
  -  - Null format initialization

## Notes and Other Information
- This function is marked as mandatory in the pg_dump architecture
- The zero end marker serves as a reliable delimiter for archive readers
- Proper cleanup of compression resources prevents memory leaks
- Setting  to NULL provides safety against use-after-free errors
- Forms a symmetric pair with  for data section lifecycle management
- Part of the pluggable archive format system enabling different storage backends
- The end marker helps distinguish between data content and control information during archive restoration