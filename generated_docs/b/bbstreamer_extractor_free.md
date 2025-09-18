# bbstreamer_extractor_free

## Location
[src/bin/pg_basebackup/bbstreamer_file.c:390-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_file.c#L390-L396)

## Overview
Releases memory allocated for a bbstreamer_extractor instance, including its basepath and the extractor structure itself.

## Definition


## Detailed Description
This function serves as the memory cleanup callback for the bbstreamer_extractor type. It is the final phase of the bbstreamer lifecycle, called after finalization to properly deallocate all memory associated with the extractor instance. The function specifically handles two memory cleanup operations: freeing the basepath string that stores the base directory path for extraction, and freeing the extractor structure itself.

This function is part of the bbstreamer framework's three-phase lifecycle where memory cleanup is separated from processing logic. This approach is necessary because the pg_basebackup utility runs in a frontend environment without PostgreSQL's memory context system, requiring explicit memory management.

## Parameters / Member Variables
- : A pointer to the bbstreamer base structure, which is cast to bbstreamer_extractor to access extractor-specific fields for cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base type for casting)
  - [bbstreamer_extractor](bbstreamer_extractor.md) (specific extractor type for casting)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
- Called from (representative examples):
  - This is a static function with no direct external callers, used as a callback through the bbstreamer_ops function pointer table

## Notes and Other Information
- This is a static function internal to the bbstreamer_file.c module
- The function performs two pfree operations: first for the basepath string, then for the extractor structure itself
- Unlike bbstreamer_extractor_finalize, this function performs actual cleanup operations rather than just validation
- The cleanup order is important: free the basepath member first, then the containing structure
- This function should only be called after finalization has completed successfully
- Located in src/bin/pg_basebackup/bbstreamer_file.c at lines 390-396