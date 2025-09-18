# read_relmap_file

## Location
[src/backend/utils/cache/relmapper.c:784-888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L784-L888)

## Overview
read_relmap_file loads and validates relation mapping data from disk files, providing the core functionality for reading both shared and local relation mapping files.

## Definition


## Detailed Description
read_relmap_file is the fundamental function that reads relation mapping files from disk into memory structures. It handles the complete process of opening, reading, validating, and verifying relation mapping files. The function implements proper locking, error handling, file I/O operations, and data integrity checks including CRC verification.

The function manages concurrent access by acquiring RelationMappingLock unless the caller already holds it. It opens the file only after acquiring the lock to avoid Windows file renaming issues, reads the entire RelMapFile structure, and performs comprehensive validation including magic number checks, bounds checking, and CRC verification to ensure data integrity.

## Parameters / Member Variables
- : Pointer to RelMapFile structure where the loaded data will be stored
- : Database path string ("global" for shared relations, or specific database path for local relations)
- : Boolean indicating whether caller already holds RelationMappingLock
- : Error level for reporting problems (must be at least ERROR)

## Dependencies
- Functions called/Symbols referenced:
  - LWLockAcquire/LWLockRelease (locking primitives)
  - OpenTransientFile/CloseTransientFile (file operations)
  - pgstat_report_wait_start/pgstat_report_wait_end (wait event reporting)
  - INIT_CRC32C/COMP_CRC32C/FIN_CRC32C/EQ_CRC32C (CRC calculation and verification)
  - ereport/errmsg (error reporting)
  - RELMAPPER_FILENAME constant
  - RELMAPPER_FILEMAGIC/MAX_MAPPINGS constants
- Called from (representative examples):
  - [RelationMapOidToFilenumberForDatabase](../R/RelationMapOidToFilenumberForDatabase.md) (at src/backend/utils/cache/relmapper.c:271)
  - [RelationMapCopy](../R/RelationMapCopy.md) (at src/backend/utils/cache/relmapper.c:299)
  - [load_relmap_file](../l/load_relmap_file.md) (at src/backend/utils/cache/relmapper.c:768, 770)

## Notes and Other Information
- This is a static function, only accessible within the relmapper.c file
- Implements proper concurrent access control using RelationMappingLock
- Performs comprehensive data validation including magic number, bounds checking, and CRC verification
- Uses wait event reporting for monitoring file I/O operations
- Handles platform-specific file locking considerations (especially for Windows)
- Critical for PostgreSQL system catalog access - failures at ERROR level or higher
- The sinval mechanism ensures re-reading if files are updated during operation
- Part of PostgreSQL's core relation mapping infrastructure for system catalogs