# read_relmap_file

## Location
[src/backend/utils/cache/relmapper.c:784-888](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/relmapper.c#L784-L888)

## Overview
read_relmap_file loads and validates relation mapping data from disk files, providing the core functionality for reading both shared and local relation mapping files.

## Definition

```c
static void
read_relmap_file(RelMapFile *map, char *dbpath, bool lock_held, int elevel)
```
## Detailed Description
read_relmap_file is the fundamental function that reads relation mapping files from disk into memory structures. It handles the complete process of opening, reading, validating, and verifying relation mapping files. The function implements proper locking, error handling, file I/O operations, and data integrity checks including CRC verification.

The function manages concurrent access by acquiring RelationMappingLock unless the caller already holds it. It opens the file only after acquiring the lock to avoid Windows file renaming issues, reads the entire RelMapFile structure, and performs comprehensive validation including magic number checks, bounds checking, and CRC verification to ensure data integrity.

## Parameters / Member Variables
- `*map`: Pointer to RelMapFile structure where the loaded data will be stored
- `*dbpath`: Database path string ("global" for shared relations, or specific database path for local relations)
- `lock_held`: Boolean indicating whether caller already holds RelationMappingLock
- `elevel`: Error level for reporting problems (must be at least ERROR)
## Dependencies
- Functions called/Symbols referenced:
  - [LWLockAcquire](../L/LWLockAcquire.md)/LWLockRelease (locking primitives)
  - [OpenTransientFile](../O/OpenTransientFile.md)/CloseTransientFile (file operations)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/pgstat_report_wait_end (wait event reporting)
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

## Simplified Source

```c
static void read_relmap_file(RelMapFile *map, char *dbpath, bool lock_held, int elevel) {
    char mapfilename[MAXPGPATH];
    pg_crc32c crc;
    int fd, r;

    // Acquire lock unless caller already holds it
    if (!lock_held)
        LWLockAcquire(RelationMappingLock, LW_SHARED);

    // Build filename and open the relation mapping file
    snprintf(mapfilename, sizeof(mapfilename), "%s/%s", dbpath, RELMAPPER_FILENAME);
    fd = OpenTransientFile(mapfilename, O_RDONLY | PG_BINARY);
    if (fd < 0)
        ereport(elevel, (errcode_for_file_access(),
                errmsg("could not open file \"%s\": %m", mapfilename)));

    // Read the entire RelMapFile structure
    pgstat_report_wait_start(WAIT_EVENT_RELATION_MAP_READ);
    r = read(fd, map, sizeof(RelMapFile));
    if (r != sizeof(RelMapFile)) {
        if (r < 0)
            ereport(elevel, (errcode_for_file_access(),
                    errmsg("could not read file \"%s\": %m", mapfilename)));
        else
            ereport(elevel, (errcode(ERRCODE_DATA_CORRUPTED),
                    errmsg("could not read file \"%s\": read %d of %zu",
                           mapfilename, r, sizeof(RelMapFile))));
    }
    pgstat_report_wait_end();

    // Close file and release lock
    CloseTransientFile(fd);
    if (!lock_held)
        LWLockRelease(RelationMappingLock);

    // Validate file contents: magic number and mapping count
    if (map->magic != RELMAPPER_FILEMAGIC ||
        map->num_mappings < 0 ||
        map->num_mappings > MAX_MAPPINGS)
        ereport(elevel, (errmsg("relation mapping file \"%s\" contains invalid data",
                               mapfilename)));

    // Verify CRC checksum
    INIT_CRC32C(crc);
    COMP_CRC32C(crc, (char *) map, offsetof(RelMapFile, crc));
    FIN_CRC32C(crc);

    if (!EQ_CRC32C(crc, map->crc))
        ereport(elevel, (errmsg("relation mapping file \"%s\" contains incorrect checksum",
                               mapfilename)));
}
```