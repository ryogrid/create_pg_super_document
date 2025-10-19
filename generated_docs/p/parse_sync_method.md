# parse_sync_method

## Location
[src/fe_utils/option_utils.c:90-111](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/fe_utils/option_utils.c#L90-L111)

## Overview
Provides standardized parsing of the --sync-method command-line option across PostgreSQL utilities.

## Definition

```c
bool
parse_sync_method(const char *optarg, DataDirSyncMethod *sync_method)
```
## Detailed Description
This utility function implements consistent parsing and validation of synchronization method options across PostgreSQL tools that perform data directory operations. It converts string arguments to the appropriate DataDirSyncMethod enumeration values while ensuring platform compatibility.

The function supports two synchronization methods:
- "fsync": Standard file synchronization using fsync()
- "syncfs": Filesystem-level synchronization (Linux-specific, requires HAVE_SYNCFS)

The function performs validation to ensure only recognized sync methods are accepted, and provides platform-specific error handling for unsupported methods.

## Parameters / Member Variables
- `*optarg`: String containing the sync method name to parse
- `*sync_method`: Pointer to DataDirSyncMethod enum where the result will be stored
## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - pg_log_error
  - [DataDirSyncMethod](../D/DataDirSyncMethod.md) (enum type)
  - DATA_DIR_SYNC_METHOD_FSYNC
  - DATA_DIR_SYNC_METHOD_SYNCFS
  - HAVE_SYNCFS (preprocessor macro)
- Called from (representative examples):
  - [main](../m/main.md) (in initdb)
  - [main](../m/main.md) (in pg_basebackup)
  - [main](../m/main.md) (in pg_checksums)
  - [main](../m/main.md) (in pg_combinebackup)
  - [main](../m/main.md) (in pg_dump)
  - [parseCommandLine](parseCommandLine.md) (in pg_upgrade)

## Notes and Other Information
- Returns true on successful parsing, false on error or unsupported method
- The "syncfs" method is only available on platforms with HAVE_SYNCFS defined
- Provides informative error messages for unrecognized methods and unsupported builds
- Part of the fe_utils library for consistent option handling
- Essential for tools that need to synchronize data directories with different performance characteristics
- The syncfs method can be significantly faster than fsync for large directory trees on supporting filesystems

## Simplified Source

```c
bool parse_sync_method(const char *optarg, DataDirSyncMethod *sync_method) {
    if (strcmp(optarg, "fsync") == 0) {
        *sync_method = DATA_DIR_SYNC_METHOD_FSYNC;
    } else if (strcmp(optarg, "syncfs") == 0) {
#ifdef HAVE_SYNCFS
        *sync_method = DATA_DIR_SYNC_METHOD_SYNCFS;
#else
        pg_log_error("this build does not support sync method \"%s\"", "syncfs");
        return false;
#endif
    } else {
        pg_log_error("unrecognized sync method: %s", optarg);
        return false;
    }

    return true;
}
```