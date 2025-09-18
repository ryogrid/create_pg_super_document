# parse_sync_method

## Location
src/fe_utils/option_utils.c: 90 - 111

## Overview
Provides standardized parsing of the --sync-method command-line option across PostgreSQL utilities.

## Definition


## Detailed Description
This utility function implements consistent parsing and validation of synchronization method options across PostgreSQL tools that perform data directory operations. It converts string arguments to the appropriate DataDirSyncMethod enumeration values while ensuring platform compatibility.

The function supports two synchronization methods:
- "fsync": Standard file synchronization using fsync()
- "syncfs": Filesystem-level synchronization (Linux-specific, requires HAVE_SYNCFS)

The function performs validation to ensure only recognized sync methods are accepted, and provides platform-specific error handling for unsupported methods.

## Parameters / Member Variables
- : String containing the sync method name to parse
- : Pointer to DataDirSyncMethod enum where the result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - strcmp
  - pg_log_error
  - DataDirSyncMethod (enum type)
  - DATA_DIR_SYNC_METHOD_FSYNC
  - DATA_DIR_SYNC_METHOD_SYNCFS
  - HAVE_SYNCFS (preprocessor macro)
- Called from (representative examples):
  - main (in initdb)
  - main (in pg_basebackup)
  - main (in pg_checksums)
  - main (in pg_combinebackup)
  - main (in pg_dump)
  - parseCommandLine (in pg_upgrade)

## Notes and Other Information
- Returns true on successful parsing, false on error or unsupported method
- The "syncfs" method is only available on platforms with HAVE_SYNCFS defined
- Provides informative error messages for unrecognized methods and unsupported builds
- Part of the fe_utils library for consistent option handling
- Essential for tools that need to synchronize data directories with different performance characteristics
- The syncfs method can be significantly faster than fsync for large directory trees on supporting filesystems