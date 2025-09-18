# bbsink_zstd_new

## Location
src/backend/backup/basebackup_zstd.c: 61 - 87

## Overview
Creates a new basebackup sink that performs zstd compression on backup data, wrapping another basebackup sink to provide compression functionality in the backup pipeline.

## Definition


## Detailed Description
This function creates a new zstd compression basebackup sink that wraps another sink in a chain. It allocates and initializes a bbsink_zstd structure with the appropriate operations table and compression specification. The function performs a compile-time check to ensure zstd compression support is available in the build - if not, it raises an error. The created sink will compress data using zstd before passing it to the next sink in the chain.

## Parameters / Member Variables
- : Pointer to the next bbsink in the processing chain that will receive compressed data
- : Compression specification containing zstd compression parameters and settings

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting when zstd not supported)
  - [palloc0](../p/palloc0.md) (for memory allocation)
  - bbsink_zstd_ops (operations table for zstd sink)
  - bbsink (base sink type)
  - [pg_compress_specification](../p/pg_compress_specification.md) (compression configuration)
  - [bbsink_zstd](bbsink_zstd.md) (zstd-specific sink structure)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (main backup function)
  - bbsink_cleanup (cleanup utility)

## Notes and Other Information
- Requires USE_ZSTD compile flag to be enabled, otherwise raises ERRCODE_FEATURE_NOT_SUPPORTED
- Returns NULL when zstd support is not available (though this is never reached due to ereport ERROR)
- Uses Assert to ensure next parameter is not NULL
- The sink follows the chain-of-responsibility pattern where each sink processes data before passing to the next
- Memory is allocated using palloc0 to zero-initialize the structure