# bbsink_zstd_new

## Location
[src/backend/backup/basebackup_zstd.c:61-87](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_zstd.c#L61-L87)

## Overview
Creates a new basebackup sink that performs zstd compression on backup data, wrapping another basebackup sink to provide compression functionality in the backup pipeline.

## Definition

```c
bbsink *
bbsink_zstd_new(bbsink *next, pg_compress_specification *compress)
```
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
  - [bbsink](bbsink.md) (base sink type)
  - [pg_compress_specification](../p/pg_compress_specification.md) (compression configuration)
  - [bbsink_zstd](bbsink_zstd.md) (zstd-specific sink structure)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (main backup function)
  - [bbsink_cleanup](bbsink_cleanup.md) (cleanup utility)

## Notes and Other Information
- Requires USE_ZSTD compile flag to be enabled, otherwise raises ERRCODE_FEATURE_NOT_SUPPORTED
- Returns NULL when zstd support is not available (though this is never reached due to ereport ERROR)
- Uses Assert to ensure next parameter is not NULL
- The sink follows the chain-of-responsibility pattern where each sink processes data before passing to the next
- Memory is allocated using palloc0 to zero-initialize the structure

## Simplified Source

```c
// Simplified version of bbsink_zstd_new
bbsink *bbsink_zstd_new(bbsink *next, pg_compress_specification *compress) {
#ifndef USE_ZSTD
    // Error if zstd support not compiled in
    ereport(ERROR,
            (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
             errmsg("zstd compression is not supported by this build")));
    return NULL;
#else
    bbsink_zstd *sink;

    // Validate next sink exists
    Assert(next != NULL);

    // Allocate and initialize zstd sink structure
    sink = palloc0(sizeof(bbsink_zstd));

    // Set up operations table for zstd functionality
    *((const bbsink_ops **) &sink->base.bbs_ops) = &bbsink_zstd_ops;

    // Chain to next sink in pipeline
    sink->base.bbs_next = next;

    // Store compression specification
    sink->compress = compress;

    return &sink->base;
#endif
}
```

Key simplifications made:
- Added clear comments explaining build-time check and error handling
- Preserved all validation logic and error conditions
- Maintained the chain-of-responsibility pattern setup
- Kept compression specification storage intact
- Simplified structure while preserving all functionality
- Note: zstd compression levels are validated elsewhere in the compression specification