# bbsink_gzip_new

## Location
[src/backend/backup/basebackup_gzip.c:62-93](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_gzip.c#L62-L93)

## Overview
Creates a new basebackup sink that performs gzip compression on the data stream before passing it to the next sink in the chain.

## Definition
```c
bbsink *bbsink_gzip_new(bbsink *next, pg_compress_specification *compress)
```

## Detailed Description
This function constructs a new gzip compression sink for PostgreSQL's base backup system. It serves as a factory function that creates a bbsink_gzip structure and configures it with the specified compression parameters. The function implements a chain-of-responsibility pattern where this compression sink processes data and forwards the compressed output to the next sink in the pipeline.

The function validates that gzip compression is supported at build time (requires libz) and ensures the compression level is within valid bounds (1-9 or Z_DEFAULT_COMPRESSION). It allocates memory for the sink structure, sets up the operation callbacks, and initializes the compression level.

## Parameters / Member Variables
- `next`: The next bbsink in the processing chain that will receive compressed data
- `compress`: Compression specification structure containing compression level and other parameters

## Dependencies
- Functions called/Symbols referenced:
  - ereport (error reporting)
  - Assert (assertion checking)
  - [palloc0](../p/palloc0.md) (memory allocation)
  - bbsink_gzip_ops (operation callbacks structure)
- Called from (representative examples):
  - [SendBaseBackup](../S/SendBaseBackup.md) (src/backend/backup/basebackup.c:1042)
  - [bbsink_cleanup](bbsink_cleanup.md) (src/include/backup/basebackup_sink.h:287)

## Notes and Other Information
- Function is only available when PostgreSQL is built with libz support (HAVE_LIBZ)
- Raises ERROR if gzip compression is not supported by the build
- Compression level must be between 1-9 or Z_DEFAULT_COMPRESSION
- Memory is allocated using palloc0, which initializes the structure to zero
- The function follows PostgreSQL's bbsink interface for chaining backup sinks