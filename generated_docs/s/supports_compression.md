# supports_compression

## Location
[src/bin/pg_dump/compress_io.c:88-123](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_dump/compress_io.c#L88-L123)

## Overview
This function checks whether support for a specific compression algorithm is implemented in pg_dump/restore tools and returns an error message if the algorithm is not supported.

## Definition

```c
char *
supports_compression(const pg_compress_specification compression_spec)
```
## Detailed Description
The  function validates whether a given compression algorithm is available in the current build of PostgreSQL's pg_dump/restore utilities. It checks compile-time flags to determine if specific compression libraries (libz, LZ4, ZSTD) were included during the build process. The function returns NULL on success (indicating the algorithm is supported) or a malloc'ed error string that can be used in error messages when the algorithm is not supported.

The function supports four compression algorithms:
- PG_COMPRESSION_NONE (always supported)
- PG_COMPRESSION_GZIP (requires HAVE_LIBZ)
- PG_COMPRESSION_LZ4 (requires USE_LZ4)
- PG_COMPRESSION_ZSTD (requires USE_ZSTD)

## Parameters / Member Variables
- : A pg_compress_specification structure containing the compression algorithm and related configuration to be validated

## Dependencies
- Functions called/Symbols referenced:
  - [get_compress_algorithm_name](../g/get_compress_algorithm_name.md)
  - [psprintf](../p/psprintf.md)
  - [pg_compress_specification](../p/pg_compress_specification.md)
  - [pg_compress_algorithm](../p/pg_compress_algorithm.md)
  - PG_COMPRESSION_NONE
  - PG_COMPRESSION_GZIP
  - PG_COMPRESSION_LZ4
  - PG_COMPRESSION_ZSTD
- Called from (representative examples):
  - [RestoreArchive](../R/RestoreArchive.md) (src/bin/pg_dump/pg_backup_archiver.c:374)
  - [ReadHead](../R/ReadHead.md) (src/bin/pg_dump/pg_backup_archiver.c:4050)
  - [main](../m/main.md) (src/bin/pg_dump/pg_dump.c:806)

## Notes and Other Information
- The function returns a malloc'ed string on failure, so the caller is responsible for freeing the memory
- The availability of compression algorithms depends on compile-time configuration and linked libraries
- PG_COMPRESSION_NONE is always supported as it represents no compression
- The error message is internationalized using the _() macro
- Located in src/bin/pg_dump/compress_io.c at lines 88-123