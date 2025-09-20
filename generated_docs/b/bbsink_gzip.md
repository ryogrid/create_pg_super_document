# bbsink_gzip

## Location
[src/backend/backup/basebackup_gzip.c:22-35](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/backup/basebackup_gzip.c#L22-L35)

## Overview
A structure that extends the base backup sink () to provide gzip compression functionality for PostgreSQL base backups.

## Definition

```c
typedef struct bbsink_gzip
{
	/* Common information for all types of sink. */
	bbsink		base;

	/* Compression level. */
	int			compresslevel;

	/* Compressed data stream. */
	z_stream	zstream;

	/* Number of bytes staged in output buffer. */
	size_t		bytes_written;
} bbsink_gzip;
```
## Detailed Description
The  structure is a specialized backup sink that implements gzip compression for PostgreSQL base backup operations. It inherits from the base  structure and adds compression-specific functionality using the zlib library. This structure is part of PostgreSQL's backup infrastructure and is used to compress backup data streams on-the-fly during base backup operations.

The structure maintains compression state through the  member and tracks the compression level and output buffer status. It integrates with PostgreSQL's backup sink chain architecture, allowing compression to be inserted as a processing layer in the backup data flow.

## Parameters / Member Variables
- : The base  structure containing common sink functionality and operations
- : Integer specifying the gzip compression level (1-9 or Z_DEFAULT_COMPRESSION)
- : The zlib compression stream state structure used for actual compression operations
- : Size tracking the number of bytes that have been staged in the output buffer

## Dependencies
- Functions called/Symbols referenced:
  -  (base structure)
  -  (from zlib)
- Used by:
  -  (constructor function)
  -  (archive initialization)
  -  (content compression)
  -  (archive finalization)

## Notes and Other Information
- This structure is only available when PostgreSQL is built with zlib support (HAVE_LIBZ)
- The compression level must be between 1-9 or use Z_DEFAULT_COMPRESSION
- Part of PostgreSQL's pluggable backup sink architecture introduced for flexible backup processing
- Located in 
- The structure enables streaming compression of backup data without requiring intermediate storage of uncompressed data