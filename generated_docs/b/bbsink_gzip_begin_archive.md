# bbsink_gzip_begin_archive

## Location
src/backend/backup/basebackup_gzip.c: 114 - 166

## Overview
Initializes the gzip compression stream and prepares to compress the next archive in the base backup, setting up zlib deflate parameters and forwarding to the next sink with a .gz filename extension.

## Definition
```c
static void bbsink_gzip_begin_archive(bbsink *sink, const char *archive_name)
```

## Detailed Description
This function prepares the gzip compression sink to begin processing a new archive file. It performs several critical initialization steps for the zlib compression stream:

1. Initializes the z_stream structure with custom memory allocation functions (gzip_palloc/gzip_pfree)
2. Sets up the output buffer to point to the next sink's buffer
3. Configures zlib using deflateInit2() with specific parameters to generate gzip format (rather than zlib format) by adding 16 to the window bits parameter
4. Creates a new archive name with ".gz" extension to match pg_basebackup behavior
5. Forwards the archive initialization to the next sink in the chain

The function uses deflateInit2() instead of deflateInit() to explicitly request gzip headers, configuring compression level, window size (15+16 for gzip), memory level (8), and strategy (Z_DEFAULT_STRATEGY).

## Parameters / Member Variables
- `sink`: The bbsink structure representing this gzip compression sink
- `archive_name`: The base name of the archive to be compressed (without extension)

## Dependencies
- Functions called/Symbols referenced:
  - memset (memory initialization)
  - deflateInit2 (zlib compression initialization)
  - ereport/errcode/errmsg (error reporting)
  - psprintf (formatted string creation)
  - bbsink_begin_archive (forwards to next sink)
  - pfree (memory deallocation)
  - Assert (assertion checking)
  - gzip_palloc/gzip_pfree (custom memory allocation functions)
- Called from (representative examples):
  - Used as callback function in bbsink_gzip_ops structure

## Notes and Other Information
- This is a static function, only accessible within the compilation unit
- Uses deflateInit2() with window bits = 15+16 to generate gzip headers instead of zlib headers
- Compression level comes from the mysink->compresslevel field set during sink creation
- Archive names get ".gz" extension appended (matches pg_basebackup -z behavior)
- Memory allocation functions are customized for PostgreSQL's memory contexts
- Raises ERROR if compression library initialization fails
- Default parameters: Z_DEFLATED method, 8 memory level, Z_DEFAULT_STRATEGY