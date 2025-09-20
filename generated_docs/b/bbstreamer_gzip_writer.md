# bbstreamer_gzip_writer

## Location
[src/bin/pg_basebackup/bbstreamer_gzip.c:26-31](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_basebackup/bbstreamer_gzip.c#L26-L31)

## Overview
A structure representing a gzip compression stream writer that compresses data using zlib and writes it to a file.

## Definition

```c
typedef struct bbstreamer_gzip_writer
{
	bbstreamer	base;
	char	   *pathname;
	gzFile		gzfile;
} bbstreamer_gzip_writer;
```
## Detailed Description
The  is a specialized bbstreamer implementation that provides gzip compression functionality for PostgreSQL's base backup system. It inherits from the base  structure and adds gzip-specific functionality through the zlib library. This structure is used internally by pg_basebackup to create compressed backup archives.

The structure works in conjunction with the bbstreamer framework, implementing the standard content/finalize/free operations through function pointers in the  operations table. When data is fed to this streamer, it compresses the data using gzip and writes it to the specified file or file descriptor.

## Parameters / Member Variables
- : The base bbstreamer structure containing common streamer functionality and operation function pointers
- : String containing the file path, used primarily for error reporting and file operations when no file handle is provided
- : The zlib gzFile handle used for gzip compression and file writing operations

## Dependencies
- Functions called/Symbols referenced:
  - [bbstreamer](bbstreamer.md) (base structure)
  - gzFile (zlib type)
- Called from (representative examples):
  - [bbstreamer_gzip_writer_new](bbstreamer_gzip_writer_new.md)
  - [bbstreamer_gzip_writer_content](bbstreamer_gzip_writer_content.md)
  - [bbstreamer_gzip_writer_finalize](bbstreamer_gzip_writer_finalize.md)
  - [bbstreamer_gzip_writer_free](bbstreamer_gzip_writer_free.md)

## Notes and Other Information
- This structure is only available when PostgreSQL is compiled with zlib support (HAVE_LIBZ)
- The structure is typically instantiated through  which handles initialization of the gzip file handle and compression parameters
- The pathname member is always duplicated (pstrdup) to ensure memory ownership
- Error handling includes specific gzip error reporting through 
- Supports both file-based and stdout-based output through different initialization paths