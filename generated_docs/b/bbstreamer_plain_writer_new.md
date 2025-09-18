# bbstreamer_plain_writer_new

## Location
src/bin/pg_basebackup/bbstreamer_file.c: 78 - 103

## Overview
Creates a new base backup streamer that writes data directly to a file without any processing or compression.

## Definition


## Detailed Description
This function creates and initializes a bbstreamer_plain_writer structure that implements the base backup streaming interface for writing data to a file. The function can operate in two modes: it can either create and manage its own file handle by opening the specified pathname, or use a provided FILE pointer. When creating its own file handle, it opens the file in binary write mode ("wb") and takes responsibility for closing it when the streamer is finalized.

The function allocates memory for the streamer structure using palloc0(), sets up the operations table, and stores the pathname for error reporting purposes. If no FILE pointer is provided, it attempts to open the specified file and stores a flag indicating that it should close the file when done.

## Parameters / Member Variables
- : Path to the output file, used for error reporting and file creation if file parameter is NULL
- : Optional FILE pointer to write to; if NULL, the function will open the pathname for writing

## Dependencies
- Functions called/Symbols referenced:
  - [palloc0](../p/palloc0.md) (memory allocation)
  - [pstrdup](../p/pstrdup.md) (string duplication)
  - fopen (file opening)
  - [pg_fatal](../p/pg_fatal.md) (error reporting)
  - [bbstreamer_plain_writer](bbstreamer_plain_writer.md) (struct type)
  - [bbstreamer_ops](bbstreamer_ops.md) (operations table type)

- Called from (representative examples):
  - [CreateBackupStreamer](../C/CreateBackupStreamer.md) (in pg_basebackup.c)
  - [bbstreamer_buffer_until](bbstreamer_buffer_until.md) (referenced in bbstreamer.h)

## Notes and Other Information
- The streamer takes ownership of the pathname string by duplicating it
- When file parameter is NULL, the function opens the file in binary write mode
- The should_close_file flag is set to true only when the function creates its own file handle
- Error handling uses pg_fatal() which terminates the program on file creation failure
- Part of the PostgreSQL base backup streaming infrastructure used by pg_basebackup utility